/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.rpc.location.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.model.partition.ObPartitionLocationInfo;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObFetchPartitionMetaRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObFetchPartitionMetaResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObFetchPartitionMetaType;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.rpc.location.LocationUtil.*;
import static com.alipay.oceanbase.rpc.location.model.TableEntry.HBASE_ROW_KEY_ELEMENT;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;

public class TableLocations {
    private static final Logger       logger                                  = getLogger(TableLocations.class);
    private static final ObjectMapper objectMapper                            = new ObjectMapper();
    static {
        // FAIL_ON_EMPTY_BEANS means that whether throwing exception if there is no any serializable member with getter or setter in an object
        // considering partitionElements in range partDesc is a list of Comparable interface and Comparable has no getter and setter
        // we have to set this configuration as false because tableEntry may be serialized in debug log
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }
    private final ObTableClient       tableClient;
    private Map<String, Lock>         metaRefreshingLocks                     = new ConcurrentHashMap<String, Lock>();
    private Map<String, Lock>         locationBatchRefreshingLocks            = new ConcurrentHashMap<String, Lock>();
    /*
     * TableName -> TableEntry, containing table meta and location information
     */
    private Map<String, TableEntry> locations                               = new ConcurrentHashMap<String, TableEntry>();
    private AtomicInteger           tableEntryRefreshContinuousFailureCount = new AtomicInteger(0);

    public TableLocations(ObTableClient tabelClient) {
        this.tableClient = tabelClient;
    }

    public TableEntry getTableEntry(String tableName) {
        return locations.get(tableName);
    }

    public Map<String, TableEntry> getLocations() {
        return locations;
    }

    public void eraseTableEntry(String tableName) {
        locations.remove(tableName);
    }

    public Lock getMetaRefreshLock(String tableName) {
        Lock tempLock = new ReentrantLock();
        Lock lock = metaRefreshingLocks.putIfAbsent(tableName, tempLock);
        lock = (lock == null) ? tempLock : lock;
        return lock;
    }

    public Lock getLocationBatchRefreshLock(String tableName) {
        Lock tempLock = new ReentrantLock();
        Lock lock = locationBatchRefreshingLocks.putIfAbsent(tableName, tempLock);
        lock = (lock == null) ? tempLock : lock;
        return lock;
    }

    /**
     * refresh TableEntry meta information if the last refresh is beyond 3 seconds ago
     * to avoid frequent refreshing, the sequent refreshing will not operate if the interval is within 100 ms
     * @param tableName
     * @param serverRoster
     * @param sysUA
     * @return
     * @throws ObTableEntryRefreshException
     */
    public TableEntry refreshMeta(String tableName, ServerRoster serverRoster,
                                  final ObUserAuth sysUA) throws Exception {
        long runtimeMaxWait = tableClient.getRuntimeMaxWait();
        int tableEntryRefreshContinuousFailureCeiling = tableClient
            .getTableEntryRefreshContinuousFailureCeiling();
        long tableEntryRefreshLockTimeout = tableClient.getTableEntryRefreshLockTimeout();
        long refreshMetaInterval = getTableLevelRefreshInterval(serverRoster);

        TableEntry tableEntry = locations.get(tableName);
        // avoid bad contention in high concurrent situation
        if (tableEntry != null) {
            long current = System.currentTimeMillis();
            long fetchMetaInterval = current - tableEntry.getRefreshMetaTimeMills();
            // if refreshed within refreshMetaInterval, do not refresh
            if (fetchMetaInterval < refreshMetaInterval) {
                logger
                    .info(
                        "punish table entry {} : table entry refresh time {} punish interval {} current time {}.",
                        tableName, tableEntry.getRefreshMetaTimeMills(), refreshMetaInterval,
                        current);
                return tableEntry;
            }
        }
        Lock lock = getMetaRefreshLock(tableName);
        boolean acquired = false;
        try {
            int tryTimes = 0;
            long startExecute = System.currentTimeMillis();
            while (true) {
                long costMillis = System.currentTimeMillis() - startExecute;
                if (costMillis > runtimeMaxWait) {
                    logger.error("table name: {} it has tried " + tryTimes
                            + " times to refresh table meta and it has waited " + costMillis + " ms"
                            + " which exceeds runtime max wait timeout "
                            + runtimeMaxWait + " ms", tableName);
                    throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                            + " times and it has waited " + costMillis
                            + "ms which exceeds runtime max wait timeout "
                            + runtimeMaxWait + " ms");
                }
                tryTimes++;
                try {
                    if (!acquired) {
                        acquired = lock.tryLock(tableEntryRefreshLockTimeout, TimeUnit.MILLISECONDS);
                        if (!acquired) {
                            String errMsg = "try to lock tableEntry refreshing timeout, tableName:" + tableName
                                    + " , timeout:" + tableEntryRefreshLockTimeout + ".";
                            RUNTIME.warn(errMsg);
                            throw new ObTableTryLockTimeoutException(errMsg);
                        }
                    }
                    logger.debug("success to acquire refresh table meta lock, tableName: {}", tableName);
                    tableEntry = locations.get(tableName);
                    if (tableEntry != null) {
                        long current = System.currentTimeMillis();
                        long fetchMetaInterval = current - tableEntry.getRefreshMetaTimeMills();
                        // if refreshed within refreshMetaInterval, do not refresh
                        if (fetchMetaInterval < refreshMetaInterval) {
                            logger
                                    .info(
                                            "punish table entry {} : table entry refresh time {} punish interval {} current time {}.",
                                            tableName, tableEntry.getRefreshMetaTimeMills(), refreshMetaInterval,
                                            current);
                            return tableEntry;
                        }
                    }
                    return refreshTableEntry(tableEntry, tableName, serverRoster, sysUA);
                } catch (ObTableTryLockTimeoutException e) {
                    // if try lock timeout, need to retry
                    RUNTIME.warn("wait to try lock to timeout when refresh table meta, tryTimes: {}",
                            tryTimes, e);
                } catch (ObTableNotExistException e) {
                    RUNTIME.error("refresh table meta meet exception", e);
                    throw e;
                } catch (ObTableEntryRefreshException e) {
                    RUNTIME.error("refresh table meta meet exception", e);
                    // maybe the observers have changed if keep failing, need to refresh roster
                    if (tableEntryRefreshContinuousFailureCount.incrementAndGet() > tableEntryRefreshContinuousFailureCeiling) {
                        logger.error(LCD.convert("01-00019"),
                                tableEntryRefreshContinuousFailureCeiling);
                        tableClient.syncRefreshMetadata(false);
                        tableEntryRefreshContinuousFailureCount.set(0);
                        serverRoster = tableClient.getTableRoute().getServerRoster();
                    } else if (e.isConnectInactive()) {
                        // getMetaRefreshConnection failed, maybe the server is down, so we need to refresh metadata directly
                        tableClient.syncRefreshMetadata(true);
                        tableEntryRefreshContinuousFailureCount.set(0);
                        serverRoster = tableClient.getTableRoute().getServerRoster();
                    }
                } catch (Throwable t) {
                    RUNTIME.error("refresh table meta meet exception", t);
                    throw t;
                }
            } // end while
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    /**
     * refresh TableEntry meta information
     * @param tableEntry
     * @param tableName
     * @param serverRoster
     * @param sysUA
     * @return
     * @throws ObTableEntryRefreshException
     */
    private TableEntry refreshTableEntry(TableEntry tableEntry, String tableName,
                                         final ServerRoster serverRoster, final ObUserAuth sysUA)
                                                                                                 throws Exception {
        TableEntryKey tableEntryKey = new TableEntryKey(tableClient.getClusterName(),
            tableClient.getTenantName(), tableClient.getDatabase(), tableName);
        try {
            // tableEntry will point to a new object, the old tableEntry will be gc by jvm
            tableEntry = loadTableEntryWithPriority(serverRoster, //
                tableEntry, //
                tableEntryKey,//
                tableClient.getTableEntryAcquireConnectTimeout(),//
                tableClient.getTableEntryAcquireSocketTimeout(),//
                tableClient.getServerAddressPriorityTimeout(), sysUA, !tableClient
                    .getServerCapacity().isSupportDistributedExecute());
            if (tableEntry.isPartitionTable()) {
                switch (tableClient.getRunningMode()) {
                    case HBASE:
                        tableClient.addRowKeyElement(tableName, HBASE_ROW_KEY_ELEMENT);
                        tableEntry.setRowKeyElement(HBASE_ROW_KEY_ELEMENT);
                        break;
                    case NORMAL:
                        Map<String, Integer> rowKeyElement = tableClient
                            .getRowKeyElement(tableName);
                        if (rowKeyElement != null) {
                            tableEntry.setRowKeyElement(rowKeyElement);
                        } else {
                            RUNTIME
                                .error("partition table must add row key element name for table: "
                                       + tableName + " with table entry key: " + tableEntryKey);
                            throw new ObTableUnexpectedException(
                                "partition table must add row key element name for table: "
                                        + tableName + ", failed to get table entry key="
                                        + tableEntryKey);
                        }
                }
                tableEntry.prepare();
            }
        } catch (ObTableNotExistException e) {
            RUNTIME.error("refreshTableEntry meet table not exist exception", e);
            throw e;
        } catch (ObTableUnexpectedException e) {
            RUNTIME.error("refreshTableEntry meet unexpected exception", e);
            throw e;
        } catch (ObTableSchemaVersionMismatchException e) {
            RUNTIME.error("refreshTableEntry meet schema version mismatch exception", e);
            throw e;
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00020"), tableEntryKey, tableEntry, e);
            if (e instanceof ObTableEntryRefreshException) {
                throw new ObTableEntryRefreshException(String.format(
                    "failed to get table entry key=%s original tableEntry=%s ", tableEntryKey,
                    tableEntry), e, ((ObTableEntryRefreshException) e).isConnectInactive());
            } else {
                throw new ObTableEntryRefreshException(String.format(
                    "failed to get table entry key=%s original tableEntry=%s ", tableEntryKey,
                    tableEntry), e);
            }
        }
        // prepare the table entry for weak read.
        tableEntry.prepareForWeakRead(serverRoster.getServerLdcLocation());
        locations.put(tableName, tableEntry);
        tableEntryRefreshContinuousFailureCount.set(0);
        if (logger.isDebugEnabled()) {
            logger.debug("refresh table entry, tableName: {}, key:{} entry:{} ", tableName,
                tableEntryKey, objectMapper.writeValueAsString(tableEntry));
        }
        return tableEntry;
    }

    public TableEntry refreshPartitionLocation(TableEntry tableEntry, String tableName,
                                               long tabletId, ServerRoster serverRoster,
                                               final ObUserAuth sysUA) throws Exception {
        TableEntryKey tableEntryKey = new TableEntryKey(tableClient.getClusterName(),
            tableClient.getTenantName(), tableClient.getDatabase(), tableName);
        if (tableEntry == null) {
            throw new ObTableGetException("Need to fetch meta for table: " + tableName + ".");
        }
        ObPartitionLocationInfo locationInfo = tableEntry.getPartitionEntry().getPartitionInfo(
            tabletId);
        long refreshMetaInterval = getTableLevelRefreshInterval(serverRoster);
        int tableEntryRefreshContinuousFailureCeiling = tableClient
            .getTableEntryRefreshContinuousFailureCeiling();
        long runtimeMaxWait = tableClient.getRuntimeMaxWait();
        long tableEntryRefreshLockTimeout = tableClient.getTableEntryRefreshLockTimeout();
        long lastRefreshTime = locationInfo.getLastUpdateTime();
        long tableEntryRefreshInterval = tableClient.getTableEntryRefreshIntervalCeiling();
        long currentTime = System.currentTimeMillis();
        // do not refresh tablet location if refreshed within 300 milliseconds
        if (currentTime - lastRefreshTime < tableEntryRefreshInterval) {
            logger
                .info(
                    "punish table entry {}, last partition location refresh time {}, punish interval {}, current time {}.",
                    tableName, lastRefreshTime, tableEntryRefreshInterval, currentTime);
            return tableEntry;
        }
        Lock lock = locationInfo.refreshLock;
        boolean acquired = false;
        try {
            int tryTimes = 0;
            long startExecute = System.currentTimeMillis();
            while (true) {
                long costMillis = System.currentTimeMillis() - startExecute;
                if (costMillis > runtimeMaxWait) {
                    logger.error("table name: {} it has tried " + tryTimes
                            + " times to refresh tablet location and it has waited " + costMillis + " ms"
                            + " which exceeds runtime max wait timeout "
                            + runtimeMaxWait + " ms", tableName);
                    throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                            + " times and it has waited " + costMillis
                            + "ms which exceeds runtime max wait timeout "
                            + runtimeMaxWait + " ms");
                }
                tryTimes++;
                try {
                    if (!acquired) {
                        acquired = lock.tryLock(tableEntryRefreshLockTimeout, TimeUnit.MILLISECONDS);
                        if (!acquired) {
                            String errMsg = "try to lock tablet location refreshing timeout " + " ,tableName:"
                                    + tableName + " , timeout:" + tableEntryRefreshLockTimeout + ".";
                            RUNTIME.warn(errMsg);
                            throw new ObTableTryLockTimeoutException(errMsg);
                        }
                    }
                    logger.debug("success acquire refresh table location lock, tableName: {}", tableName);
                    locationInfo = tableEntry.getPartitionEntry().getPartitionInfo(tabletId);
                    lastRefreshTime = locationInfo.getLastUpdateTime();
                    currentTime = System.currentTimeMillis();
                    if (currentTime - lastRefreshTime < tableEntryRefreshInterval) {
                        logger
                                .info(
                                        "punish table entry {}, last partition location refresh time {}, punish interval {}, current time {}.",
                                        tableName, lastRefreshTime, tableEntryRefreshInterval, currentTime);
                        return tableEntry;
                    }
                    tableEntry = loadTableEntryLocationWithPriority(serverRoster, tableEntryKey,
                            tableEntry, tabletId, tableClient.getTableEntryAcquireConnectTimeout(),
                            tableClient.getTableEntryAcquireSocketTimeout(),
                            tableClient.getServerAddressPriorityTimeout(), sysUA, !tableClient
                                    .getServerCapacity().isSupportDistributedExecute() /* withLsId */);
                    break;
                } catch (ObTableTryLockTimeoutException e) {
                    // if try lock timeout, need to retry
                    RUNTIME.warn("wait to try lock to timeout when refresh table meta, tryTimes: {}",
                            tryTimes, e);
                } catch (ObTableNotExistException e) {
                    RUNTIME.error("refresh partition location meet table not existed exception", e);
                    throw e;
                } catch (ObTableSchemaVersionMismatchException e) {
                    RUNTIME.error(
                            "refresh partition location meet schema_version mismatched exception, tryTimes: {}", tryTimes, e);
                    long schemaVersion = tableEntry.getSchemaVersion();
                    logger.debug(
                            "schema_version mismatch when refreshing tablet location, old schema_version is: {}", schemaVersion);
                    tableEntry = locations.get(tableName);
                    // sleep over waiting interval of refreshing meta to refresh meta
                    long interval = System.currentTimeMillis() - tableEntry.getRefreshMetaTimeMills();
                    if (interval < refreshMetaInterval) {
                        Thread.sleep(refreshMetaInterval - interval);
                    }
                    tableEntry = locations.get(tableName);
                    // if schema_version has been updated, directly retry
                    if (schemaVersion == tableEntry.getSchemaVersion()) {
                        tableEntry = refreshMeta(tableName, serverRoster, sysUA);
                    }
                } catch (ObTableEntryRefreshException e) {
                    RUNTIME.error("refresh partition location meet entry refresh exception, tryTimes: {}", tryTimes, e);
                    // maybe the observers have changed if keep failing, need to refresh roster
                    if (tableEntryRefreshContinuousFailureCount.incrementAndGet() > tableEntryRefreshContinuousFailureCeiling) {
                        logger.error(LCD.convert("01-00019"),
                                tableEntryRefreshContinuousFailureCeiling);
                        tableClient.syncRefreshMetadata(false);
                        tableEntryRefreshContinuousFailureCount.set(0);
                        serverRoster = tableClient.getTableRoute().getServerRoster();
                    } else if (e.isConnectInactive()) {
                        // getMetaRefreshConnection failed, maybe the server is down, so we need to refresh metadata directly
                        tableClient.syncRefreshMetadata(true);
                        tableEntryRefreshContinuousFailureCount.set(0);
                        serverRoster = tableClient.getTableRoute().getServerRoster();
                    }
                } catch (Throwable t) {
                    RUNTIME.error("refresh partition location meet exception", t);
                    throw t;
                }
            } // end while
            tableEntry.prepareForWeakRead(serverRoster.getServerLdcLocation());
            locations.put(tableName, tableEntry);
            tableEntryRefreshContinuousFailureCount.set(0);
            return tableEntry;
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    /**
     * refresh all tablet locations in this table
     * only used in batch when large portion of tablets have changed
     * */
    public TableEntry refreshTabletLocationBatch(TableEntry tableEntry, String tableName,
                                                 ServerRoster serverRoster, final ObUserAuth sysUA)
                                                                                                   throws Exception {
        TableEntryKey tableEntryKey = new TableEntryKey(tableClient.getClusterName(),
            tableClient.getTenantName(), tableClient.getDatabase(), tableName);
        if (tableEntry == null) {
            throw new ObTableGetException("Need to fetch meta for table: " + tableName + ".");
        }
        int tableEntryRefreshContinuousFailureCeiling = tableClient
            .getTableEntryRefreshContinuousFailureCeiling();
        long runtimeMaxWait = tableClient.getRuntimeMaxWait();
        long tableEntryRefreshLockTimeout = tableClient.getTableEntryRefreshLockTimeout();
        long lastRefreshTime = tableEntry.getPartitionEntry().getLastRefreshAllTime();
        long refreshBatchTabletInterval = getTableLevelRefreshInterval(serverRoster);
        long currentTime = System.currentTimeMillis();
        // do not refresh tablet location if refreshed within refreshBatchTabletInterval
        if (currentTime - lastRefreshTime < refreshBatchTabletInterval) {
            logger
                .info(
                    "punish table entry {}, last batch location refresh time {}, punish interval {}, current time {}.",
                    tableName, lastRefreshTime, refreshBatchTabletInterval, currentTime);
            return tableEntry;
        }
        Lock lock = getLocationBatchRefreshLock(tableName);
        boolean acquired = false;
        try {
            int tryTimes = 0;
            long startExecute = System.currentTimeMillis();
            while (true) {
                long costMillis = System.currentTimeMillis() - startExecute;
                if (costMillis > runtimeMaxWait) {
                    logger.error("table name: {} it has tried " + tryTimes
                            + " times to refresh tablet location in batch and it has waited " + costMillis + " ms"
                            + " which exceeds runtime max wait timeout "
                            + runtimeMaxWait + " ms", tableName);
                    throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                            + " times and it has waited " + costMillis
                            + "ms which exceeds runtime max wait timeout "
                            + runtimeMaxWait + " ms");
                }
                tryTimes++;
                try {
                    if (!acquired) {
                        acquired = lock.tryLock(tableEntryRefreshLockTimeout, TimeUnit.MILLISECONDS);
                        if (!acquired) {
                            String errMsg = "try to lock locations refreshing in batch timeout " + " ,tableName:"
                                    + tableName + " , timeout:" + tableEntryRefreshLockTimeout + ".";
                            RUNTIME.warn(errMsg);
                            throw new ObTableTryLockTimeoutException(errMsg);
                        }
                    }
                    logger.debug("success to acquire refresh tablet locations in batch lock, tableName: {}", tableName);
                    lastRefreshTime = tableEntry.getPartitionEntry().getLastRefreshAllTime();
                    currentTime = System.currentTimeMillis();
                    if (currentTime - lastRefreshTime < refreshBatchTabletInterval) {
                        logger
                                .info(
                                        "punish table entry {}, last batch location refresh time {}, punish interval {}, current time {}.",
                                        tableName, lastRefreshTime, refreshBatchTabletInterval, currentTime);
                        return tableEntry;
                    }
                    tableEntry = loadTableEntryLocationInBatchWithPriority(serverRoster,
                            tableEntryKey, tableEntry,
                            tableClient.getTableEntryAcquireConnectTimeout(),
                            tableClient.getTableEntryAcquireSocketTimeout(),
                            tableClient.getServerAddressPriorityTimeout(), sysUA, !tableClient
                                    .getServerCapacity().isSupportDistributedExecute() /* withLsId */);
                    break;
                } catch (ObTableTryLockTimeoutException e) {
                    // if try lock timeout, need to retry
                    RUNTIME.warn("wait to try lock to timeout when refresh table meta, tryTimes: {}",
                            tryTimes, e);
                } catch (ObTableNotExistException e) {
                    RUNTIME.error("refresh location in batch meet table not existed exception", e);
                    throw e;
                } catch (ObTableSchemaVersionMismatchException e) {
                    RUNTIME.error(
                            "refresh location in batch meet schema_version mismatched exception, tryTimes: {}", tryTimes, e);
                    long schemaVersion = tableEntry.getSchemaVersion();
                    logger.debug(
                            "schema_version mismatch when refreshing tablet locations in batch, old schema_version is: {}", schemaVersion);
                    tableEntry = locations.get(tableName);
                    // sleep over waiting interval of refreshing meta to refresh meta
                    long interval = System.currentTimeMillis() - tableEntry.getRefreshMetaTimeMills();
                    if (interval < refreshBatchTabletInterval) {
                        Thread.sleep(refreshBatchTabletInterval - interval);
                    }
                    tableEntry = locations.get(tableName);
                    // if schema_version has been updated, directly retry
                    if (schemaVersion == tableEntry.getSchemaVersion()) {
                        tableEntry = refreshMeta(tableName, serverRoster, sysUA);
                    }
                } catch (ObTableEntryRefreshException e) {
                    RUNTIME.error("refresh location in batch meet entry refresh exception", e);
                    // maybe the observers have changed if keep failing, need to refresh roster
                    if (tableEntryRefreshContinuousFailureCount.incrementAndGet() > tableEntryRefreshContinuousFailureCeiling) {
                        logger.error(LCD.convert("01-00019"),
                                tableEntryRefreshContinuousFailureCeiling);
                        tableClient.syncRefreshMetadata(false);
                        tableEntryRefreshContinuousFailureCount.set(0);
                        serverRoster = tableClient.getTableRoute().getServerRoster();
                    } else if (e.isConnectInactive()) {
                        // getMetaRefreshConnection failed, maybe the server is down, so we need to refresh metadata directly
                        tableClient.syncRefreshMetadata(true);
                        tableEntryRefreshContinuousFailureCount.set(0);
                        serverRoster = tableClient.getTableRoute().getServerRoster();
                    }
                } catch (Throwable t) {
                    RUNTIME.error("refresh location in batch meet exception", t);
                    throw t;
                }
            } // end while
            tableEntry.prepareForWeakRead(serverRoster.getServerLdcLocation());
            locations.put(tableName, tableEntry);
            tableEntryRefreshContinuousFailureCount.set(0);
            return tableEntry;
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    private long getTableLevelRefreshInterval(ServerRoster serverRoster) {
        long tableEntryRefreshIntervalBase = tableClient.getTableEntryRefreshIntervalBase();
        long tableEntryRefreshIntervalCeiling = tableClient.getTableEntryRefreshIntervalCeiling();
        long refreshInterval = (long) (tableEntryRefreshIntervalBase * Math.pow(2,
                -serverRoster.getMaxPriority()));
        refreshInterval = Math.min(refreshInterval, tableEntryRefreshIntervalCeiling);
        return refreshInterval;
    }

    /**
     * fetch ODP partition meta information
     * only support by ODP version after 4.3.2
     * @param tableName table name to query
     * @param forceRefresh flag to force ODP to fetch the latest partition meta information
     * @param odpTable odp table to execute refreshing
     * @return TableEntry ODPTableEntry
     * @throws Exception Exception
     */
    public TableEntry refreshOdpMeta(String tableName, boolean forceRefresh, ObTable odpTable)
                                                                                              throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        long runtimeMaxWait = tableClient.getRuntimeMaxWait();
        long reFetchInterval = tableClient.getTableEntryRefreshIntervalCeiling();
        TableEntry odpTableEntry = locations.get(tableName);
        long lastOdpCreateTimeMills = -1;

        // already have odpTableEntry
        if (odpTableEntry != null) {
            long lastRefreshTime = odpTableEntry.getRefreshMetaTimeMills();
            if (!forceRefresh && System.currentTimeMillis() - lastRefreshTime < reFetchInterval) {
                return odpTableEntry;
            }
            lastOdpCreateTimeMills = odpTableEntry.getODPMetaCreateTimeMills();
        }
        Lock lock = getMetaRefreshLock(tableName);
        boolean acquired = false;
        boolean forceRenew = forceRefresh;
        try {
            int tryTimes = 0;
            long startExecute = System.currentTimeMillis();
            while (true) {
                long costMillis = System.currentTimeMillis() - startExecute;
                if (costMillis > runtimeMaxWait) {
                    logger.error("table name: {} it has tried " + tryTimes
                            + " times to refresh odp table meta and it has waited " + costMillis + " ms"
                            + " which exceeds runtime max wait timeout "
                            + runtimeMaxWait + " ms", tableName);
                    throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                            + " times and it has waited " + costMillis
                            + "ms which exceeds runtime max wait timeout "
                            + runtimeMaxWait + " ms");
                }
                tryTimes++;
                try {
                    // attempt lock the refreshing action, avoiding concurrent refreshing
                    // use the time-out mechanism, avoiding the rpc hanging up
                    if (!acquired) {
                        acquired = lock.tryLock(tableClient.getODPTableEntryRefreshLockTimeout(),
                                TimeUnit.MILLISECONDS);
                        if (!acquired) {
                            String errMsg = "try to lock odpTable-entry refreshing timeout " + " ,tableName:"
                                    + tableName + " , timeout:"
                                    + tableClient.getODPTableEntryRefreshLockTimeout() + ".";
                            RUNTIME.warn(errMsg);
                            throw new ObTableTryLockTimeoutException(errMsg);
                        }
                    }
                    if (locations.get(tableName) != null) {
                        odpTableEntry = locations.get(tableName);
                        long interval = System.currentTimeMillis()
                                - odpTableEntry.getRefreshMetaTimeMills();
                        // do not fetch partition meta if and only if the refresh interval is less than 0.5 seconds
                        // and no need to fore renew
                        if (interval < reFetchInterval) {
                            if (!forceRefresh) {
                                return odpTableEntry;
                            }
                            Thread.sleep(reFetchInterval - interval);
                        }
                    }
                    ObFetchPartitionMetaRequest request = ObFetchPartitionMetaRequest.getInstance(
                            ObFetchPartitionMetaType.GET_PARTITION_META.getIndex(), tableName,
                            tableClient.getClusterName(), tableClient.getTenantName(),
                            tableClient.getDatabase(), forceRenew,
                            odpTable.getObTableOperationTimeout());
                    ObPayload result = odpTable.execute(request);
                    checkODPPartitionMetaResult(lastOdpCreateTimeMills, request, result);
                    ObFetchPartitionMetaResult obFetchPartitionMetaResult = (ObFetchPartitionMetaResult) result;
                    odpTableEntry = obFetchPartitionMetaResult.getTableEntry();
                    TableEntryKey key = new TableEntryKey(tableClient.getClusterName(),
                            tableClient.getTenantName(), tableClient.getDatabase(), tableName);
                    odpTableEntry.setTableEntryKey(key);
                    if (odpTableEntry.isPartitionTable()) {
                        switch (tableClient.getRunningMode()) {
                            case HBASE:
                                tableClient.addRowKeyElement(tableName, HBASE_ROW_KEY_ELEMENT);
                                odpTableEntry.setRowKeyElement(HBASE_ROW_KEY_ELEMENT);
                                break;
                            case NORMAL:
                                Map<String, Integer> rowKeyElement = tableClient
                                        .getRowKeyElement(tableName);
                                if (rowKeyElement != null) {
                                    odpTableEntry.setRowKeyElement(rowKeyElement);
                                } else {
                                    RUNTIME.error("partition table must has row key element key ="
                                            + key);
                                    throw new ObTableUnexpectedException(
                                            "partition table must has row key element key =" + key);
                                }
                        }
                    }
                    locations.put(tableName, odpTableEntry);
                    return odpTableEntry;
                } catch (ObTableTryLockTimeoutException e) {
                    // if try lock timeout, need to retry
                    RUNTIME.warn("wait to try lock to timeout when refresh table meta, tryTimes: {}",
                            tryTimes, e);
                } catch (ObTableException ex) {
                    if (tableClient.getRowKeyElement(tableName) == null) {
                        // if the error is missing row key element, directly throw
                        throw ex;
                    }
                    if (tableClient.isRetryOnChangeMasterTimes()) {
                        if (ex.getErrorCode() == ResultCodes.OB_NOT_SUPPORTED.errorCode) {
                            RUNTIME.error("This version of ODP does not support for getPartition.");
                            throw ex;
                        }
                        RUNTIME
                                .warn(
                                        "meet exception while refreshing ODP table meta, need to retry. TableName: {}, exception: {}",
                                        tableName, ex.getMessage());
                        forceRenew = true; // force ODP to fetch the latest partition meta
                    } else {
                        RUNTIME
                                .error(
                                        "meet exception while refreshing ODP table meta. TableName: {}, exception: {}",
                                        tableName, ex.getMessage());
                        throw ex;
                    }
                } catch (Exception ex) {
                    RUNTIME
                            .error(
                                    "meet exception while refreshing ODP table meta. TableName: {}, exception: {}",
                                    tableName, ex.getMessage());
                    throw ex;
                }
            } // end while
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    private void checkODPPartitionMetaResult(long lastOdpCreateTimeMills,
                                             ObFetchPartitionMetaRequest request, ObPayload result) {
        if (result == null) {
            RUNTIME.error("client get unexpected NULL result");
            throw new ObTableException("client get unexpected NULL result");
        }

        if (!(result instanceof ObFetchPartitionMetaResult)) {
            RUNTIME.error("client get unexpected result: " + result.getClass().getName());
            throw new ObTableException("client get unexpected result: "
                                       + result.getClass().getName());
        }

        if (lastOdpCreateTimeMills != -1) {
            if (lastOdpCreateTimeMills >= ((ObFetchPartitionMetaResult) result).getCreateTime()) {
                throw new ObTableException("client get outdated result from ODP");
            }
        }
    }

}

/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
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

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.constant.Constants;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.get.Get;
import com.alipay.oceanbase.rpc.location.model.*;
import com.alipay.oceanbase.rpc.location.model.partition.*;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutate;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObBorderFlag;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncRequest;
import com.alipay.oceanbase.rpc.table.*;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.*;
import com.alipay.remoting.util.StringUtils;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.rpc.constant.Constants.*;
import static com.alipay.oceanbase.rpc.location.model.ObServerRoute.STRONG_READ;
import static com.alipay.oceanbase.rpc.property.Property.*;
import static com.alipay.oceanbase.rpc.protocol.payload.Constants.INVALID_TABLET_ID;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType.*;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;

public class ObTableClient extends AbstractObTableClient implements Lifecycle {
    private static final Logger                               logger                                  = getLogger(ObTableClient.class);

    private static final String                               usernameSeparators                      = ":;-;.";

    private String                                            dataSourceName;
    private String                                            paramURL;
    /*
     * user name
     * Standard format: user@tenant#cluster
     * NonStandard format: cluster:tenant:user
     */
    private String                                            fullUserName;
    private String                                            userName;
    private String                                            tenantName;
    private String                                            clusterName;
    private String                                            password;
    private String                                            database;

    /*
     * sys user auth to access meta table.
     */
    private ObUserAuth                                        sysUA                                   = new ObUserAuth(
                                                                                                          Constants.PROXY_SYS_USER_NAME,
                                                                                                          "");

    private TableRoute                                        tableRoute                              = null;

    private volatile RunningMode                              runningMode                             = RunningMode.NORMAL;

    /*
     * TableName -> rowKey element
     */
    private Map<String, Map<String, Integer>>                 tableRowKeyElement                      = new ConcurrentHashMap<String, Map<String, Integer>>();
    private boolean                                           retryOnChangeMasterTimes                = true;
    /*
     * TableName -> Failures/Lock
     */
    private ConcurrentHashMap<String, AtomicLong>             tableContinuousFailures                 = new ConcurrentHashMap<String, AtomicLong>();

    private volatile boolean                                  initialized                             = false;
    private volatile boolean                                  closed                                  = false;
    private ReentrantLock                                     statusLock                              = new ReentrantLock();

    private String                                            currentIDC;
    private ObReadConsistency                                 readConsistency                         = ObReadConsistency.STRONG;
    private ObRoutePolicy                                     obRoutePolicy                           = ObRoutePolicy.IDC_ORDER;

    private boolean                                           odpMode                                 = false;

    private String                                            odpAddr                                 = "127.0.0.1";

    private int                                               odpPort                                 = 2883;


    private Long                                              clientId;
    private Map<String, Object>                               TableConfigs                            = new HashMap<>();
    /*
     * Init.
     */
    public void init() throws Exception {
        if (initialized) {
            return;
        }
        statusLock.lock();
        try {
            if (initialized) {
                return;
            }
            // 1. init clientId
            clientId = Math.abs(UUID.randomUUID().getLeastSignificantBits());
            // 2. init table configs map
            initTableConfigs();
            // 3. init properties
            initProperties();
            // 4. init metadata
            initMetadata();
            initialized = true;
        } catch (Throwable t) {
            BOOT.warn("failed to init ObTableClient", t);
            RUNTIME.warn("failed to init ObTableClient", t);
            throw new RuntimeException(t);
        } finally {
            BOOT.info("init ObTableClient successfully");
            statusLock.unlock();
        }
    }

    /*
     * Close.
     */
    @Override
    public void close() throws Exception {
        if (closed) {
            return;
        }
        statusLock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
            if (tableRoute != null) {
                tableRoute.close();
                ObTable odpTable = tableRoute.getOdpTable();
                if (odpTable != null) {
                    odpTable.close();
                }
            }
        } finally {
            BOOT.info("ObTableClient is closed");
            statusLock.unlock();
        }
    }

    /*
     * Check status.
     */
    public void checkStatus() throws IllegalStateException {
        if (!initialized) {
            throw new IllegalStateException("param url " + paramURL + "fullUserName "
                                            + fullUserName + " is not initialized");
        }

        if (closed) {
            throw new IllegalStateException("param url " + paramURL + " fullUserName "
                                            + fullUserName + " is closed");
        }
    }

    public Long getClientId() {
        return clientId;
    }

    public Map<String, Object> getTableConfigs() {
        return TableConfigs;
    }

    private void initTableConfigs() {
        TableConfigs.put("client_id", clientId);
        TableConfigs.put("runtime", new HashMap<String, String>());
        TableConfigs.put("log", new HashMap<String, String>());
        TableConfigs.put("route", new HashMap<String, String>());
        TableConfigs.put("thread_pool", new HashMap<String, Boolean>());
    }

    private void initProperties() {
        rpcConnectTimeout = parseToInt(RPC_CONNECT_TIMEOUT.getKey(), rpcConnectTimeout);

        // metadata.refresh.interval is preferred.
        metadataRefreshInterval = parseToLong(METADATA_REFRESH_INTERVAL.getKey(),
            metadataRefreshInterval);
        metadataRefreshInterval = parseToLong(METADATA_REFRESH_INTERNAL.getKey(),
            metadataRefreshInterval);

        metadataRefreshLockTimeout = parseToLong(METADATA_REFRESH_LOCK_TIMEOUT.getKey(),
            metadataRefreshLockTimeout);

        rsListAcquireConnectTimeout = parseToInt(RS_LIST_ACQUIRE_CONNECT_TIMEOUT.getKey(),
            rsListAcquireConnectTimeout);

        rsListAcquireReadTimeout = parseToInt(RS_LIST_ACQUIRE_READ_TIMEOUT.getKey(),
            rsListAcquireReadTimeout);

        rsListAcquireTryTimes = parseToInt(RS_LIST_ACQUIRE_TRY_TIMES.getKey(),
            rsListAcquireTryTimes);

        // rs.list.acquire.retry.interval is preferred.
        rsListAcquireRetryInterval = parseToLong(RS_LIST_ACQUIRE_RETRY_INTERVAL.getKey(),
            rsListAcquireRetryInterval);
        rsListAcquireRetryInterval = parseToLong(RS_LIST_ACQUIRE_RETRY_INTERNAL.getKey(),
            rsListAcquireRetryInterval);

        tableEntryAcquireConnectTimeout = parseToLong(TABLE_ENTRY_ACQUIRE_CONNECT_TIMEOUT.getKey(),
            tableEntryAcquireConnectTimeout);

        tableEntryAcquireSocketTimeout = parseToLong(TABLE_ENTRY_ACQUIRE_SOCKET_TIMEOUT.getKey(),
            tableEntryAcquireSocketTimeout);

        // table.entry.refresh.interval.base is preferred.
        tableEntryRefreshIntervalBase = parseToLong(TABLE_ENTRY_REFRESH_INTERVAL_BASE.getKey(),
            tableEntryRefreshIntervalBase);
        tableEntryRefreshIntervalBase = parseToLong(TABLE_ENTRY_REFRESH_INTERNAL_BASE.getKey(),
            tableEntryRefreshIntervalBase);

        // table.entry.refresh.interval.ceiling is preferred.
        tableEntryRefreshIntervalCeiling = parseToLong(
            TABLE_ENTRY_REFRESH_INTERVAL_CEILING.getKey(), tableEntryRefreshIntervalCeiling);
        tableEntryRefreshIntervalCeiling = parseToLong(
            TABLE_ENTRY_REFRESH_INTERNAL_CEILING.getKey(), tableEntryRefreshIntervalCeiling);

        tableEntryRefreshIntervalWait = parseToBoolean(TABLE_ENTRY_REFRESH_INTERVAL_WAIT.getKey(),
            tableEntryRefreshIntervalWait);

        tableEntryRefreshLockTimeout = parseToLong(TABLE_ENTRY_REFRESH_LOCK_TIMEOUT.getKey(),
            tableEntryRefreshLockTimeout);

        ODPTableEntryRefreshLockTimeout = parseToLong(ODP_TABLE_ENTRY_REFRESH_LOCK_TIMEOUT.getKey(),
                ODPTableEntryRefreshLockTimeout);

        tableEntryRefreshTryTimes = parseToInt(TABLE_ENTRY_REFRESH_TRY_TIMES.getKey(),
            tableEntryRefreshTryTimes);

        tableEntryRefreshContinuousFailureCeiling = parseToInt(
            TABLE_ENTRY_REFRESH_CONTINUOUS_FAILURE_CEILING.getKey(),
            tableEntryRefreshContinuousFailureCeiling);

        serverAddressPriorityTimeout = parseToLong(SERVER_ADDRESS_PRIORITY_TIMEOUT.getKey(),
            serverAddressPriorityTimeout);

        serverAddressCachingTimeout = parseToLong(SERVER_ADDRESS_CACHING_TIMEOUT.getKey(),
            serverAddressCachingTimeout);

        runtimeContinuousFailureCeiling = parseToInt(RUNTIME_CONTINUOUS_FAILURE_CEILING.getKey(),
            runtimeContinuousFailureCeiling);

        this.runtimeRetryTimes = parseToInt(RUNTIME_RETRY_TIMES.getKey(), this.runtimeRetryTimes);

        runtimeRetryInterval = parseToInt(RUNTIME_RETRY_INTERVAL.getKey(), runtimeRetryInterval);

        runtimeMaxWait = parseToLong(RUNTIME_MAX_WAIT.getKey(), runtimeMaxWait);

        runtimeBatchMaxWait = parseToLong(RUNTIME_BATCH_MAX_WAIT.getKey(), runtimeBatchMaxWait);

        rpcExecuteTimeout = parseToInt(RPC_EXECUTE_TIMEOUT.getKey(), rpcExecuteTimeout);

        rpcLoginTimeout = parseToInt(RPC_LOGIN_TIMEOUT.getKey(), rpcLoginTimeout);

        slowQueryMonitorThreshold = parseToLong(SLOW_QUERY_MONITOR_THRESHOLD.getKey(),
            slowQueryMonitorThreshold);
        maxConnExpiredTime = parseToLong(MAX_CONN_EXPIRED_TIME.getKey(), maxConnExpiredTime);


        // add configs value to TableConfigs

        // runtime
        Object value = TableConfigs.get("runtime");
        if (value instanceof Map) {
            Map<String, String> runtimeMap = (Map<String, String>) value;
            runtimeMap.put(RUNTIME_RETRY_TIMES.getKey(), String.valueOf(runtimeRetryTimes));
            runtimeMap.put(RPC_EXECUTE_TIMEOUT.getKey(), String.valueOf(rpcExecuteTimeout));
            runtimeMap.put(RUNTIME_MAX_WAIT.getKey(), String.valueOf(runtimeMaxWait));
            runtimeMap.put(RUNTIME_RETRY_INTERVAL.getKey(), String.valueOf(runtimeRetryInterval));
            runtimeMap.put(RUNTIME_RETRY_TIMES.getKey(), String.valueOf(runtimeRetryTimes));
            runtimeMap.put(MAX_CONN_EXPIRED_TIME.getKey(), String.valueOf(maxConnExpiredTime));
        }
        // log
        value = TableConfigs.get("log");
        if (value instanceof Map) {
            Map<String, String> logMap = (Map<String, String>) value;
            logMap.put(SLOW_QUERY_MONITOR_THRESHOLD.getKey(), String.valueOf(slowQueryMonitorThreshold));
        }

        value = TableConfigs.get("route");
        if (value instanceof Map) {
            Map<String, String> routeMap = (Map<String, String>) value;
            routeMap.put(METADATA_REFRESH_INTERVAL.getKey(), String.valueOf(metadataRefreshInterval));
            routeMap.put(RUNTIME_CONTINUOUS_FAILURE_CEILING.getKey(), String.valueOf(runtimeContinuousFailureCeiling));
            routeMap.put(SERVER_ADDRESS_CACHING_TIMEOUT.getKey(), String.valueOf(serverAddressCachingTimeout));
            routeMap.put(SERVER_ADDRESS_PRIORITY_TIMEOUT.getKey(), String.valueOf(serverAddressPriorityTimeout));
            routeMap.put(TABLE_ENTRY_ACQUIRE_CONNECT_TIMEOUT.getKey(), String.valueOf(tableEntryAcquireConnectTimeout));
            routeMap.put(TABLE_ENTRY_ACQUIRE_SOCKET_TIMEOUT.getKey(), String.valueOf(tableEntryAcquireSocketTimeout));
            routeMap.put(TABLE_ENTRY_REFRESH_INTERVAL_BASE.getKey(), String.valueOf(tableEntryRefreshIntervalBase));
            routeMap.put(TABLE_ENTRY_REFRESH_INTERVAL_CEILING.getKey(), String.valueOf(tableEntryRefreshIntervalCeiling));
            routeMap.put(TABLE_ENTRY_REFRESH_TRY_TIMES.getKey(), String.valueOf(tableEntryRefreshTryTimes));
        }
        Boolean useExecutor = false;
        if (runtimeBatchExecutor != null) {
           useExecutor = true;
        }

        value = TableConfigs.get("thread_pool");
        if (value instanceof Map) {
            Map<String, Boolean> threadPoolMap = (Map<String, Boolean>) value;
            threadPoolMap.put(RUNTIME_BATCH_EXECUTOR.getKey(), useExecutor);
        }
    }

    private void initMetadata() throws Exception {
        BOOT.info("begin initMetadata for all tables in database: {}", this.database);
        this.tableRoute = new TableRoute(this, sysUA);

        if (odpMode) {
            try {
                ObTableClientType clientType = runningMode == RunningMode.HBASE ? ObTableClientType.JAVA_HBASE_CLIENT : ObTableClientType.JAVA_TABLE_CLIENT;
                tableRoute.buildOdpInfo(odpAddr, odpPort, clientType);
            } catch (Exception e) {
                logger
                        .warn(
                                "The addr{}:{} failed to put into table roster, the node status may be wrong, Ignore",
                                odpAddr, odpPort);
                throw e;
            }
            return;
        }
        // build ConfigServerInfo to get rsList
        tableRoute.loadConfigServerInfo();

        // build tableRoster and ServerRoster
        TableEntryKey rootServerKey = new TableEntryKey(clusterName, tenantName,
                OCEANBASE_DATABASE, ALL_DUMMY_TABLE);
        tableRoute.initRoster(rootServerKey, initialized, runningMode);
        // create background refresh-checker task
        tableRoute.launchRouteRefresher();
    }

    public boolean isOdpMode() {
        return odpMode;
    }

    public void setOdpMode(boolean odpMode) {
        this.odpMode = odpMode;
    }

    public ObTable getOdpTable() {
        if (tableRoute != null) {
            return tableRoute.getOdpTable();
        }
        return null;
    }

    private abstract class TableExecuteCallback<T> {
        private final Object[] rowKey;

        TableExecuteCallback(Object[] rowKey) {
            this.rowKey = rowKey;
        }

        void checkObTableOperationResult(String ip, int port, ObPayload request, ObPayload result) {

            if (result == null) {
                RUNTIME.error("client get unexpected NULL result");
                throw new ObTableException("client get unexpected NULL result");
            }

            if (!(result instanceof ObTableOperationResult)) {
                RUNTIME.error("client get unexpected result: " + result.getClass().getName());
                throw new ObTableException("client get unexpected result: "
                                           + result.getClass().getName());
            }

            ObTableOperationResult obTableOperationResult = (ObTableOperationResult) result;
            ObTableOperationRequest obTableOperationRequest = (ObTableOperationRequest) request;
            obTableOperationResult.setExecuteHost(ip);
            obTableOperationResult.setExecutePort(port);
            long sequence = obTableOperationResult.getSequence() == 0 ? obTableOperationRequest
                .getSequence() : obTableOperationResult.getSequence();
            long uniqueId = obTableOperationResult.getUniqueId() == 0 ? obTableOperationRequest
                .getUniqueId() : obTableOperationResult.getUniqueId();
            ExceptionUtil.throwObTableException(ip, port, sequence, uniqueId,
                obTableOperationResult.getHeader().getErrno(), obTableOperationResult.getHeader()
                    .getErrMsg());
        }

        void checkObTableQueryAndMutateResult(String ip, int port, ObPayload result) {

            if (result == null) {
                RUNTIME.error("client get unexpected NULL result");
                throw new ObTableException("client get unexpected NULL result");
            }

            if (!(result instanceof ObTableQueryAndMutateResult)) {
                RUNTIME.error("client get unexpected result: " + result.getClass().getName());
                throw new ObTableException("client get unexpected result: "
                                           + result.getClass().getName());
            }
            // TODO: Add func like throwObTableException()
            //       which will output the ip / port / error information
        }

        abstract T execute(ObTableParam tableParam) throws Exception;

        /*
         * Get row key.
         */
        public Object[] getRowKey() {
            return this.rowKey;
        }
    }

    private <T> T execute(String tableName, TableExecuteCallback<T> callback) throws Exception {
        // force strong read by default, for backward compatibility.
        return execute(tableName, callback, getRoute(false));
    }

    /**
     * Execute with a route strategy.
     */
    private <T> T execute(String tableName, TableExecuteCallback<T> callback, ObServerRoute route)
                                                                                                  throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        int tryTimes = 0;
        boolean needRefreshPartitionLocation = false;
        long startExecute = System.currentTimeMillis();
        Row rowKey = transformToRow(tableName, callback.getRowKey());
        while (true) {
            checkStatus();
            long currentExecute = System.currentTimeMillis();
            long costMillis = currentExecute - startExecute;
            if (costMillis > runtimeMaxWait) {
                throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                                                 + " times and it has waited " + costMillis
                                                 + "/ms which exceeds response timeout "
                                                 + runtimeMaxWait + "/ms");
            }
            tryTimes++;
            ObTableParam tableParam = null;
            try {
                if (odpMode) {
                    ObTable odpTable = tableRoute.getOdpTable();
                    tableParam = new ObTableParam(odpTable);
                } else {
                    if (tryTimes > 1 && needRefreshPartitionLocation) {
                        // refresh partition location
                        TableEntry entry = tableRoute.getTableEntry(tableName);
                        long partId = tableRoute.getPartId(entry, rowKey);
                        long tabletId = tableRoute.getTabletIdByPartId(entry, partId);
                        tableRoute.refreshPartitionLocation(tableName, tabletId, entry);
                    }
                    tableParam = getTableParamWithRoute(tableName, rowKey, route);
                }
                T t = callback.execute(tableParam);
                resetExecuteContinuousFailureCount(tableName);
                return t;
            } catch (Exception ex) {
                if (odpMode) {
                    // about routing problems, ODP will retry on their side
                    if (ex instanceof ObTableException) {
                        // errors needed to retry will retry until timeout
                        if (((ObTableException) ex).isNeedRetryServerError()) {
                            logger.warn(
                                    "execute while meet server error in odp mode, need to retry, errorCode: {} , errorMsg: {}, try times {}",
                                    ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                    tryTimes);
                        } else {
                            logger.warn("meet table exception when execute in odp mode." +
                                    "tablename: {}, errMsg: {}", tableName, ex.getMessage());
                            throw ex;
                        }
                    } else {
                        logger.warn("meet exception when execute in odp mode." +
                                "tablename: {}, errMsg: {}", tableName, ex.getMessage());
                        throw ex;
                    }
                } else {
                    needRefreshPartitionLocation = true;
                    if (ex instanceof ObTableReplicaNotReadableException) {
                        if (tableParam != null && System.currentTimeMillis() - startExecute < runtimeMaxWait) {
                            logger.warn("retry when replica not readable: {}", ex.getMessage());
                            route.addToBlackList(tableParam.getObTable().getIp());
                        } else {
                            logger.warn("retry to timeout when replica not readable: {}",
                                ex.getMessage());
                            RUNTIME.error("replica not readable", ex);
                            throw ex;
                        }
                    } else if (ex instanceof ObTableException
                            && (((ObTableException) ex).isNeedRefreshTableEntry() || ((ObTableException) ex).isNeedRetryServerError())) {
                        if (ex instanceof ObTableNotExistException) {
                            String logMessage = String.format(
                                    "exhaust retry while meet TableNotExist Exception, table name: %s, errorCode: %d",
                                    tableName,
                                    ((ObTableException) ex).getErrorCode()
                            );
                            logger.warn(logMessage, ex);
                            throw ex;
                        }
                        if (retryOnChangeMasterTimes) {
                            if (ex instanceof ObTableNeedFetchMetaException) {
                                tableRoute.refreshMeta(tableName);
                                // reset failure count while fetch all route info
                                this.resetExecuteContinuousFailureCount(tableName);
                            } else if (((ObTableException) ex).isNeedRetryServerError()) {
                                // retry server errors, no need to refresh partition location
                                needRefreshPartitionLocation = false;
                                logger.warn(
                                        "execute while meet server error, need to retry, errorCode: {} , errorMsg: {}, try times {}",
                                        ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                        tryTimes);
                            }
                        } else {
                            String logMessage = String.format(
                                    "retry is disabled while meet NeedRefresh Exception, table name: %s, errorCode: %d",
                                    tableName,
                                    ((ObTableException) ex).getErrorCode()
                            );
                            logger.warn(logMessage, ex);
                            calculateContinuousFailure(tableName, ex.getMessage());
                            throw ex;
                        }
                    } else {
                        if (ex instanceof ObTableTransportException &&
                                ((ObTableTransportException) ex).getErrorCode() == TransportCodes.BOLT_TIMEOUT) {
                            syncRefreshMetadata(true);
                        }
                        String logMessage;
                        if (ex instanceof ObTableException) {
                            logMessage = String.format(
                                    "exhaust retry while meet Exception, table name: %s, batch ops refresh table, errorCode: %d",
                                    tableName,
                                    ((ObTableException) ex).getErrorCode()
                            );
                        } else {
                            logMessage = String.format(
                                    "exhaust retry while meet Exception, table name: %s, batch ops refresh table",
                                    tableName
                            );
                        }
                        logger.warn(logMessage, ex);
                        calculateContinuousFailure(tableName, ex.getMessage());
                        throw ex;
                    }
                }
            }
            Thread.sleep(runtimeRetryInterval);
        }
    }

    private abstract class OperationExecuteCallback<T> {
        private final Row              rowKey;
        private final TableQuery       query;

        OperationExecuteCallback(Row rowKey, TableQuery query) {
            this.rowKey = rowKey;
            this.query = query;
        }

        void checkResult(String ip, int port, ObPayload request, ObPayload result) {
            if (result == null) {
                RUNTIME.error("client get unexpected NULL result");
                throw new ObTableException("client get unexpected NULL result");
            }

            if (result instanceof ObTableOperationResult) {
                ObTableOperationResult obTableOperationResult = (ObTableOperationResult) result;
                ObTableOperationRequest obTableOperationRequest = (ObTableOperationRequest) request;
                obTableOperationResult.setExecuteHost(ip);
                obTableOperationResult.setExecutePort(port);
                long sequence = obTableOperationResult.getSequence() == 0 ? obTableOperationRequest
                    .getSequence() : obTableOperationResult.getSequence();
                long uniqueId = obTableOperationResult.getUniqueId() == 0 ? obTableOperationRequest
                    .getUniqueId() : obTableOperationResult.getUniqueId();
                ExceptionUtil.throwObTableException(ip, port, sequence, uniqueId,
                    obTableOperationResult.getHeader().getErrno(), obTableOperationResult
                        .getHeader().getErrMsg());
            } else if (result instanceof ObTableQueryAndMutateResult) {
                // TODO: Add func like throwObTableException()
                //       which will output the ip / port / error information
            } else {
                RUNTIME.error("client get unexpected result: " + result.getClass().getName());
                throw new ObTableException("client get unexpected result: "
                                           + result.getClass().getName());
            }
        }

        abstract T execute(ObTableParam tableParam) throws Exception;

        /*
         * Get row key.
         */
        public Row getRowKey() {
            return rowKey;
        }

        /*
         * Get key ranges.
         */
        public TableQuery getQuery() {
            return query;
        }

    }

    /**
     * For mutation
     */
    private <T> T execute(String tableName, OperationExecuteCallback<T> callback)
                                                                                        throws Exception {
        // force strong read by default, for backward compatibility.
        return execute(tableName, callback, getRoute(false));
    }

    /**
     * Execute with a route strategy for mutation
     */
    private <T> T execute(String tableName, OperationExecuteCallback<T> callback,
                                  ObServerRoute route) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        int tryTimes = 0;
        boolean needRefreshPartitionLocation = false;
        long startExecute = System.currentTimeMillis();
        while (true) {
            checkStatus();
            long currentExecute = System.currentTimeMillis();
            long costMillis = currentExecute - startExecute;
            if (costMillis > runtimeMaxWait) {
                throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                                                 + " times and it has waited " + costMillis
                                                 + "/ms which exceeds response timeout "
                                                 + runtimeMaxWait + "/ms");
            }
            tryTimes++;
            ObTableParam tableParam = null;
            try {
                if (odpMode) {
                    if (null == callback.getRowKey() && null == callback.getQuery()) {
                        throw new ObTableException("RowKey or scan range is null");
                    }
                    ObTable odpTable = tableRoute.getOdpTable();
                    tableParam = new ObTableParam(odpTable);
                } else {
                    if (null != callback.getRowKey()) {
                        if (tryTimes > 1 && needRefreshPartitionLocation) {
                            // refresh partition location
                            TableEntry entry = tableRoute.getTableEntry(tableName);
                            long partId = tableRoute.getPartId(entry, callback.getRowKey());
                            long tabletId = tableRoute.getTabletIdByPartId(entry, partId);
                            tableRoute.refreshPartitionLocation(tableName, tabletId, entry);
                        }
                        // using row key
                        tableParam = tableRoute.getTableParamWithRoute(tableName, callback.getRowKey(), route);
                    } else if (null != callback.getQuery()) {
                        ObTableQuery tableQuery = callback.getQuery().getObTableQuery();
                        // using scan range
                        tableParam = tableRoute.getTableParam(tableName, tableQuery.getScanRangeColumns(),
                            tableQuery.getKeyRanges());
                    } else {
                        throw new ObTableException("RowKey or scan range is null");
                    }
                }
                T t = callback.execute(tableParam);
                resetExecuteContinuousFailureCount(tableName);
                return t;
            } catch (Exception ex) {
                RUNTIME.error("execute while meet exception", ex);
                if (odpMode) {
                    // about routing problems, ODP will retry on their side
                    if (ex instanceof ObTableException) {
                        // errors needed to retry will retry until timeout
                        if (((ObTableException) ex).isNeedRetryServerError()) {
                            logger.warn(
                                    "execute while meet server error in odp mode, need to retry, errorCode: {} , errorMsg: {}, try times {}",
                                    ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                    tryTimes);
                        } else {
                            logger.warn("meet table exception when execute in odp mode." +
                                    "tablename: {}, errMsg: {}", tableName, ex.getMessage());
                            throw ex;
                        }
                    } else {
                        logger.warn("meet exception when execute in odp mode." +
                                "tablename: {}, errMsg: {}", tableName, ex.getMessage());
                        throw ex;
                    }
                } else {
                    needRefreshPartitionLocation = true;
                    if (ex instanceof ObTableReplicaNotReadableException) {
                        if (tableParam != null && System.currentTimeMillis() - startExecute > runtimeMaxWait) {
                            logger.warn("retry when replica not readable: {}", ex.getMessage());
                            route.addToBlackList(tableParam.getObTable().getIp());
                        } else {
                            logger.warn("retry to timeout when replica not readable: {}", ex.getMessage());
                            RUNTIME.error("replica not readable", ex);
                            throw ex;
                        }
                    } else if (ex instanceof ObTableException
                            && (((ObTableException) ex).isNeedRefreshTableEntry() || ((ObTableException) ex).isNeedRetryServerError())) {
                        if (ex instanceof ObTableNotExistException) {
                            String logMessage = String.format(
                                    "exhaust retry while meet TableNotExist Exception, table name: %s, errorCode: %d",
                                    tableName,
                                    ((ObTableException) ex).getErrorCode()
                            );
                            logger.warn(logMessage, ex);
                            throw ex;
                        }
                        if (retryOnChangeMasterTimes) {
                            if (ex instanceof ObTableNeedFetchMetaException) {
                                logger.warn("execute while meet need fetch meta error, need to retry, errorCode: {} , errorMsg: {}, try times {}",
                                        ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                        tryTimes);
                                tableRoute.refreshMeta(tableName);
                                // reset failure count while fetch all route info
                                this.resetExecuteContinuousFailureCount(tableName);
                            } else if (((ObTableException) ex).isNeedRetryServerError()) {
                                // retry server errors, no need to refresh partition location
                                needRefreshPartitionLocation = false;
                                logger.warn(
                                        "execute while meet server error, need to retry, errorCode: {} , errorMsg: {}, try times {}",
                                        ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                        tryTimes);
                            }
                        } else {
                            String logMessage = String.format(
                                    "retry is disabled while meet NeedRefresh Exception, table name: %s, errorCode: %d",
                                    tableName,
                                    ((ObTableException) ex).getErrorCode()
                            );
                            logger.warn(logMessage, ex);
                            calculateContinuousFailure(tableName, ex.getMessage());
                            throw new ObTableRetryExhaustedException(logMessage, ex);
                        }
                    } else {
                        if (ex instanceof ObTableTransportException &&
                                ((ObTableTransportException) ex).getErrorCode() == TransportCodes.BOLT_TIMEOUT) {
                            syncRefreshMetadata(true);
                        }
                        String logMessage;
                        if (ex instanceof ObTableException) {
                            logMessage = String.format(
                                    "exhaust retry while meet Exception, table name: %s, batch ops refresh table, errorCode: %d",
                                    tableName,
                                    ((ObTableException) ex).getErrorCode()
                            );
                        } else {
                            logMessage = String.format(
                                    "exhaust retry while meet Exception, table name: %s, batch ops refresh table",
                                    tableName
                            );
                        }
                        logger.warn(logMessage, ex);
                        calculateContinuousFailure(tableName, ex.getMessage());
                        throw ex;
                    }
                }
            }
            Thread.sleep(runtimeRetryInterval);
        }
    }

    /**
     * Calculate continuous failure.
     * @param tableName table name
     * @param errorMsg err msg
     * @throws Exception if failed
     */
    public void calculateContinuousFailure(String tableName, String errorMsg) throws Exception {
        AtomicLong tempFailures = new AtomicLong();
        AtomicLong failures = tableContinuousFailures.putIfAbsent(tableName, tempFailures);
        failures = (failures == null) ? tempFailures : failures; // check the first failure
        if (failures.incrementAndGet() > runtimeContinuousFailureCeiling) {
            logger.warn("refresh table entry {} while execute failed times exceeded {}, msg: {}",
                tableName, runtimeContinuousFailureCeiling, errorMsg);
            refreshMeta(tableName);
            failures.set(0);
        } else {
            logger.warn("error msg: {}, current continues failure count: {}", errorMsg, failures);
        }
    }

    /**
     * Reset execute continuous failure count.
     * @param tableName table name
     */
    public void resetExecuteContinuousFailureCount(String tableName) {
        AtomicLong failures = tableContinuousFailures.get(tableName);
        if (failures != null) {
            failures.set(0);
        }
    }

    /**
     * refresh all ob server synchronized just in case rslist has changed, it will not refresh if last refresh time is 1 min ago
     * @param forceRenew flag to force refresh the rsList if changes happen
     * 1. cannot find table from tables, need refresh tables
     * 2. server list refresh failed: {see com.alipay.oceanbase.obproxy.resource.ObServerStateProcessor#MAX_REFRESH_FAILURE}
     *
     * @throws Exception if fail
     */
    public void syncRefreshMetadata(boolean forceRenew) throws Exception {// do not refresh within 5 seconds even if forceRenew
        checkStatus();
        long lastRefreshMetadataTimestamp = tableRoute.getLastRefreshMetadataTimestamp();
        if (System.currentTimeMillis() - lastRefreshMetadataTimestamp < 5000L) {
            logger
                    .warn(
                            "have to wait for more than 5 seconds to refresh metadata, it has refreshed at: {}",
                            lastRefreshMetadataTimestamp);
            return;
        }
        if (!forceRenew
                && System.currentTimeMillis() - lastRefreshMetadataTimestamp < metadataRefreshInterval) {
            logger
                    .warn(
                            "try to refresh metadata but need to wait, it has refreshed at: {}, dataSourceName: {}, url: {}",
                            lastRefreshMetadataTimestamp, dataSourceName, paramURL);
            return;
        }
        Lock refreshMetaLock = tableRoute.refreshTableRosterLock;
        boolean acquired = refreshMetaLock.tryLock(
                metadataRefreshLockTimeout, TimeUnit.MILLISECONDS);
        if (!acquired) {
            String errMsg = "try to lock rsList refreshing timeout " + " refresh timeout: "
                    + metadataRefreshLockTimeout + ".";
            RUNTIME.error(errMsg);
            // if not acquire lock, means that another thread is refreshing rsList and table roster
            // after refreshing, new table roster can be shared by all threads
            return;
        }
        try {
            // double check timestamp
            if (!forceRenew
                    && System.currentTimeMillis() - lastRefreshMetadataTimestamp < metadataRefreshInterval) {
                logger
                        .warn(
                                "try to refresh metadata but need to wait, it has refreshed at: {}, dataSourceName: {}, url: {}",
                                lastRefreshMetadataTimestamp, dataSourceName, paramURL);
                return;
            }
            ConfigServerInfo newConfigServer = tableRoute.loadConfigServerInfo();
            tableRoute.refreshRosterByRsList(newConfigServer.getRsList());
        } finally {
            refreshMetaLock.unlock();
        }
    }

    /*
     * return the table name that need get location
     * for global index: return global index table name
     * others: return primary table name
     * @param dataTableName table name
     * @param indexName used index name
     * @param scanRangeColumns columns that need to be scaned
     * @return the real table name
     */
    public String getIndexTableName(final String dataTableName, final String indexName,
                                    List<String> scanRangeColumns, boolean forceRefreshIndexInfo)
                                                                                                 throws Exception {
        return tableRoute.getIndexTableName(dataTableName, indexName, scanRangeColumns, forceRefreshIndexInfo);
    }

    public void eraseTableEntry(String tableName) {
        tableRoute.eraseTableEntry(tableName);
    }

    @Override
    public void setRpcExecuteTimeout(int rpcExecuteTimeout) {
        this.properties.put(RPC_EXECUTE_TIMEOUT.getKey(), String.valueOf(rpcExecuteTimeout));
        this.rpcExecuteTimeout = rpcExecuteTimeout;
        if (tableRoute != null) {
            ConcurrentHashMap<ObServerAddr, ObTable> tableRoster = tableRoute.getTableRoster().getTables();
            if (null != tableRoster) {
                for (ObTable obTable : tableRoster.values()) {
                    if (obTable != null) {
                        obTable.setObTableExecuteTimeout(rpcExecuteTimeout);
                    }
                }
            }
            ObTable odpTable = tableRoute.getOdpTable();
            if (null != odpTable) {
                odpTable.setObTableExecuteTimeout(rpcExecuteTimeout);
            }
        }
    }

    /**
     * Get or refresh table entry meta information.
     * work for both OcpMode and OdpMode
     * @param tableName table name
     * @return TableEntry
     * @throws Exception if fail
     */
    public TableEntry getOrRefreshTableEntry(final String tableName, boolean forceRefresh)

            throws Exception {
        if (!forceRefresh) {
            return tableRoute.getTableEntry(tableName);
        }
        return refreshMeta(tableName);
    }

    /**
     * refresh table meta information except location
     * work for both OcpMode and OdpMode
     * @param tableName table name
     * */
    private TableEntry refreshMeta(String tableName) throws Exception {
        if (odpMode) {
            return tableRoute.refreshODPMeta(tableName, true);
        } else {
            return tableRoute.refreshMeta(tableName);
        }
    }

    /**
     * Refresh tablet location by tabletId
     * @param tableName table name
     * @param tabletId real tablet id
     * @return TableEntry
     * @throws Exception
     * */
    public TableEntry refreshTableLocationByTabletId(String tableName, Long tabletId) throws Exception {
        return tableRoute.refreshPartitionLocation(tableName, tabletId, null);
    }

    public long getTabletIdByPartId(TableEntry tableEntry, Long partId) {
        return tableRoute.getTabletIdByPartId(tableEntry, partId);
    }

    public TableEntry refreshTabletLocationBatch(String tableName) throws Exception {
        return tableRoute.refreshTabletLocationBatch(tableName);
    }

    /**
     * this method is designed for the old single operations
     * @param tableName table want to get
     * @param rowkey row key
     * @return table param
     * @throws Exception exception
     */
    public ObTableParam getTableParam(String tableName, Object[] rowkey)
                                                                                     throws Exception {
        ObServerRoute route = getRoute(false);
        Row row = transformToRow(tableName, rowkey);
        return tableRoute.getTableParamWithRoute(tableName, row, route);
    }

    /**
     * this method is designed for the old single operations
     * @param tableName table want to get
     * @param rowkey row key
     * @return table param
     * @throws Exception exception
     */
    public ObTableParam getTableParamWithRoute(String tableName, Object[] rowkey, ObServerRoute route)
            throws Exception {
        Row row = transformToRow(tableName, rowkey);
        return tableRoute.getTableParamWithRoute(tableName, row, route);
    }

    /**
     * this method is designed for the old single operations
     * route is provided from old single operation execution and non-LS BatchOpsImpl
     * @param tableName table want to get
     * @param rowkey row key
     * @param route server route choice
     * @return table param
     * @throws Exception exception
     */
    public ObTableParam getTableParamWithRoute(String tableName, Row rowkey, ObServerRoute route)
            throws Exception {
        return tableRoute.getTableParamWithRoute(tableName, rowkey, route);
    }

    /**
     * this method is designed for batch operations
     * get all tableParams by rowKeys
     * @param tableName table want to get
     * @param rowkeys list of row key
     * @return table param
     * @throws Exception exception
     */
    public List<ObTableParam> getTableParams(String tableName, List<Row> rowkeys) throws Exception {
        return tableRoute.getTableParams(tableName, rowkeys);
    }

    /**
     * 根据 start-end 获取 partition ids 和 addrs
     * @param tableName table want to get
     * @param query query
     * @param start start key
     * @param startInclusive whether include start key
     * @param end end key
     * @param endInclusive whether include end key
     * @return list of table obTableParams
     * @throws Exception exception
     */
    public List<ObTableParam> getTableParams(String tableName, ObTableQuery query,
                                             Object[] start, boolean startInclusive,
                                             Object[] end, boolean endInclusive)
            throws Exception {
        return tableRoute.getTableParams(tableName, query, start, startInclusive, end, endInclusive);
    }

    /**
     * get addr by pardId
     * @param tableName table want to get
     * @param partId logic of table
     * @param route ObServer route
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    public ObTableParam getTableParamWithPartId(String tableName, long partId, ObServerRoute route)
                                                                                                   throws Exception {
        return tableRoute.getTableWithPartId(tableName, partId, route);
    }

    /**
     *
     * @param moveResponse reRoute response
     * @return
     */
    public ObTable getTable(ObTableApiMove moveResponse) throws Exception {
        ObServerAddr addr = new ObServerAddr();
        addr.setIp(moveResponse.getReplica().getServer().ipToString());
        addr.setSvrPort(moveResponse.getReplica().getServer().getPort());
        logger.info("get new server ip {}, port {} from move response ", addr.getIp(), addr.getSvrPort());

        ConcurrentHashMap<ObServerAddr, ObTable> tableRoster = tableRoute.getTableRoster().getTables();
        for (Map.Entry<ObServerAddr, ObTable> entry: tableRoster.entrySet()){
            if (Objects.equals(entry.getKey().getIp(), addr.getIp()) && Objects.equals(entry.getKey().getSvrPort(), addr.getSvrPort())){
                return entry.getValue();
            }
        }
       // If the node address does not exist, a new table is created
       return addTable(addr);
    }

    public ObTable addTable(ObServerAddr addr){

        ConcurrentHashMap<ObServerAddr, ObTable> tableRoster = tableRoute.getTableRoster().getTables();
        try {
            logger.info("server from response not exist in route cache, server ip {}, port {} , execute add Table.", addr.getIp(), addr.getSvrPort());
            ObTable obTable = new ObTable.Builder(addr.getIp(), addr.getSvrPort()) //
                    .setLoginInfo(tenantName, userName, password, database, getClientType(runningMode)) //
                    .setProperties(getProperties()).build();
            tableRoster.put(addr, obTable);
            return obTable;
        } catch (Exception e) {
            BOOT.warn(
                    "The addr{}:{} failed to put into table roster, the node status may be wrong, Ignore",
                    addr.getIp(), addr.getSvrPort());
            RUNTIME.warn("Get table from API_MOVE response ip and port meet exception", e);
            e.printStackTrace();
        }
        return null;
    }

    public Row transformToRow(String tableName, Object[] rowkey) throws Exception {
        TableEntry tableEntry = tableRoute.getTableEntry(tableName);
        Row row = new Row();
        if (tableEntry.isPartitionTable()) {
            List<String> curTableRowKeyNames = new ArrayList<String>();
            Map<String, Integer> tableRowKeyEle = getRowKeyElement(tableName);
            if (tableRowKeyEle != null) {
                curTableRowKeyNames = new ArrayList<String>(tableRowKeyEle.keySet());
            }
            if (curTableRowKeyNames.isEmpty()) {
                throw new IllegalArgumentException("Please make sure add row key elements");
            }

            // match the correct key to its row key
            for (int i = 0; i < rowkey.length; ++i) {
                if (i < curTableRowKeyNames.size()) {
                    row.add(curTableRowKeyNames.get(i), rowkey[i]);
                } else { // the rowKey element in the table only contain partition key(s) or the input row key has redundant elements
                    break;
                }
            }
        }
        return row;
    }

    /**
     * get table name with table group
     * @param tableGroupName table group name
     * @param refresh if refresh or not
     * @return actual table name
     * @throws Exception exception
     */
    public String tryGetTableNameFromTableGroupCache(final String tableGroupName,
                                                     final boolean refresh) throws Exception {
        return tableRoute.tryGetTableNameFromTableGroupCache(tableGroupName, refresh);
    }

    /**
     * Aggregate.
     * @param tableName table want to aggregate
     * @return ObTableAggregation object
     */
    public ObTableAggregation aggregate(String tableName) {
        ObTableClientQueryImpl tableQuery = new ObTableClientQueryImpl(tableName, this);
        ObClusterTableQuery clusterTableQuery = new ObClusterTableQuery(tableQuery);
        return new ObTableAggregation(clusterTableQuery);
    }

    /**
     * Query.
     */
    @Override
    public TableQuery query(String tableName) {
        ObTableClientQueryImpl tableQuery = new ObTableClientQueryImpl(tableName, this);

        return new ObClusterTableQuery(tableQuery);
    }

    /**
     * Batch.
     */
    @Override
    public TableBatchOps batch(String tableName) {
        ObTableClientBatchOpsImpl batchOps = new ObTableClientBatchOpsImpl(tableName, this);

        return new ObClusterTableBatchOps(runtimeBatchExecutor, batchOps);
    }

    @Override
    public Map<String, Object> get(final String tableName, final Object[] rowKey,
                                   final String[] columns) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        final long startTime = System.currentTimeMillis();
        final ObReadConsistency obReadConsistency = this.getReadConsistency();
        return execute(tableName, new TableExecuteCallback<Map<String, Object>>(rowKey) {
            @Override
            public Map<String, Object> execute(ObTableParam tableParam) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName,
                    GET, rowKey, columns, null, obTable.getObTableOperationTimeout());
                request.setTableId(tableParam.getTableId());
                // partId/tabletId
                request.setPartitionId(tableParam.getPartitionId());
                request.setConsistencyLevel(obReadConsistency.toObTableConsistencyLevel());
                ObPayload result = executeWithRetry(obTable, request, tableName);
                checkObTableOperationResult(obTable.getIp(), obTable.getPort(), request, result);

                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableName, "GET", endpoint, rowKey,
                    (ObTableOperationResult) result, getTableTime - startTime,
                    System.currentTimeMillis() - getTableTime, getslowQueryMonitorThreshold());
                return ((ObTableOperationResult) result).getEntity().getSimpleProperties();
            }
        }, getReadRoute());
    }

    /**
     * Update.
     */
    public Update update(String tableName) {
        return new Update(this, tableName);
    }

    /**
     * Update.
     */
    @Override
    public long update(final String tableName, final Object[] rowKey, final String[] columns,
                       final Object[] values) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        final long start = System.currentTimeMillis();
        return execute(tableName, new TableExecuteCallback<Long>(rowKey) {
            /**
             * Execute.
             */
            @Override
            public Long execute(ObTableParam tableParam) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName,
                    UPDATE, rowKey, columns, values, obTable.getObTableOperationTimeout());
                request.setTableId(tableParam.getTableId());
                // partId/tabletId
                request.setPartitionId(tableParam.getPartitionId());
                ObPayload result = executeWithRetry(obTable, request, tableName);
                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableName, "UPDATE", endpoint, rowKey,
                    (ObTableOperationResult) result, getTableTime - start,
                    System.currentTimeMillis() - getTableTime, getslowQueryMonitorThreshold());
                checkObTableOperationResult(obTable.getIp(), obTable.getPort(), request, result);
                return ((ObTableOperationResult) result).getAffectedRows();
            }
        });
    }

    /**
     * Update with result
     * @param tableName which table to update
     * @param rowKey update row key
     * @param columns columns name to update
     * @param values new values
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload updateWithResult(final String tableName, final Row rowKey,
                                      final String[] columns,
                                      final Object[] values) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, null) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObTableParam tableParam) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTable obTable = tableParam.getObTable();
                    ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                        tableName, UPDATE, rowKey.getValues(), columns, values,
                        obTable.getObTableOperationTimeout());
                    request.setTableId(tableParam.getTableId());
                    // partId/tabletId
                    request.setPartitionId(tableParam.getPartitionId());
                    ObPayload result = executeWithRetry(obTable, request, tableName);
                    String endpoint = obTable.getIp() + ":" + obTable.getPort();
                    MonitorUtil.info(request, database, tableName, "UPDATE", endpoint,
                        rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                        System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                    checkResult(obTable.getIp(), obTable.getPort(), request, result);
                    return result;
                }
            });
    }

    /**
     * Delete.
     */
    public Delete delete(String tableName) {
        return new Delete(this, tableName);
    }

    /**
     * Delete.
     */
    @Override
    public long delete(final String tableName, final Object[] rowKey) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        final long start = System.currentTimeMillis();
        return execute(tableName, new TableExecuteCallback<Long>(rowKey) {

            /**
             * Execute.
             */
            @Override
            public Long execute(ObTableParam tableParam) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName,
                    DEL, rowKey, null, null, obTable.getObTableOperationTimeout());
                request.setTableId(tableParam.getTableId());
                // partId/tabletId
                request.setPartitionId(tableParam.getPartitionId());
                ObPayload result = executeWithRetry(obTable, request, tableName);
                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableName, "DELETE", endpoint, rowKey,
                    (ObTableOperationResult) result, getTableTime - start,
                    System.currentTimeMillis() - getTableTime, getslowQueryMonitorThreshold());
                checkObTableOperationResult(obTable.getIp(), obTable.getPort(), request, result);
                return ((ObTableOperationResult) result).getAffectedRows();
            }
        });
    }

    /**
     * Delete with result
     * @param tableName which table to delete
     * @param rowKey delete row key
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload deleteWithResult(final String tableName, final Row rowKey) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, null) {

                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObTableParam tableParam) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTable obTable = tableParam.getObTable();
                    ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                        tableName, DEL, rowKey.getValues(), null, null,
                        obTable.getObTableOperationTimeout());
                    request.setTableId(tableParam.getTableId());
                    // partId/tabletId
                    request.setPartitionId(tableParam.getPartitionId());
                    ObPayload result = executeWithRetry(obTable, request, tableName);
                    String endpoint = obTable.getIp() + ":" + obTable.getPort();
                    MonitorUtil.info(request, database, tableName, "DELETE", endpoint,
                        rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                        System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                    checkResult(obTable.getIp(), obTable.getPort(), request, result);
                    return result;
                }
            });
    }

    /**
     * Insert.
     */
    public Insert insert(String tableName) {
        return new Insert(this, tableName);
    }

    /**
     * Insert.
     */
    @Override
    public long insert(final String tableName, final Object[] rowKey, final String[] columns,
                       final Object[] values) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        final long start = System.currentTimeMillis();
        return execute(tableName, new TableExecuteCallback<Long>(rowKey) {
            /**
             * Execute.
             */
            @Override
            public Long execute(ObTableParam tableParam) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName,
                    INSERT, rowKey, columns, values, obTable.getObTableOperationTimeout());
                request.setTableId(tableParam.getTableId());
                // partId/tabletId
                request.setPartitionId(tableParam.getPartitionId());
                ObPayload result = executeWithRetry(obTable, request, tableName);
                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableName, "INSERT", endpoint, rowKey,
                    (ObTableOperationResult) result, getTableTime - start,
                    System.currentTimeMillis() - getTableTime, getslowQueryMonitorThreshold());
                checkObTableOperationResult(obTable.getIp(), obTable.getPort(), request, result);
                return ((ObTableOperationResult) result).getAffectedRows();
            }
        });
    }

    /**
     * Insert with result
     * @param tableName which table to insert
     * @param rowKey insert row key
     * @param columns columns name to insert
     * @param values new values
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload insertWithResult(final String tableName, final Row rowKey,
                                      final String[] columns,
                                      final Object[] values) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, null) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObTableParam tableParam) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTable obTable = tableParam.getObTable();
                    ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                        tableName, INSERT, rowKey.getValues(), columns, values,
                        obTable.getObTableOperationTimeout());
                    request.setTableId(tableParam.getTableId());
                    // partId/tabletId
                    request.setPartitionId(tableParam.getPartitionId());
                    ObPayload result = executeWithRetry(obTable, request, tableName);
                    String endpoint = obTable.getIp() + ":" + obTable.getPort();
                    MonitorUtil.info(request, database, tableName, "INSERT", endpoint,
                        rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                        System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                    checkResult(obTable.getIp(), obTable.getPort(), request, result);
                    return result;
                }
            });
    }

    /**
     * Get.
     */
    public Get get(String tableName) {
        return new Get(this, tableName);
    }

    /**
     * get
     * @param tableName which table to insert
     * @param rowKey insert row key
     * @param selectColumns select columns
     * @return execute result
     * @throws Exception exception
     */
    public Map<String, Object> get(final String tableName, final Row rowKey,
                                   final String[] selectColumns) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
                new OperationExecuteCallback<Map<String, Object>>(rowKey, null) {
                    /**
                     * Execute.
                     */
                    @Override
                    public Map<String, Object> execute(ObTableParam tableParam) throws Exception {
                        long TableTime = System.currentTimeMillis();
                        ObTable obTable = tableParam.getObTable();
                        ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                                tableName, GET, rowKey.getValues(), selectColumns, null,
                                obTable.getObTableOperationTimeout());
                        request.setTableId(tableParam.getTableId());
                        // partId/tabletId
                        request.setPartitionId(tableParam.getPartitionId());
                        ObPayload result = executeWithRetry(obTable, request, tableName);
                        String endpoint = obTable.getIp() + ":" + obTable.getPort();
                        MonitorUtil.info(request, database, tableName, "GET", endpoint,
                                rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                                System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                        checkResult(obTable.getIp(), obTable.getPort(), request, result);
                        return ((ObTableOperationResult) result).getEntity().getSimpleProperties();
                    }
                });
    }

    /**
     * put with result
     * @param tableName which table to put
     * @param rowKey insert row key
     * @param columns columns name to put
     * @param values new values
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload putWithResult(final String tableName, final Row rowKey,
                                   final String[] columns,
                                   final Object[] values) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, null) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObTableParam tableParam) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTable obTable = tableParam.getObTable();
                    ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                        tableName, PUT, rowKey.getValues(), columns, values,
                        obTable.getObTableOperationTimeout());
                    request.setTableId(tableParam.getTableId());
                    // partId/tabletId
                    request.setPartitionId(tableParam.getPartitionId());
                    ObPayload result = executeWithRetry(obTable, request, tableName);
                    String endpoint = obTable.getIp() + ":" + obTable.getPort();
                    MonitorUtil.info(request, database, tableName, "PUT", endpoint,
                        rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                        System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                    checkResult(obTable.getIp(), obTable.getPort(), request, result);
                    return result;
                }
            });
    }

    /**
     * Replace.
     */
    public Replace replace(String tableName) {
        return new Replace(this, tableName);
    }

    /**
     * Replace.
     */
    @Override
    public long replace(final String tableName, final Object[] rowKey, final String[] columns,
                        final Object[] values) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        final long start = System.currentTimeMillis();
        return execute(tableName, new TableExecuteCallback<Long>(rowKey) {
            /**
             * Execute.
             */
            @Override
            public Long execute(ObTableParam tableParam) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName,
                    REPLACE, rowKey, columns, values, obTable.getObTableOperationTimeout());
                request.setTableId(tableParam.getTableId());
                // partId/tabletId
                request.setPartitionId(tableParam.getPartitionId());
                ObPayload result = executeWithRetry(obTable, request, tableName);
                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableName, "REPLACE", endpoint, rowKey,
                    (ObTableOperationResult) result, getTableTime - start,
                    System.currentTimeMillis() - getTableTime, getslowQueryMonitorThreshold());
                checkObTableOperationResult(obTable.getIp(), obTable.getPort(), request, result);
                return ((ObTableOperationResult) result).getAffectedRows();
            }
        });
    }

    /**
     * Replace with result
     * @param tableName which table to replace
     * @param rowKey replace row key
     * @param columns columns name to replace
     * @param values new values
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload replaceWithResult(final String tableName, final Row rowKey,
                                       final String[] columns,
                                       final Object[] values) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, null) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObTableParam tableParam) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTable obTable = tableParam.getObTable();
                    ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                        tableName, REPLACE, rowKey.getValues(), columns, values,
                        obTable.getObTableOperationTimeout());
                    request.setTableId(tableParam.getTableId());
                    // partId/tabletId
                    request.setPartitionId(tableParam.getPartitionId());
                    ObPayload result = executeWithRetry(obTable, request, tableName);
                    String endpoint = obTable.getIp() + ":" + obTable.getPort();
                    MonitorUtil.info(request, database, tableName, "REPLACE", endpoint,
                        rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                        System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                    checkResult(obTable.getIp(), obTable.getPort(), request, result);
                    return result;
                }
            });
    }

    /**
     * Insert or update.
     */
    public InsertOrUpdate insertOrUpdate(String tableName) {
        return new InsertOrUpdate(this, tableName);
    }

    /**
     * Insert or update.
     */
    @Override
    public long insertOrUpdate(final String tableName, final Object[] rowKey,
                               final String[] columns, final Object[] values) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        final long start = System.currentTimeMillis();
        return execute(tableName, new TableExecuteCallback<Long>(rowKey) {
            /**
             * Execute.
             */
            @Override
            public Long execute(ObTableParam tableParam) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName,
                    INSERT_OR_UPDATE, rowKey, columns, values, obTable.getObTableOperationTimeout());
                request.setTableId(tableParam.getTableId());
                // partId/tabletId
                request.setPartitionId(tableParam.getPartitionId());
                ObPayload result = executeWithRetry(obTable, request, tableName);
                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableName, "INERT_OR_UPDATE", endpoint, rowKey,
                    (ObTableOperationResult) result, getTableTime - start,
                    System.currentTimeMillis() - getTableTime, getslowQueryMonitorThreshold());
                checkObTableOperationResult(obTable.getIp(), obTable.getPort(), request, result);
                return ((ObTableOperationResult) result).getAffectedRows();
            }
        });
    }

    /**
     * InsertOrUpdate with result
     * @param tableName which table to InsertOrUpdate
     * @param rowKey InsertOrUpdate row key
     * @param columns columns name to InsertOrUpdate
     * @param values new values
     * @param usePut use put or not
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload insertOrUpdateWithResult(final String tableName, final Row rowKey,
                                              final String[] columns, final Object[] values,
                                              boolean usePut) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, null) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObTableParam tableParam) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTable obTable = tableParam.getObTable();
                    ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                        tableName, INSERT_OR_UPDATE, rowKey.getValues(), columns, values,
                        obTable.getObTableOperationTimeout());
                    request.setTableId(tableParam.getTableId());
                    // partId/tabletId
                    request.setPartitionId(tableParam.getPartitionId());
                    if (usePut) {
                        request.setOptionFlag(ObTableOptionFlag.USE_PUT);
                    }
                    ObPayload result = executeWithRetry(obTable, request, tableName);
                    String endpoint = obTable.getIp() + ":" + obTable.getPort();
                    MonitorUtil.info(request, database, tableName, "INERT_OR_UPDATE", endpoint,
                        rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                        System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                    checkResult(obTable.getIp(), obTable.getPort(), request, result);
                    return result;
                }
            });
    }

    /**
     * Put.
     */
    public Put put(String tableName) {
        return new Put(this, tableName);
    }

    /**
     * Increment.
     */
    public Increment increment(String tableName) {
        return new Increment(this, tableName);
    }

    /**
     *
     * @param tableName which table to increment
     * @param rowKey increment row key
     * @param columns columns name to increment
     * @param values new valuess
     * @param withResult whether to bring back result
     * @return execute result
     * @throws Exception exception
     */
    @Override
    public Map<String, Object> increment(final String tableName, final Object[] rowKey,
                                         final String[] columns, final Object[] values,
                                         final boolean withResult) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        final long start = System.currentTimeMillis();
        return execute(tableName, new TableExecuteCallback<Map<String, Object>>(rowKey) {
            /**
             *
             * @param tableParam
             * @return
             * @throws Exception
             */
            @Override
            public Map<String, Object> execute(ObTableParam tableParam) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName,
                    INCREMENT, rowKey, columns, values, obTable.getObTableOperationTimeout());
                request.setReturningAffectedEntity(withResult);
                request.setTableId(tableParam.getTableId());
                request.setPartitionId(tableParam.getPartitionId());
                ObPayload result = executeWithRetry(obTable, request, tableName);
                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableName, "INCREMENT", endpoint, rowKey,
                    (ObTableOperationResult) result, getTableTime - start,
                    System.currentTimeMillis() - getTableTime, getslowQueryMonitorThreshold());
                checkObTableOperationResult(obTable.getIp(), obTable.getPort(), request, result);
                return ((ObTableOperationResult) result).getEntity().getSimpleProperties();
            }
        });
    }

    /**
     * Increment with result
     * @param tableName which table to increment
     * @param rowKey increment row key
     * @param columns columns name to increment
     * @param values new values
     * @param withResult whether to bring back result
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload incrementWithResult(final String tableName, final Row rowKey,
                                         final String[] columns,
                                         final Object[] values, final boolean withResult)
                                                                                         throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, null) {
                /**
                 *
                 * @param tableParam table parameters
                 * @return
                 * @throws Exception
                 */
                @Override
                public ObPayload execute(ObTableParam tableParam) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTable obTable = tableParam.getObTable();
                    ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                        tableName, INCREMENT, rowKey.getValues(), columns, values,
                        obTable.getObTableOperationTimeout());
                    request.setReturningAffectedEntity(withResult);
                    request.setTableId(tableParam.getTableId());
                    // partId/tabletId
                    request.setPartitionId(tableParam.getPartitionId());
                    ObPayload result = executeWithRetry(obTable, request, tableName);
                    String endpoint = obTable.getIp() + ":" + obTable.getPort();
                    MonitorUtil.info(request, database, tableName, "INCREMENT", endpoint,
                        rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                        System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                    checkResult(obTable.getIp(), obTable.getPort(), request, result);
                    return result;
                }
            });
    }

    /**
     * Append.
     */
    public Append append(String tableName) {
        return new Append(this, tableName);
    }

    @Override
    public Map<String, Object> append(final String tableName, final Object[] rowKey,
                                      final String[] columns, final Object[] values,
                                      final boolean withResult) throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        final long start = System.currentTimeMillis();
        return execute(tableName, new TableExecuteCallback<Map<String, Object>>(rowKey) {
            @Override
            public Map<String, Object> execute(ObTableParam tableParam) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName,
                    APPEND, rowKey, columns, values, obTable.getObTableOperationTimeout());
                request.setReturningAffectedEntity(withResult);
                request.setTableId(tableParam.getTableId());
                // partId/tabletId
                request.setPartitionId(tableParam.getPartitionId());
                ObPayload result = executeWithRetry(obTable, request, tableName);
                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableName, "INCREMENT", endpoint, rowKey,
                    (ObTableOperationResult) result, getTableTime - start,
                    System.currentTimeMillis() - getTableTime, getslowQueryMonitorThreshold());
                checkObTableOperationResult(obTable.getIp(), obTable.getPort(), request, result);
                return ((ObTableOperationResult) result).getEntity().getSimpleProperties();
            }
        });
    }

    /**
     * Append with result
     * @param tableName which table to append
     * @param rowKey append row key
     * @param columns columns name to append
     * @param values new values
     * @param withResult whether to bring back row result
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload appendWithResult(final String tableName, final Row rowKey,
                                      final String[] columns,
                                      final Object[] values, final boolean withResult)
                                                                                      throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, null) {
                @Override
                public ObPayload execute(ObTableParam tableParam) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTable obTable = tableParam.getObTable();
                    ObTableOperationRequest request = ObTableOperationRequest.getInstance(
                        tableName, APPEND, rowKey.getValues(), columns, values,
                        obTable.getObTableOperationTimeout());
                    request.setReturningAffectedEntity(withResult);
                    request.setTableId(tableParam.getTableId());
                    // partId/tabletId
                    request.setPartitionId(tableParam.getPartitionId());
                    ObPayload result = executeWithRetry(obTable, request, tableName);
                    String endpoint = obTable.getIp() + ":" + obTable.getPort();
                    MonitorUtil.info(request, database, tableName, "INCREMENT", endpoint,
                        rowKey.getValues(), (ObTableOperationResult) result, TableTime - start,
                        System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                    checkResult(obTable.getIp(), obTable.getPort(), request, result);
                    return result;
                }
            });
    }

    /**
     * batch mutation.
     */
    public BatchOperation batchOperation(String tableName) {
        return new BatchOperation(this, tableName);
    }

    /**
     * get partition information and host information by row key in Row
     * @param tableName table name
     * @param rowKey row key which want to query
     * @return Partition information
     * @throws Exception Exception
     */
    public Partition getPartition(String tableName, Row rowKey, boolean refresh) throws Exception {
        return getSinglePartitionInternal(tableName, rowKey, refresh);
    }

    /**
     * do the real task to obtain partition information and host information
     * @param tableName table name
     * @param rowKey row key which want to query
     * @return Partition information
     * @throws Exception Exception
     */
    private Partition getSinglePartitionInternal(String tableName, Row rowKey, boolean refresh) throws Exception {
        if (tableRowKeyElement.get(tableName) == null) {
            addRowKeyElement(tableName, rowKey.getColumns());
        }
        ObTableParam tableParam = null;
        if (refresh) {
            if (odpMode) {
                tableRoute.refreshODPMeta(tableName, true);
            } else {
                tableRoute.refreshMeta(tableName);
            }
        }
        tableParam = tableRoute.getTableParam(tableName, rowKey);
        return new Partition(tableParam.getPartitionId(), tableParam.getPartId(),
            tableParam.getTableId(), tableParam.getObTable().getIp(), tableParam.getObTable()
                .getPort(), tableParam.getLsId());
    }

    /**
     * get all partition information from table
     * @param tableName table name to query
     * @return partitions
     * @throws Exception Exception
     */
    public List<Partition> getPartition(String tableName, boolean refresh) throws Exception {
        return getAllPartitionInternal(tableName, refresh);
    }

    /**
     * do the real task to obtain all partitions information and host information
     * @param tableName table name
     * @return List<Partition> partitions
     * @throws Exception Exception
     */
    private List<Partition> getAllPartitionInternal(String tableName, boolean refresh) throws Exception {
        List<Partition> partitions = new ArrayList<>();
        if (refresh) {
            if (odpMode) {
                tableRoute.refreshODPMeta(tableName, true);
            } else {
                tableRoute.refreshMeta(tableName);
            }
        }
        List<ObTableParam> allTables = tableRoute.getTableParams(tableName, new ObTableQuery(), new Object[]{ ObObj.getMin() }, true,
                new Object[]{ ObObj.getMax() }, true);
        for (ObTableParam tableParam : allTables) {
            Partition partition = new Partition(tableParam.getPartitionId(), tableParam.getPartId(), tableParam.getTableId(),
                    tableParam.getObTable().getIp(), tableParam.getObTable().getPort(), tableParam.getLsId());
            partitions.add(partition);
        }
        return partitions;
    }

    /**
     * execute mutation with filter
     * @param tableQuery table query
     * @param rowKey row key which want to mutate
     * @param operation table operation
     * @param withResult whether to bring back result
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload mutationWithFilter(final TableQuery tableQuery, final Row rowKey,
                                        final ObTableOperation operation, final boolean withResult)
                                                                                                   throws Exception {
        return mutationWithFilter(tableQuery, rowKey, operation, withResult, false,
            false, false);
    }

    /**
     * execute mutation with filter
     * @param tableQuery table query
     * @param rowKey row key which want to mutate
     * @param operation table operation
     * @param withResult whether to bring back result
     * @param checkAndExecute whether execute check and execute instead of query and mutate
     * @param checkExists whether to check exists or not
     * @param rollbackWhenCheckFailed whether rollback or not when check failed
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload mutationWithFilter(final TableQuery tableQuery, final Row rowKey,
                                        final ObTableOperation operation, final boolean withResult,
                                        final boolean checkAndExecute, final boolean checkExists,
                                        final boolean rollbackWhenCheckFailed)
                                                                                                 throws Exception {
        final long start = System.currentTimeMillis();
        if (tableQuery != null && tableQuery.getObTableQuery().getKeyRanges().isEmpty()) {
            // fill a whole range if no range is added explicitly.
            tableQuery.getObTableQuery().addKeyRange(ObNewRange.getWholeRange());
        }
        return execute(tableQuery.getTableName(), new OperationExecuteCallback<ObPayload>(
            rowKey, tableQuery) {
            /**
             * Execute.
             */
            @Override
            public ObPayload execute(ObTableParam tableParam) throws Exception {
                long TableTime = System.currentTimeMillis();
                ObTable obTable = tableParam.getObTable();
                ObTableQueryAndMutateRequest request = obTableQueryAndMutate(operation, tableQuery,
                    false);
                request.setTimeout(obTable.getObTableOperationTimeout());
                request.setReturningAffectedEntity(withResult);
                request.setTableId(tableParam.getTableId());
                // partId/tabletId
                request.setPartitionId(tableParam.getPartitionId());
                request.getTableQueryAndMutate().setIsCheckAndExecute(checkAndExecute);
                request.getTableQueryAndMutate().setIsCheckNoExists(!checkExists);
                request.getTableQueryAndMutate().setIsRollbackWhenCheckFailed(rollbackWhenCheckFailed);
                ObPayload result = executeWithRetry(obTable, request, tableQuery.getTableName());
                String endpoint = obTable.getIp() + ":" + obTable.getPort();
                MonitorUtil.info(request, database, tableQuery.getTableName(), "QUERY_AND_MUTATE",
                    operation.getOperationType().toString(), endpoint,
                    (ObTableQueryAndMutateResult) result, tableQuery.getObTableQuery(), TableTime
                                                                                        - start,
                    System.currentTimeMillis() - TableTime, getslowQueryMonitorThreshold());
                checkResult(obTable.getIp(), obTable.getPort(), request, result);
                return result;
            }
        });
    }

    public ObPayload executeWithRetry(ObTable obTable, ObPayload request, String tableName) throws Exception {
        ObPayload result = obTable.execute(request);
        if (result != null && result.getPcode() == Pcodes.OB_TABLE_API_MOVE) {
            ObTableApiMove moveResponse = (ObTableApiMove) result;
            if (request instanceof ObTableAbstractOperationRequest) {
                long tabletId = ((ObTableAbstractOperationRequest) request).getPartitionId();
                tableRoute.refreshPartitionLocation(tableName, tabletId, null);
            }
            obTable = getTable(moveResponse);
            result = obTable.execute(request);
            if (result instanceof ObTableApiMove) {
                ObTableApiMove move = (ObTableApiMove) result;
                logger.warn("The server has not yet completed the master switch, and returned an incorrect leader with an IP address of {}. " +
                                "Rerouting return IP is {}", moveResponse.getReplica().getServer().ipToString(), move .getReplica().getServer().ipToString());
                throw new ObTableRoutingWrongException();
            }
        }
        return result;
    }

    /**
     *
     * @param tableQuery table query
     * @param columns columns name
     * @param values new value
     * @return mutate request
     * @throws Exception exceotion
     */
    public ObTableQueryAndMutateRequest obTableQueryAndUpdate(final TableQuery tableQuery,
                                                              final String[] columns,
                                                              final Object[] values)
                                                                                    throws Exception {
        if (null == columns || null == values || 0 == columns.length || 0 == values.length) {
            throw new ObTableException("client get unexpected empty columns or values");
        }
        ObTableOperation operation = ObTableOperation.getInstance(UPDATE, new Object[] {}, columns,
            values);
        return obTableQueryAndMutate(operation, tableQuery, false);
    }

    /**
     *
     * @param tableQuery table query
     * @return delete request
     * @throws Exception exception
     */

    public ObTableQueryAndMutateRequest obTableQueryAndDelete(final TableQuery tableQuery)
                                                                                          throws Exception {
        ObTableOperation operation = ObTableOperation.getInstance(DEL, new Object[] {}, null, null);
        return obTableQueryAndMutate(operation, tableQuery, false);
    }

    /**
     *
     * @param tableQuery table query
     * @param columns columns name
     * @param values new values
     * @param withResult whether to bring back result
     * @return increment result
     * @throws Exception exception
     */
    public ObTableQueryAndMutateRequest obTableQueryAndIncrement(final TableQuery tableQuery,
                                                                 final String[] columns,
                                                                 final Object[] values,
                                                                 final boolean withResult)
                                                                                          throws Exception {
        if (null == columns || null == values || 0 == columns.length || 0 == values.length) {
            throw new ObTableException("client get unexpected empty columns or values");
        }
        ObTableOperation operation = ObTableOperation.getInstance(INCREMENT, new Object[] {},
            columns, values);
        return obTableQueryAndMutate(operation, tableQuery, withResult);
    }

    /**
     *
     * @param tableQuery table query
     * @param columns columns name
     * @param values new values
     * @param withResult whether to bring back result
     * @return append result
     * @throws Exception exception
     */
    public ObTableQueryAndMutateRequest obTableQueryAndAppend(final TableQuery tableQuery,
                                                              final String[] columns,
                                                              final Object[] values,
                                                              final boolean withResult)
                                                                                       throws Exception {
        if (null == columns || null == values || 0 == columns.length || 0 == values.length) {
            throw new ObTableException("client get unexpected empty columns or values");
        }
        ObTableOperation operation = ObTableOperation.getInstance(APPEND, new Object[] {}, columns,
            values);
        return obTableQueryAndMutate(operation, tableQuery, withResult);
    }

    /**
     *
     * @param operation table operation
     * @param tableQuery table query
     * @param withResult whether to bring back result
     * @return
     * @throws Exception
     */
    ObTableQueryAndMutateRequest obTableQueryAndMutate(final ObTableOperation operation,
                                                       final TableQuery tableQuery,
                                                       final boolean withResult) throws Exception {
        ObTableQuery obTableQuery = tableQuery.getObTableQuery();
        String tableName = tableQuery.getTableName();
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }

        ObTableBatchOperation operations = new ObTableBatchOperation();

        operations.addTableOperation(operation);

        ObTableQueryAndMutate queryAndMutate = buildObTableQueryAndMutate(obTableQuery, operations);
        if (runningMode == RunningMode.HBASE) {
            if (operation.getEntity() != null || operation.getEntity().getRowKeySize() != 3) {
                throw new IllegalArgumentException("rowkey size is not 3");
            }
            long ts = (long)operation.getEntity().getRowKeyValue(2).getValue();
            if (ts != -Long.MAX_VALUE) {
                queryAndMutate.setIsUserSpecifiedT(true);
            }
        }
        ObTableQueryAndMutateRequest request = buildObTableQueryAndMutateRequest(queryAndMutate,
            tableName);

        request.setOptionFlag(ObTableOptionFlag.DEFAULT);
        request.setReturningAffectedEntity(withResult);
        request.setReturningAffectedRows(true);

        return request;
    }

    /**
     * Execute.
     */
    /**
     * Excute
     * @param request request
     * @return response
     * @throws Exception if fail
     */
    public ObPayload execute(final ObTableAbstractOperationRequest request) throws Exception {
        if (request.getTableName() == null || request.getTableName().isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        if (request instanceof ObTableOperationRequest) {
            ObTableBatchOperation batchOperation = new ObTableBatchOperation();
            batchOperation.addTableOperation(((ObTableOperationRequest) request)
                    .getTableOperation());
            ObTableClientBatchOpsImpl batchOps = new ObTableClientBatchOpsImpl(
                    request.getTableName(), batchOperation, this);
            batchOps.setEntityType(request.getEntityType());
            ObTableBatchOperationResult batchOpsResult = new ObClusterTableBatchOps(batchOps)
                    .executeInternal();
            return batchOpsResult.getResults().get(0);
        } else if (request instanceof ObTableQueryRequest) {
            // TableGroup -> TableName
            String tableName = request.getTableName();
            ObTableClientQueryImpl tableQuery = new ObTableClientQueryImpl(tableName,
                    ((ObTableQueryRequest) request).getTableQuery(), this);
            tableQuery.setEntityType(request.getEntityType());
            return new ObClusterTableQuery(tableQuery).executeInternal();
        } else if (request instanceof ObTableQueryAsyncRequest) {
            // TableGroup -> TableName
            String tableName = request.getTableName();
            ObTableClientQueryImpl tableQuery = new ObTableClientQueryImpl(tableName,
                    ((ObTableQueryAsyncRequest) request).getObTableQueryRequest().getTableQuery(), this);
            tableQuery.setEntityType(request.getEntityType());
            ObClusterTableQuery clusterTableQuery = new ObClusterTableQuery(tableQuery);
            clusterTableQuery.setAllowDistributeScan(((ObTableQueryAsyncRequest) request).isAllowDistributeScan());
            return clusterTableQuery.asyncExecuteInternal();
        } else if (request instanceof ObTableBatchOperationRequest) {
            ObTableClientBatchOpsImpl batchOps = new ObTableClientBatchOpsImpl(
                    request.getTableName(),
                    ((ObTableBatchOperationRequest) request).getBatchOperation(), this);
            batchOps.setEntityType(request.getEntityType());
            return new ObClusterTableBatchOps(runtimeBatchExecutor, batchOps).executeInternal();
        } else if (request instanceof ObTableQueryAndMutateRequest) {
            ObTableQueryAndMutate tableQueryAndMutate = ((ObTableQueryAndMutateRequest) request)
                    .getTableQueryAndMutate();
            ObTableQuery tableQuery = tableQueryAndMutate.getTableQuery();
            // fill a whole range if no range is added explicitly.
            if (tableQuery.getKeyRanges().isEmpty()) {
                tableQuery.addKeyRange(ObNewRange.getWholeRange());
            }
            if (isOdpMode()) {
                request.setTimeout(getOdpTable().getObTableOperationTimeout());
                return getOdpTable().execute(request);
            } else {
                int tryTimes = 0;
                long startExecute = System.currentTimeMillis();
                Map<Long, ObTableParam> partIdMapObTable = new HashMap<Long, ObTableParam>();
                while (true) {
                    long currentExecute = System.currentTimeMillis();
                    long costMillis = currentExecute - startExecute;
                    if (costMillis > getRuntimeMaxWait()) {
                        logger.error(
                                "tablename:{} it has tried " + tryTimes
                                        + " times and it has waited " + costMillis
                                        + "/ms which exceeds response timeout "
                                        + getRuntimeMaxWait() + "/ms", request.getTableName());
                        throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                                + " times and it has waited " + costMillis
                                + "/ms which exceeds response timeout "
                                + getRuntimeMaxWait() + "/ms");
                    }
                    try {
                        // Recalculate partIdMapObTable
                        // Clear the map before recalculating
                        partIdMapObTable.clear();
                        for (ObNewRange rang : tableQuery.getKeyRanges()) {
                            ObRowKey startKey = rang.getStartKey();
                            int startKeySize = startKey.getObjs().size();
                            ObRowKey endKey = rang.getEndKey();
                            int endKeySize = endKey.getObjs().size();
                            Object[] start = new Object[startKeySize];
                            Object[] end = new Object[endKeySize];
                            for (int i = 0; i < startKeySize; i++) {
                                ObObj curStart = startKey.getObj(i);
                                if (curStart.isMinObj()) {
                                    start[i] = curStart;
                                } else {
                                    start[i] = curStart.getValue();
                                }
                            }

                            for (int i = 0; i < endKeySize; i++) {
                                ObObj curEnd = endKey.getObj(i);
                                if (curEnd.isMaxObj()) {
                                    end[i] = curEnd;
                                } else {
                                    end[i] = curEnd.getValue();
                                }
                            }
                            ObBorderFlag borderFlag = rang.getBorderFlag();
                            List<ObTableParam> params = getTableParams(request.getTableName(),
                                    tableQuery, start, borderFlag.isInclusiveStart(), end,
                                    borderFlag.isInclusiveEnd());
                            for (ObTableParam param : params) {
                                partIdMapObTable.put(param.getPartId(), param);
                            }
                        }

                        // Check if partIdMapObTable size is greater than 1
                        boolean isDistributedExecuteSupported = getServerCapacity().isSupportDistributedExecute();
                        if (partIdMapObTable.size() > 1 && !isDistributedExecuteSupported) {
                            throw new ObTablePartitionConsistentException(
                                    "query and mutate must be a atomic operation");
                        }
                        // Proceed with the operation
                        Map.Entry<Long, ObTableParam> entry = partIdMapObTable.entrySet().iterator().next();
                        ObTableParam tableParam = entry.getValue();
                        request.setTableId(tableParam.getTableId());
                        long partitionId = isDistributedExecuteSupported ? INVALID_TABLET_ID : tableParam.getPartitionId();
                        request.setPartitionId(partitionId);
                        request.setTimeout(tableParam.getObTable().getObTableOperationTimeout());
                        ObTable obTable = tableParam.getObTable();

                        // Attempt to execute the operation
                        return executeWithRetry(obTable, request, request.getTableName());
                    } catch (Exception ex) {
                        tryTimes++;
                        if (ex instanceof ObTableException &&
                                (((ObTableException) ex).isNeedRefreshTableEntry() || ((ObTableException) ex).isNeedRetryServerError())) {
                            logger.warn(
                                    "tablename:{} partition id:{} batch ops refresh table while meet ObTableMasterChangeException, errorCode: {}",
                                    request.getTableName(), request.getPartitionId(), ((ObTableException) ex).getErrorCode(), ex);

                            if (isRetryOnChangeMasterTimes()) {
                                logger.warn(
                                        "tablename:{} partition id:{} batch ops retry while meet ObTableMasterChangeException, errorCode: {} , retry times {}",
                                        request.getTableName(), request.getPartitionId(), ((ObTableException) ex).getErrorCode(),
                                        tryTimes, ex);

                                if (ex instanceof ObTableNeedFetchMetaException) {
                                    // Refresh table info
                                    refreshMeta(request.getTableName());
                                }
                            } else {
                                calculateContinuousFailure(request.getTableName(), ex.getMessage());
                                throw ex;
                            }
                        } else {
                            calculateContinuousFailure(request.getTableName(), ex.getMessage());
                            // Handle other exceptions or rethrow
                            throw ex;
                        }
                    }
                }
            }
        }

        throw new FeatureNotSupportedException("request type " + request.getClass().getSimpleName()
                + "is not supported. make sure the correct version");
    }

    private ObTableQueryAndMutate buildObTableQueryAndMutate(ObTableQuery obTableQuery,
                                                             ObTableBatchOperation obTableBatchOperation) {
        ObTableQueryAndMutate queryAndMutate = new ObTableQueryAndMutate();
        queryAndMutate.setTableQuery(obTableQuery);
        queryAndMutate.setMutations(obTableBatchOperation);
        return queryAndMutate;
    }

    private ObTableQueryAndMutateRequest buildObTableQueryAndMutateRequest(ObTableQueryAndMutate queryAndMutate,
                                                                           String targetTableName) {
        ObTableQueryAndMutateRequest request = new ObTableQueryAndMutateRequest();
        request.setTableName(targetTableName);
        request.setTableQueryAndMutate(queryAndMutate);
        request.setEntityType(ObTableEntityType.KV);
        return request;
    }

    /**
     * checkAndInsUp.
     */
    public CheckAndInsUp checkAndInsUp(String tableName, ObTableFilter filter,
                                       InsertOrUpdate insUp, boolean checkExists) {
        return new CheckAndInsUp(this, tableName, filter, insUp, checkExists);
    }

    /**
     * checkAndInsUp.
     */
    public CheckAndInsUp checkAndInsUp(String tableName, ObTableFilter filter, InsertOrUpdate insUp,
                                       boolean checkExists, boolean rollbackWhenCheckFailed) {
        return new CheckAndInsUp(this, tableName, filter, insUp, checkExists, rollbackWhenCheckFailed);
    }

    /**
     * Set full username
     * @param fullUserName user name
     * @throws IllegalArgumentException if userName invalid
     */
    public void setFullUserName(String fullUserName) throws IllegalArgumentException {
        if (StringUtils.isBlank(fullUserName)) {
            RUNTIME.error(String.format("full username is empty, full username=%s", fullUserName));
            throw new IllegalArgumentException(String.format(
                "full username is empty, full username=%s", fullUserName));
        }
        if (this.odpMode == true) {
            // do nothing, just pass raw username to odp
        } else if (-1 != fullUserName.indexOf('@') || -1 != fullUserName.indexOf('#')) {
            parseStandardFullUsername(fullUserName);
        } else {
            parseNonStandardFullUsername(fullUserName);
        }
        this.fullUserName = fullUserName;
    }

    /**
     * Set sys user name to access meta table.
     * @param sysUserName system user name
     */
    public void setSysUserName(String sysUserName) {
        sysUA.setUserName(sysUserName);
    }

    /**
     * Set sys user password to access meta table.
     * @param sysPassword system password
     */
    public void setSysPassword(String sysPassword) {
        sysUA.setPassword(sysPassword);
    }

    /**
     * Set sys user encrypted password to access meta table.
     * @param encSysPassword encrypted system password
     * @throws Exception if fail
     */
    public void setEncSysPassword(String encSysPassword) throws Exception {
        sysUA.setEncPassword(encSysPassword);
    }

    private void parseStandardFullUsername(String username) {
        int utIndex = -1;
        int tcIndex = -1;
        utIndex = username.indexOf('@');
        tcIndex = username.indexOf('#');
        if (-1 == utIndex || -1 == tcIndex || utIndex >= tcIndex) {
            RUNTIME.error(String.format("invalid full username, username=%s", username));
            throw new IllegalArgumentException(
                String
                    .format(
                        "invalid full username, username=%s (which should be userName@tenantName#clusterName)",
                        username));
        }

        String user = username.substring(0, utIndex);
        String tenant = username.substring(utIndex + 1, tcIndex);
        String cluster = username.substring(tcIndex + 1);
        handleFullUsername(user, tenant, cluster, username);
    }

    private void parseNonStandardFullUsername(String username) {
        if (StringUtils.isBlank(usernameSeparators)) {
            RUNTIME.error(String.format(
                "non standard username separators has not been set, full username=%s", username));
            throw new IllegalArgumentException(String.format(
                "non standard username separators has not been set, full username=%s", username));
        }
        String[] separators = usernameSeparators.split(";");
        char separatorChar = '\0';
        int ctIndex = -1;
        int tuIndex = -1;
        for (String separator : separators) {
            separatorChar = separator.charAt(0);
            ctIndex = username.indexOf(separatorChar);
            tuIndex = username.lastIndexOf(separatorChar);
            if (ctIndex != tuIndex) {
                break;
            }
        }

        if (-1 == ctIndex || -1 == tuIndex || (ctIndex == tuIndex)) {
            RUNTIME.error(String.format("invalid full username, username=%s, userSeparators=%s",
                username, usernameSeparators));
            throw new IllegalArgumentException(String.format(
                "invalid full username, username=%s, userSeparators=%s", username,
                usernameSeparators));
        }

        String cluster = username.substring(0, ctIndex);
        String tenant = username.substring(ctIndex + 1, tuIndex);
        String user = username.substring(tuIndex + 1);
        handleFullUsername(user, tenant, cluster, username);
    }

    /**
     *
     * @param user
     * @param tenant
     * @param cluster
     * @param username
     */
    private void handleFullUsername(String user, String tenant, String cluster, String username) {
        if (StringUtils.isBlank(user)) {
            RUNTIME.error(String.format("user has not been set, username=%s", username));
            throw new IllegalArgumentException(String.format("user has not been set, username=%s",
                username));
        }
        if (StringUtils.isBlank(tenant)) {
            RUNTIME.error(String.format("tenant has not been set, username=%s", username));
            throw new IllegalArgumentException(String.format(
                "tenant has not been set, username=%s", username));
        }
        if (StringUtils.isBlank(cluster)) {
            RUNTIME.error(String.format("cluster has not been set, username=%s", username));
            throw new IllegalArgumentException(String.format(
                "cluster has not been set, username=%s", username));
        }
        setUserName(user);
        setTenantName(tenant);
        setClusterName(cluster);
    }

    /**
     * Get param url
     * @return param url
     */
    public String getParamURL() {
        return paramURL;
    }

    /**
     * Set param url.
     * @param paramURL param url
     * @throws IllegalArgumentException if paramURL invalid
     */
    public void setParamURL(String paramURL) throws IllegalArgumentException {
        if (StringUtils.isBlank(paramURL)) {
            RUNTIME.error(String.format("zdal url is empty, url=%s", paramURL));
            throw new IllegalArgumentException(String.format("zdal url is empty, url=%s", paramURL));
        }
        int paramIndex = paramURL.indexOf('?');
        if (-1 == paramIndex || (paramIndex + 1) == paramURL.length()) {
            RUNTIME.error(String.format("invalid zdal url, parameters are not set. url=%s",
                paramURL));
            throw new IllegalArgumentException(String.format(
                "invalid zdal url, parameters are not set. url=%s", paramURL));
        }
        String[] params = paramURL.substring(paramIndex + 1).split("&");
        String db = null;
        // in order to be compatible with old version, database should be the last parameter,
        // however, we will not strictly need this limitation since the version
        for (String param : params) {
            String kv[] = param.split("=");
            if (2 != kv.length) {
                RUNTIME.error(String.format("invalid parameter format. url=%s", paramURL));
                throw new IllegalArgumentException(String.format(
                    "invalid parameter format. url=%s", paramURL));
            }
            if (Constants.DATABASE.equalsIgnoreCase(kv[0])) {
                db = kv[1];
                if (BOOT.isInfoEnabled()) {
                    BOOT.info(String.format("will set database=%s", kv[1]));
                }
            } else if (Constants.READ_CONSISTENCY.equalsIgnoreCase(kv[0])) {
                readConsistency = ObReadConsistency.getByName(kv[1]);
                if (BOOT.isInfoEnabled()) {
                    BOOT.info(String.format("will set %s=%s", Constants.READ_CONSISTENCY, kv[1]));
                }
            } else if (Constants.OB_ROUTE_POLICY.equalsIgnoreCase(kv[0])) {
                obRoutePolicy = ObRoutePolicy.getByName(kv[1]);
                if (BOOT.isInfoEnabled()) {
                    BOOT.info(String.format("will set %s=%s", Constants.OB_ROUTE_POLICY, kv[1]));
                }
            }
        }

        if (StringUtils.isBlank(db)) {
            throw new IllegalArgumentException(String.format(
                "database is empty in paramURL(configURL). url=%s", paramURL));
        }
        setDatabase(db);
        this.paramURL = paramURL;
    }

    /**
     * Get full username
     * @return user name
     */
    public String getFullUserName() {
        return fullUserName;
    }

    /**
     * Get username
     * @return username
     */
    public String getUserName() {
        return userName;
    }

    /**
     * Set username
     * @param userName username
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /**
     * Get tenant name
     * @return tenant name
     */
    public String getTenantName() {
        return tenantName;
    }

    /**
     * Set tenant name.
     * @param tenantName tenant name
     */
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    /**
     * Get cluster name
     * @return ob cluster name
     */
    public String getClusterName() {
        return clusterName;
    }

    /**
     * Set cluster name
     * @param clusterName ob cluster name
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /**
     * Get password
     * @return password
     */
    public String getPassword() {
        return password;
    }

    /**
     * Set password
     * @param password password
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Get database
     * @return database
     */
    public String getDatabase() {
        return database;
    }

    /**
     * Set database
     * @param database database
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /**
     * Get data source name
     * @return data source name
     */
    public String getDataSourceName() {
        return dataSourceName;
    }

    /**
     * Set data source name
     * @param dataSourceName data source name
     */
    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    /**
     * Is retry on change master times.
     * @return is retry
     */
    public boolean isRetryOnChangeMasterTimes() {
        return retryOnChangeMasterTimes;
    }

    /**
     * Set retry on change master times.
     * @param retryOnChangeMasterTimes set retry
     */
    public void setRetryOnChangeMasterTimes(boolean retryOnChangeMasterTimes) {
        this.retryOnChangeMasterTimes = retryOnChangeMasterTimes;
    }

    /**
     * Add row key element
     * @param tableName table name
     * @param columns rowkey columns
     */
    public void addRowKeyElement(String tableName, String[] columns) {
        if (columns == null || columns.length == 0) {
            RUNTIME.error("add row key element error table " + tableName + " column "
                          + Arrays.toString(columns));
            throw new IllegalArgumentException("add row key element error table " + tableName
                                               + " column " + Arrays.toString(columns));
        }
        if (tableName == null || tableName.length() == 0) {
            throw new IllegalArgumentException("table name is null");
        }
        Map<String, Integer> rowKeyElement = new LinkedHashMap<String, Integer>();
        for (int i = 0; i < columns.length; i++) {
            rowKeyElement.put(columns[i], i);
        }
        tableRowKeyElement.put(tableName, rowKeyElement);
    }

    public void addRowKeyElement(String tableName, Map<String, Integer> rowKeyElement) {
        if (rowKeyElement == null || rowKeyElement.isEmpty()) {
            RUNTIME.error("add row key element error table " + tableName + " elements "
                    + rowKeyElement);
            throw new IllegalArgumentException("add row key element error table " + tableName
                    + " elements " + rowKeyElement);
        }
        if (tableName == null || tableName.length() == 0) {
            throw new IllegalArgumentException("table name is null");
        }
        tableRowKeyElement.put(tableName, rowKeyElement);
    }

    public Map<String, Integer> getRowKeyElement(String tableName) {
        return tableRowKeyElement.get(tableName);
    }

    /**
     * Set running mode.
     * @param runningMode mode, NORMAL: table client, HBASE: hbase client.
     */
    public void setRunningMode(RunningMode runningMode) {
        this.runningMode = runningMode;
    }

    public RunningMode getRunningMode() {
        return this.runningMode;
    }

    public enum RunningMode {
        NORMAL, HBASE;
    }

    public ObTableClientType getClientType(RunningMode runningMode) {
        if (ObGlobal.isDistributedExecSupport()) {
            return runningMode == RunningMode.HBASE ? ObTableClientType.JAVA_HBASE_CLIENT : ObTableClientType.JAVA_TABLE_CLIENT;
        } else {
            return ObTableClientType.JAVA_TABLE_CLIENT;
        }
    }

    /**
     * Get read consistency.
     * @return read consistency level.
     */
    public ObReadConsistency getReadConsistency() {
        ObReadConsistency readConsistency = ThreadLocalMap.getReadConsistency();
        if (readConsistency == null) {
            readConsistency = this.readConsistency;
        }
        return readConsistency;
    }

    /**
     * Get OB router policy.
     * @return policy
     */
    public ObRoutePolicy getObRoutePolicy() {
        return obRoutePolicy;
    }

    /**
     * Get OB router.
     * @return router
     */
    public ObServerRoute getReadRoute() {
        if (odpMode) {
            return null;
        }
        if (getReadConsistency().isStrong()) {
            return STRONG_READ;
        }
        ServerRoster serverRoster = tableRoute.getServerRoster();
        return new ObServerRoute(ObReadConsistency.WEAK, obRoutePolicy, serverRoster
            .getServerLdcLocation().isLdcUsed());
    }

    /**
     * Get route for read or write.
     * @param readonly is readonly
     * @return route
     */
    public ObServerRoute getRoute(boolean readonly) {
        if (readonly) {
            return getReadRoute();
        } else {
            return STRONG_READ;
        }
    }

    public ObTableServerCapacity getServerCapacity() {
        return tableRoute.getServerCapacity();
    }

    public TableRoute getTableRoute() {
        return tableRoute;
    }

    public void setOdpAddr(String odpAddr) {
        this.odpAddr = odpAddr;
    }

    public void setOdpPort(int odpPort) {
        this.odpPort = odpPort;
    }

    /**
     * Set current IDC, for testing only.
     * @param idc idc
     */
    public void setCurrentIDC(String idc) {
        this.currentIDC = idc;
    }

    public String getCurrentIDC() {
        return this.currentIDC;
    }

    @VisibleForTesting
    public int getConnectionNum() {
        ObTable randomTable = tableRoute.getFirstObTable();
        return randomTable.getConnectionNum();
    }

    @Override
    public String toString() {
        ConcurrentHashMap<ObServerAddr, ObTable> tableRoster = tableRoute.getTableRoster().getTables();
        ServerRoster serverRoster = tableRoute.getServerRoster();
        Map<String, TableEntry> tableLocations = tableRoute.getTableLocations();
        ConfigServerInfo configServerInfo = tableRoute.getConfigServerInfo();
        return "ObTableClient {\n serverRoster = " + serverRoster.getMembers()
               + ", \n serverIdc = " + serverRoster.getServerLdcLocation()
               + ", \n tableLocations = " + tableLocations + ", \n tableRoster = " + tableRoster
               + ", \n ocpModel = " + configServerInfo + "\n}\n";
    }

    public ConcurrentHashMap<String, String> getTableGroupInverted() {
        return tableRoute.getTableGroupInverted();
    }

    public ConcurrentHashMap<String, String> getTableGroupCache() {
        return tableRoute.getTableGroupCache();
    }

    /**
     * get table route fail than clear table group message
     * @param tableGroupName table group name that need to delete
     */
    public void eraseTableGroupFromCache(String tableGroupName) {
        tableRoute.eraseTableGroupFromCache(tableGroupName);
    }

    /*
    * check table name whether group name
    */
    public boolean isTableGroupName(String tabName) {
        return !tabName.contains("$");
    }

    /*
    * get phy table name form table group
    * if odp mode then do nothing
    */
    public String getPhyTableNameFromTableGroup(ObTableQueryRequest request, String tableName)
                                                                                              throws Exception {
        if (odpMode) {
            // do nothing
        } else if (request.getTableQuery().isHbaseQuery() && isTableGroupName(tableName)) {
            tableName = tryGetTableNameFromTableGroupCache(tableName, false);
        }
        return tableName;
    }

    public String getPhyTableNameFromTableGroup(ObTableEntityType type, String tableName) throws Exception {
        if (odpMode) {
            // do nothing
        } else if (type == ObTableEntityType.HKV && isTableGroupName(tableName)) {
            tableName = tryGetTableNameFromTableGroupCache(tableName, false);
        }
        return tableName;
    }

    /*
     * Get the start keys of different tablets, byte[0] = [] = EMPTY_START_ROW = EMPTY_END_ROW
     * Example:
     *   For Non   Partition: return [[[]]]
     *   For Key   Partition: return [[[]]]
     *   For Hash  Partition: return [[[]]]
     *   For Range Partition: return [[[], [], []], ['a', [], []], ['z', 'b', 'c']]
     */
    public byte[][][] getFirstPartStartKeys(String tableName) throws Exception {
        // Check row key element
        // getOrRefreshTableEntry() need row key element, we could remove this after we remove rk element
        if (this.runningMode != RunningMode.HBASE
            && !this.tableRowKeyElement.containsKey(tableName)) {
            throw new IllegalArgumentException("Row key element is empty for " + tableName);
        }

        // Get the latest table entry
        TableEntry tableEntry = getOrRefreshTableEntry(tableName, false);

        // Define start keys
        byte[][][] firstPartStartKeys = new byte[0][][];

        if (tableEntry.isPartitionTable()) {
            if (null != tableEntry.getPartitionInfo()) {
                if (null != tableEntry.getPartitionInfo().getFirstPartDesc()) {
                    ObPartFuncType obPartFuncType = tableEntry.getPartitionInfo()
                        .getFirstPartDesc().getPartFuncType();
                    if (obPartFuncType.isRangePart()) {
                        // Range Part
                        ObRangePartDesc rangePartDesc = (ObRangePartDesc) tableEntry
                            .getPartitionInfo().getFirstPartDesc();
                        List<List<byte[]>> highBoundVals = rangePartDesc.getHighBoundValues();
                        int startKeysLen = highBoundVals.size();
                        int partKeyLen = highBoundVals.get(0).size();
                        firstPartStartKeys = new byte[startKeysLen][][];

                        // Init start keys
                        firstPartStartKeys[0] = new byte[partKeyLen][];
                        for (int i = 0; i < partKeyLen; i++) {
                            firstPartStartKeys[0][i] = new byte[0];
                        }

                        // Fulfill other start keys
                        for (int i = 0; i < startKeysLen - 1; i++) {
                            List<byte[]> innerList = highBoundVals.get(i);
                            firstPartStartKeys[i + 1] = new byte[innerList.size()][];
                            for (int j = 0; j < innerList.size(); j++) {
                                firstPartStartKeys[i + 1][j] = innerList.get(j);
                            }
                        }
                    } else {
                        // Key / Hash Part
                        ObPartDesc partDesc = tableEntry.getPartitionInfo().getFirstPartDesc();
                        int partKeyLen = partDesc.getPartColumns().size();

                        // Init start keys
                        firstPartStartKeys = new byte[1][partKeyLen][];
                        Arrays.fill(firstPartStartKeys[0], new byte[0]);
                    }
                }
            }
        } else {
            // Non partition table
            firstPartStartKeys = new byte[1][1][];
            Arrays.fill(firstPartStartKeys[0], new byte[0]);
        }

        return firstPartStartKeys;
    }

    /*
     * Get the start keys of different tablets, byte[0] = [] = EMPTY_START_ROW = EMPTY_BYTE_ARRAY
     * Example:
     *   For Key   Partition: return [[[]]]
     *   For Hash  Partition: return [[[]]]
     *   For Range Partition: return [['a', [], []], ['z', 'b', 'c'], [[], [], []]]
     */
    public byte[][][] getFirstPartEndKeys(String tableName) throws Exception {
        // Check row key element
        // getOrRefreshTableEntry() need row key element, we could remove this after we remove rk element
        if (this.runningMode != RunningMode.HBASE && this.tableRowKeyElement.containsKey(tableName)) {
            throw new IllegalArgumentException("Row key element is empty for " + tableName);
        }

        // Get the latest table entry
        TableEntry tableEntry = getOrRefreshTableEntry(tableName, false);

        // Define end keys
        byte[][][] firstPartEndKeys = new byte[0][][];

        if (tableEntry.isPartitionTable()) {
            if (null != tableEntry.getPartitionInfo()) {
                if (null != tableEntry.getPartitionInfo().getFirstPartDesc()) {
                    ObPartFuncType obPartFuncType = tableEntry.getPartitionInfo()
                        .getFirstPartDesc().getPartFuncType();
                    if (obPartFuncType.isRangePart()) {
                        // Range Part
                        ObRangePartDesc rangePartDesc = (ObRangePartDesc) tableEntry
                            .getPartitionInfo().getFirstPartDesc();
                        List<List<byte[]>> highBoundVals = rangePartDesc.getHighBoundValues();
                        int endKeysLen = highBoundVals.size();
                        firstPartEndKeys = new byte[endKeysLen][][];

                        // Fulfill end keys
                        for (int i = 0; i < endKeysLen; i++) {
                            List<byte[]> innerList = highBoundVals.get(i);
                            firstPartEndKeys[i] = new byte[innerList.size()][];
                            for (int j = 0; j < innerList.size(); j++) {
                                firstPartEndKeys[i][j] = innerList.get(j);
                            }
                        }
                    } else {
                        // Key / Hash Part
                        ObPartDesc partDesc = tableEntry.getPartitionInfo().getFirstPartDesc();
                        int partKeyLen = partDesc.getPartColumns().size();

                        // Init end keys
                        firstPartEndKeys = new byte[1][partKeyLen][];
                        Arrays.fill(firstPartEndKeys[0], new byte[0]);
                    }
                }
            }
        } else {
            // Non partition table
            firstPartEndKeys = new byte[1][1][];
            Arrays.fill(firstPartEndKeys[0], new byte[0]);
        }

        return firstPartEndKeys;
    }

    /*
     * Get the start keys of HBase table
     * Example:
     *   For Non   Partition: return [EMPTY_START_ROW]
     *   For Key   Partition: return [EMPTY_START_ROW]
     *   For Hash  Partition: return [EMPTY_START_ROW]
     *   For Range Partition: return [[EMPTY_START_ROW, EMPTY_START_ROW, EMPTY_START_ROW], ['a', [], []], ['z', 'b', 'c']]
     */
    public byte[][] getHBaseTableStartKeys(String hbaseTableName) throws Exception {
        // Check HBase client
        if (this.runningMode != RunningMode.HBASE) {
            throw new IllegalArgumentException("getHBaseTableStartKeys only support in HBase mode");
        }

        // Get actual table name
        String tableName = tryGetTableNameFromTableGroupCache(hbaseTableName, false);

        // Get start keys of first partition
        byte[][][] firstPartStartKeys = getFirstPartStartKeys(tableName);

        // Result start keys
        byte[][] startKeys = new byte[firstPartStartKeys.length][];

        // Construct result keys
        for (int i = 0; i < firstPartStartKeys.length; i++) {
            if (firstPartStartKeys[i] == null || firstPartStartKeys[i].length != 1
                || firstPartStartKeys[i][0] == null || firstPartStartKeys[i][0].length > 1) {
                throw new IllegalArgumentException("Invalid start keys structure at index " + i
                                                   + " for table " + hbaseTableName);
            }
            startKeys[i] = firstPartStartKeys[i][0];
        }

        return startKeys;
    }

    /*
     * Get the start keys of HBase table
     * Example:
     *   For Non   Partition: return [EMPTY_END_ROW]
     *   For Key   Partition: return [EMPTY_END_ROW]
     *   For Hash  Partition: return [EMPTY_END_ROW]
     *   For Range Partition: return [['a', [], []], ['z', 'b', 'c'], [EMPTY_END_ROW, EMPTY_END_ROW, EMPTY_END_ROW]]
     */
    public byte[][] getHBaseTableEndKeys(String hbaseTableName) throws Exception {
        // Check HBase client
        if (this.runningMode != RunningMode.HBASE) {
            throw new IllegalArgumentException("getHBaseTableStartKeys only support in HBase mode");
        }

        // Get actual table name
        String tableName = tryGetTableNameFromTableGroupCache(hbaseTableName, false);

        // Get end keys of first partition
        byte[][][] firstPartEndKeys = getFirstPartEndKeys(tableName);

        // Result end keys
        byte[][] endKeys = new byte[firstPartEndKeys.length][];

        // Construct result keys
        for (int i = 0; i < firstPartEndKeys.length; i++) {
            if (firstPartEndKeys[i] == null || firstPartEndKeys[i].length != 1
                || firstPartEndKeys[i][0] == null || firstPartEndKeys[i][0].length > 1) {
                throw new IllegalArgumentException("Invalid end keys structure at index " + i
                                                   + " for table " + hbaseTableName);
            }
            endKeys[i] = firstPartEndKeys[i][0];
        }

        return endKeys;
    }
    public static void setRowKeyValue(Mutation mutation, int index, Object value) {
        if (mutation.getRowKeyValues() == null || (index < 0 || mutation.getRowKeyValues().size() <= index)) {
            throw new IllegalArgumentException("rowkey is null or index is out of range");
        }
        ((ObObj) mutation.getRowKeyValues().get(index)).setValue(value);
    }

    public static Object getRowKeyValue(Mutation mutation, int index) {
        if (mutation.getRowKeyValues() == null || (index < 0 || index >= mutation.getRowKeyValues().size())) {
            throw new IllegalArgumentException("rowkey is null or index is out of range");
        }
        return ((ObObj) mutation.getRowKeyValues().get(index)).getValue();
    }
}

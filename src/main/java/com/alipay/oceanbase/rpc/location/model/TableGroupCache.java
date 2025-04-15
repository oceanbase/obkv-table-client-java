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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableEntryRefreshException;
import com.alipay.oceanbase.rpc.exception.ObTableNotExistException;
import com.alipay.oceanbase.rpc.location.LocationUtil;
import org.slf4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.getLogger;

public class TableGroupCache {
    private static final Logger               logger               = getLogger(TableGroupCache.class);
    // tableGroup -> lock
    private ConcurrentHashMap<String, Lock>   TableGroupCacheLocks = new ConcurrentHashMap<String, Lock>();
    // tableGroup -> Table
    private ConcurrentHashMap<String, String> TableGroupCache      = new ConcurrentHashMap<String, String>();
    // Table -> tableGroup
    private ConcurrentHashMap<String, String> TableGroupInverted   = new ConcurrentHashMap<String, String>();
    private final ObTableClient               tableClient;

    public TableGroupCache(ObTableClient tableClient) {
        this.tableClient = tableClient;
    }

    public String tryGetTableNameFromTableGroupCache(final String tableGroupName,
                                                     final boolean refresh,
                                                     final ServerRoster serverRoster,
                                                     final ObUserAuth sysUA) throws Exception {
        String physicalTableName = TableGroupCache.get(tableGroupName); // tableGroup -> Table
        // get tableName from cache
        if (physicalTableName != null && !refresh) {
            return physicalTableName;
        }

        // not find in cache, should get tableName from observer
        Lock tempLock = new ReentrantLock();
        Lock lock = TableGroupCacheLocks.putIfAbsent(tableGroupName, tempLock);
        lock = (lock == null) ? tempLock : lock; // check the first lock

        long metadataRefreshLockTimeout = tableClient.getMetadataRefreshLockTimeout();
        // attempt lock the refreshing action, avoiding concurrent refreshing
        // use the time-out mechanism, avoiding the rpc hanging up
        boolean acquired = lock.tryLock(metadataRefreshLockTimeout, TimeUnit.MILLISECONDS);

        if (!acquired) {
            String errMsg = "try to lock tableGroup inflect timeout" + " ,tableGroupName:"
                            + tableGroupName + " , timeout:" + metadataRefreshLockTimeout + ".";
            RUNTIME.error(errMsg);
            throw new ObTableEntryRefreshException(errMsg);
        }

        try {
            String newPhyTableName = TableGroupCache.get(tableGroupName);
            if (((physicalTableName == null) && (newPhyTableName == null))
                || (refresh && newPhyTableName.equalsIgnoreCase(physicalTableName))) {
                if (logger.isInfoEnabled()) {
                    if (physicalTableName != null) {
                        logger.info(
                            "realTableName need refresh, create new table entry, tablename: {}",
                            tableGroupName);
                    } else {
                        logger.info(
                            "realTableName not exist, create new table entry, tablename: {}",
                            tableGroupName);
                    }
                }

                try {
                    return refreshTableNameByTableGroup(physicalTableName, tableGroupName,
                        serverRoster, sysUA);
                } catch (Throwable t) {
                    RUNTIME.error("getOrRefreshTableName from TableGroup meet exception", t);
                    throw t;
                }
            }
            return newPhyTableName;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 根据 tableGroup 获取其中一个tableName
     * physicalTableName Complete table from table group
     * @param physicalTableName
     * @param tableGroupName
     * @return
     * @throws ObTableNotExistException
     */
    private String refreshTableNameByTableGroup(String physicalTableName, String tableGroupName,
                                                final ServerRoster serverRoster,
                                                final ObUserAuth sysUA)
                                                                       throws ObTableNotExistException {
        TableEntryKey tableEntryKey = new TableEntryKey(tableClient.getClusterName(),
            tableClient.getTenantName(), tableClient.getDatabase(), tableGroupName);
        String oldTableName = physicalTableName;
        try {
            physicalTableName = LocationUtil.loadTableNameWithGroupName(serverRoster, //
                tableEntryKey,//
                tableClient.getTableEntryAcquireConnectTimeout(),//
                tableClient.getTableEntryAcquireSocketTimeout(),//
                tableClient.getServerAddressPriorityTimeout(), sysUA);
        } catch (ObTableNotExistException e) {
            RUNTIME.error("refreshTableNameByTableGroup from tableGroup meet exception", e);
            throw e;
        } catch (Exception e) {
            RUNTIME.error("refreshTableEntry from tableGroup meet exception", tableEntryKey,
                physicalTableName, e);
            throw new ObTableNotExistException(String.format(
                "failed to get table name key=%s original tableName=%s ", tableEntryKey,
                physicalTableName), e);
        }
        if (!TableGroupInverted.isEmpty() && oldTableName != null
            && TableGroupInverted.containsKey(oldTableName)) {
            TableGroupInverted.remove(oldTableName, tableGroupName);
        }
        TableGroupCache.put(tableGroupName, physicalTableName);
        TableGroupInverted.put(physicalTableName, tableGroupName);
        if (logger.isInfoEnabled()) {
            logger
                .info(
                    "get table name from tableGroup, tableGroupName: {}, refresh: {} key:{} realTableName:{} ",
                    tableGroupName, true, tableEntryKey, physicalTableName);
        }
        return physicalTableName;
    }

    public void eraseTableGroupFromCache(String tableGroupName) {
        // clear table group cache
        TableGroupInverted.remove(TableGroupCache.get(tableGroupName));
        TableGroupCache.remove(tableGroupName);
        TableGroupCacheLocks.remove(tableGroupName);
    }

    public ConcurrentHashMap<String, String> getTableGroupInverted() {
        return TableGroupInverted;
    }

    public ConcurrentHashMap<String, String> getTableGroupCache() {
        return TableGroupCache;
    }
}

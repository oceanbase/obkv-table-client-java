/*-
* #%L
 * * OceanBase Table Client Framework
 * *
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
 * *
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

package com.alipay.oceanbase.rpc.dds;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

import com.alibaba.fastjson.JSON;
import com.alipay.common.tracer.util.LoadTestUtil;
import com.alipay.oceanbase.rpc.Lifecycle;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.OperationExecuteAble;
import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.dds.config.DistributeConfigHandler;
import com.alipay.oceanbase.rpc.dds.config.DdsConfigUpdateHandler;
import com.alipay.oceanbase.rpc.dds.group.ObTableClientGroup;
import com.alipay.oceanbase.rpc.dds.rule.DatabaseAndTable;
import com.alipay.oceanbase.rpc.dds.rule.DistributeDispatcher;
import com.alipay.oceanbase.rpc.dds.rule.LogicalTable;
import com.alipay.oceanbase.rpc.dds.util.ConfigWrapper;
import com.alipay.oceanbase.rpc.dds.util.DataSourceFactory;
import com.alipay.oceanbase.rpc.dds.util.VersionedConfigSnapshot;
import com.alipay.oceanbase.rpc.exception.DistributeDispatchException;
import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.exception.ObTableCloseException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.mutation.Append;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Delete;
import com.alipay.oceanbase.rpc.mutation.Increment;
import com.alipay.oceanbase.rpc.mutation.Insert;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.MutationFactory;
import com.alipay.oceanbase.rpc.mutation.Put;
import com.alipay.oceanbase.rpc.mutation.Replace;
import com.alipay.oceanbase.rpc.mutation.Update;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import static com.alipay.oceanbase.rpc.property.Property.DDS_CONFIG_FATCH_TIMEOUT;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableAbstractOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObBorderFlag;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.table.AbstractTable;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.BOOT;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.getLogger;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceWeight;
import com.alipay.sofa.dds.config.group.AtomDataSourceWeight;
import com.alipay.sofa.dds.config.rule.AppRule;

/**
* @author zhiqi.zzq
* @since 2021/7/7 下午5:49
*/
public class DdsObTableClient extends AbstractTable implements OperationExecuteAble, Lifecycle {

    private static final Logger              logger      = TableClientLoggerFactory
                                                            .getRULELogger();

    private String                           appName;
    private String                           appDsName;
    private String                           version;
    private ObTableClient.RunningMode        runningMode = ObTableClient.RunningMode.NORMAL;
    private long                             configFetchOnceTimeoutMillis = DDS_CONFIG_FATCH_TIMEOUT.getDefaultLong();

    private String                           testLoadSuffix;
    public static final String               DEFAULT_TEST_LOAD_SUFFIX     = "_t";
    public static final String               DEFAULT_HBASE_FAMILY_SEP     = "$";

    private final AtomicReference<VersionedConfigSnapshot> currentConfigRef = new AtomicReference<>();

    // private Map<String, ObTableClient>       atomDataSources;
    // private Map<Integer, ObTableClientGroup> groupDataSources;

    private DistributeDispatcher             distributeDispatcher;

    private volatile boolean                 initialized                  = false;
    private volatile boolean                 closed                       = false;
    private ReentrantLock                    statusLock                   = new ReentrantLock();
    
    // 添加对DdsConfigUpdateHandler的引用，用于资源清理
    private DdsConfigUpdateHandler           dynamicHandler;

    private final Map<String, String[]>            tableRowKeyElement = new ConcurrentHashMap<>();
    private Properties                       tableClientProperty = new Properties();
    /**
    *
    * @param tableName
    * @param columns
    */
    public void addRowKeyElement(String tableName, String[] columns) {
        if (columns == null || columns.length == 0) {
            RUNTIME.error("add row key element error table "+ tableName
                    + " column " + Arrays.toString(columns));
            throw new IllegalArgumentException("add row key element error table " + tableName
                    + " column " + Arrays.toString(columns));
        }
        tableRowKeyElement.put(tableName, columns);
    }

    /**
    *
    * @throws Exception
    */
    @Override
    public void init() throws Exception {
        if (initialized) {
            return;
        }
        statusLock.lock();
        try {
            if (initialized) {
                return;
            }

            this.dynamicHandler = new DdsConfigUpdateHandler(
                runningMode,
                tableClientProperty,
                currentConfigRef);
            // 设置配置更新回调，用于在新增数据源时同步 row key elements
            // 注意：回调在配置生效前执行，确保新数据源在暴露前已完成配置同步
            dynamicHandler.setConfigUpdateCallback((newConfig, oldConfig) -> {
                syncRowKeyElementsToAllClients(newConfig);
            });
            // 初始化配置处理器
            DistributeConfigHandler distributeConfigHandler = new DistributeConfigHandler(appName,
                appDsName, version, configFetchOnceTimeoutMillis, dynamicHandler);
            dynamicHandler.setExtendedDataSourceSupplier(distributeConfigHandler::getExtendedDataSourceConfigs);
            dynamicHandler.setAppRuleSupplier(distributeConfigHandler::getAppRule);
            distributeConfigHandler.init();

            // 获取初始配置
            GroupClusterConfig groupClusterConfig = distributeConfigHandler.getGroupClusterConfig();
            Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs = new ConcurrentHashMap<>();
            Map<String, ExtendedDataSourceConfig> fetchedConfigs =
                distributeConfigHandler.getExtendedDataSourceConfigs();
            if (fetchedConfigs != null) {
                extendedDataSourceConfigs.putAll(fetchedConfigs);
            }

            // 初始化数据源 (will create and set initial config snapshot)
            initDistributeDataSource(groupClusterConfig, extendedDataSourceConfigs,
                distributeConfigHandler.getAppRule());

            // init distribute dispatcher
            distributeDispatcher = new DistributeDispatcher(currentConfigRef::get);
            distributeDispatcher.init();
            
            // Sync row key elements to all ObTableClient instances after initialization
            syncRowKeyElementsToAllClients();
            
            initialized = true;
        } catch (Exception t) {
            logger.error("DdsObTableClient init failed", t);
            throw new RuntimeException(t);
        } finally {
            statusLock.unlock();
        }
    }

    /**
     * Warm up all ObTableClient instances with table names
     * This method will iterate through all ObTableClient instances and call warmUp
     * to pre-load table entries for better performance.
     * 
     * @param tableNames array of table names to warm up
     * @throws Exception if warmUp fails
     */
    public void warmUp(String[] tableNames) throws Exception {
        if (tableNames == null || tableNames.length == 0) {
            logger.warn("Table names array is null or empty, skip warmUp");
            return;
        }

        checkStatus();

        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }

        // Get all ObTableClient instances from atom data sources (with deduplication)
        Map<String, ObTableClient> atomDataSources = snapshot.getAtomDataSources();
        if (atomDataSources == null || atomDataSources.isEmpty()) {
            logger.warn("No atom data sources found, skip warmUp");
            return;
        }

        Set<ObTableClient> allTableClients = new HashSet<>(atomDataSources.values());

        if (allTableClients.isEmpty()) {
            logger.warn("No ObTableClient instances found, skip warmUp");
            return;
        }

        logger.info("Starting warmUp for {} table(s) on {} ObTableClient instance(s)", 
            tableNames.length, allTableClients.size());

        // Warm up each ObTableClient instance
        int successCount = 0;
        int failureCount = 0;
        Exception lastException = null;
        
        for (ObTableClient tableClient : allTableClients) {
            try {
                tableClient.warmUp(tableNames);
                successCount++;
            } catch (Exception e) {
                failureCount++;
                lastException = e;
                logger.warn("Failed to warmUp ObTableClient: {}", e.getMessage(), e);
            }
        }

        logger.info("WarmUp completed: {} success, {} failure", successCount, failureCount);
        
        // If all warmUp operations failed, throw the last exception
        if (successCount == 0 && lastException != null) {
            throw new Exception("All warmUp operations failed", lastException);
        }
    }

    /**
    *
    * @throws Exception
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

            Map<String, ObTableClient> atomDataSources = getAtomDataSources();
            if (atomDataSources != null) {
                Exception throwException = null;
                List<String> exceptionClient = new ArrayList<>();
                for (Map.Entry<String, ObTableClient> entry : atomDataSources.entrySet()) {
                    try {
                        entry.getValue().close();
                    } catch (Exception e) {
                        // do not throw exception immediately
                        logger.error(LCD.convert("02-10000"), entry.getKey(), e);
                        throwException = e;
                        exceptionClient.add(entry.getKey());
                    }
                }
                if (!exceptionClient.isEmpty()) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("following atom data source [");
                    for (int i = 0; i < exceptionClient.size(); i++) {
                        if (i != 0) {
                            sb.append(",");
                        }
                        sb.append(exceptionClient.get(i));
                    }
                    sb.append("] close error.");
                    throw new ObTableCloseException(sb.toString(), throwException);
                }
            }

            // ✅ 修复：清理动态处理器中的ScheduledExecutorService，防止资源泄漏
            if (dynamicHandler != null) {
                try {
                    dynamicHandler.shutdownAsyncCleanup();
                } catch (Exception e) {
                    logger.error("Failed to shutdown async cleanup executor", e);
                }
            }

        } finally {
            statusLock.unlock();
        }
    }

    /**
    * Check status.
    */
    public void checkStatus() throws IllegalStateException {
        if (!initialized) {
            throw new IllegalStateException("OBKV-STATUS app name [" + appName
                                            + "] app data source name [" + appDsName
                                            + "] version [" + version + "] is not initialized.");
        }

        if (closed) {
            throw new IllegalStateException("OBKV-STATUS app name [" + appName
                                            + "] app data source name [" + appDsName
                                            + "] version [" + version + "] is closed.");
        }
    }


    private void initDistributeDataSource(GroupClusterConfig groupClusterConfig,
                                          Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs,
                                          AppRule appRule)
                                                                                                          throws Exception {
        BOOT.info("init AtomicDataSourceConcurrent start");

        Map<String, ObTableClient> atomDataSources = DataSourceFactory.createAtomDataSources(
            extendedDataSourceConfigs, this.runningMode, this.tableClientProperty);
        
        BOOT.info("init AtomDataSourceConcurrent end  " + atomDataSources);
        
        if (groupClusterConfig != null && !groupClusterConfig.getGroupCluster().isEmpty()) {
            validateDataSourcesReadyForGroups(groupClusterConfig, atomDataSources);
        }
        
        BOOT.info("init GroupDataSourceConcurrent start");

        Map<Integer, ObTableClientGroup> groupDataSources = DataSourceFactory.createGroupDataSources(
            groupClusterConfig, atomDataSources);
        
        BOOT.info("init GroupDataSourceConcurrent end  " + JSON.toJSONString(groupDataSources.keySet()));
        
        // Create initial configuration snapshot
        VersionedConfigSnapshot initialConfig = ConfigWrapper.wrapConfig(
            groupClusterConfig, extendedDataSourceConfigs, appRule,
            atomDataSources, groupDataSources);
        currentConfigRef.set(initialConfig);
    }

    private Map<String, ObTableClient> getAtomDataSources() {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            return Collections.emptyMap();
        }
        Map<String, ObTableClient> atomDataSources = snapshot.getAtomDataSources();
        return atomDataSources;
    }

    private Map<Integer, ObTableClientGroup> getGroupDataSources() {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            return Collections.emptyMap();
        }
        Map<Integer, ObTableClientGroup> groupDataSources = snapshot.getGroupDataSources();
        return groupDataSources;
    }

    /**
     * Sync row key elements from DdsObTableClient to all managed ObTableClient instances.
     * This method collects all ObTableClient instances from atomDataSources.
     * Note: groupDataSources contain references to the same ObTableClient instances from atomDataSources,
     * so we only need to sync to atomDataSources.
     *
     * This method applies all row key elements that were set via addRowKeyElement() before init().
     *
     * @param snapshot the configuration snapshot to sync from (pass null to use currentConfigRef)
     */
    private void syncRowKeyElementsToAllClients(VersionedConfigSnapshot snapshot) {
        if (tableRowKeyElement.isEmpty()) {
            // No row key elements to sync
            return;
        }

        // Use provided snapshot or fall back to currentConfigRef
        VersionedConfigSnapshot targetSnapshot = snapshot;
        if (targetSnapshot == null) {
            targetSnapshot = currentConfigRef.get();
        }

        if (targetSnapshot == null) {
            logger.warn("Configuration snapshot is not available, cannot sync row key elements");
            return;
        }

        // Get all ObTableClient instances from atomDataSources
        // Note: groupDataSources contain references to the same instances, so we only need atomDataSources
        Map<String, ObTableClient> atomDataSources = targetSnapshot.getAtomDataSources();
        if (atomDataSources == null || atomDataSources.isEmpty()) {
            logger.warn("No ObTableClient instances found to sync row key elements");
            return;
        }

        // Sync each row key element to all ObTableClient instances
        for (Map.Entry<String, String[]> entry : tableRowKeyElement.entrySet()) {
            String tableName = entry.getKey();
            String[] rowKeyColumns = entry.getValue();

            if (rowKeyColumns == null || rowKeyColumns.length == 0) {
                continue;
            }

            for (ObTableClient tableClient : atomDataSources.values()) {
                try {
                    tableClient.addRowKeyElement(tableName, rowKeyColumns);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Synced row key element for table {} to ObTableClient: {}",
                            tableName, Arrays.toString(rowKeyColumns));
                    }
                } catch (Exception e) {
                    logger.warn("Failed to sync row key element for table {} to ObTableClient: {}",
                        tableName, e.getMessage());
                }
            }
        }

        if (logger.isInfoEnabled()) {
            logger.info("Synced {} row key element(s) to {} ObTableClient instance(s)",
                tableRowKeyElement.size(), atomDataSources.size());
        }
    }

    /**
     * Sync row key elements using current config (convenience method for init phase).
     * This is called during initialization to sync to all existing data sources.
     */
    private void syncRowKeyElementsToAllClients() {
        syncRowKeyElementsToAllClients(null);
    }


    @Override
    public TableEntry getOrRefreshTableEntry(final String tableName, final Integer groupID, final boolean refresh,
                                            final boolean waitForRefresh) throws Exception {
        Map<String, ObTableClient> atomDataSources = getAtomDataSources();
        getLogger().info("DDS getOrRefreshTableEntry for {}, atomDataSources size {}, keys {}", tableName, atomDataSources.size(), JSON.toJSONString(atomDataSources.keySet()));
        Map<Integer, ObTableClientGroup> groupDataSources = getGroupDataSources();
        ObTableClientGroup obTableClientGroup = groupDataSources.get(groupID);
        ObTableClient obTableClient = obTableClientGroup.select(true, 0);

        try {
            return obTableClient.getOrRefreshTableEntry(tableName, refresh, waitForRefresh);
        } catch (Exception t) {
            logger.error("getOrRefreshTableEntry execption", t);
            throw new ObTableException("getOrRefreshTableEntry execption", t);
        }
    }

    @Override
    public TableEntry getOrRefreshTableEntry(final String tableName, final boolean refresh,
                                            final boolean waitForRefresh) throws Exception {
        Map<String, ObTableClient> atomDataSources = getAtomDataSources();
        getLogger().info("DDS getOrRefreshTableEntry for {}, atomDataSources size {}, keys {}", tableName, atomDataSources.size(), JSON.toJSONString(atomDataSources.keySet()));
        for (Map.Entry<String, ObTableClient> entry: atomDataSources.entrySet()) {
            try {
                entry.getValue().getOrRefreshTableEntry(tableName, refresh, waitForRefresh);
            } catch (Exception t) {
                logger.error("getOrRefreshTableEntry execption", t);
            }
        }

        return null;
    }


    @Override
    public void setRuntimeBatchExecutor(ExecutorService runtimeBatchExecutor) {
        Map<String, ObTableClient> atomDataSources = getAtomDataSources();
        if (atomDataSources != null) {
            for (Map.Entry<String, ObTableClient> entry: atomDataSources.entrySet()) {
                try {
                    entry.getValue().setRuntimeBatchExecutor(runtimeBatchExecutor);
                } catch (Throwable t) {
                    logger.error("getOrRefreshTableEntry execption", t);
                }
            }
        } else {
            BOOT.warn("atomDataSources is null, setRuntimeBatchExecutor should after at dds init");
        }

    }

    /**
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    @Override
    public TableQuery query(String tableName) throws Exception {
        checkStatus();
        return new DdsObTableClientQuery(tableName, this);
    }

    public TableQuery query(String tableName, int groupID, String[] rowKeyColumns) {
        checkStatus();
        DdsObTableClientQuery res = new DdsObTableClientQuery(tableName, this);
        res.SetDatabaseAndTable(groupID, tableName, rowKeyColumns);
        return res;
    }

    // /**
    //  *
    //  * @param tableName
    //  * @return
    //  * @throws Exception
    //  */
    // @Override
    // public TableQuery queryByBatchV2(String tableName) throws Exception {
    //     checkStatus();
    //     throw new FeatureNotSupportedException("async query is not supported");
    // }

    // /**
    //  *
    //  * @param tableName
    //  * @return
    //  * @throws Exception
    //  */
    // @Override
    // public TableQuery queryByBatch(String tableName) throws Exception {
    //     checkStatus();
    //     throw new FeatureNotSupportedException("query by batch is not supported");

    // }

    /**
     *
     * @param tableName
     * @return
     * @throws Exception
     */
    @Override
    public TableBatchOps batch(String tableName) throws Exception {
        checkStatus();
        return new DdsObTableClientBatchOps(tableName, this);
    }

    private DatabaseAndTable getDatabaseAndTable(String tableName, Object[] rowkeys, boolean readOnly) {
        return getDatabaseAndTable(tableName, rowkeys, readOnly, null);
    }

    private DatabaseAndTable getDatabaseAndTable(String tableName, Object[] rowkeys, boolean readOnly,
                                                  VersionedConfigSnapshot snapshot) {
        checkStatus();
        VersionedConfigSnapshot actualSnapshot = snapshot != null ? snapshot : currentConfigRef.get();
        return calculateDatabaseAndTable(tableName, rowkeys, readOnly, actualSnapshot);
    }

    private ObTableClient buildObTableClient(DatabaseAndTable databaseAndTable) {
        return buildObTableClient(databaseAndTable, null);
    }

    private ObTableClient buildObTableClient(DatabaseAndTable databaseAndTable, VersionedConfigSnapshot snapshot) {
        Map<Integer, ObTableClientGroup> currentGroupDataSources;
        if (snapshot != null) {
            currentGroupDataSources = snapshot.getGroupDataSources();
        } else {
            currentGroupDataSources = getGroupDataSources();
        }
        
        if (currentGroupDataSources == null) {
            logger.error("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
            throw new DistributeDispatchException("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
        }
        
        ObTableClientGroup obTableClientGroup = currentGroupDataSources.get(databaseAndTable
                .getDatabaseShardValue());

        if (obTableClientGroup == null) {
            logger.error("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
            throw new DistributeDispatchException("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
        }

        ObTableClient obTableClient = obTableClientGroup.select(!databaseAndTable.isReadOnly(),
                databaseAndTable.getElasticIndexValue());

        if (obTableClient == null) {
            logger.error("No ObTableClient available for shard: {} (readOnly: {}, elasticIndex: {})",
                    databaseAndTable.getDatabaseShardValue(),
                    databaseAndTable.isReadOnly(),
                    databaseAndTable.getElasticIndexValue());
            throw new DistributeDispatchException("No ObTableClient available for shard: "
                    + databaseAndTable.getDatabaseShardValue());
        }

        if (databaseAndTable.getRowKeyColumns() != null) {
            obTableClient.addRowKeyElement(databaseAndTable.getTableName(),
                    databaseAndTable.getRowKeyColumns());
        }
        return obTableClient;
    }
    /**
     *
     * @param tableName
     * @param rowkeys
     * @param columns
     * @return
     * @throws Exception
     */
    @Override
    public Map<String, Object> get(String tableName, Object[] rowkeys, String[] columns)
                                                                                        throws Exception {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, true, snapshot);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable, snapshot);

        return obTableClient.get(getTargetTableName(databaseAndTable.getTableName()), rowkeys,
            columns);
    }

    /**
     *
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param values
     * @return
     * @throws Exception
     */
    @Override
    public long update(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                             throws Exception {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false, snapshot);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable, snapshot);

        return obTableClient.update(getTargetTableName(databaseAndTable.getTableName()), rowkeys, columns, values);
    }

    /**
     *
     * @param tableName
     * @param rowkeys
     * @return
     * @throws Exception
     */
    @Override
    public long delete(String tableName, Object[] rowkeys) throws Exception {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, true, snapshot);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable, snapshot);

        return obTableClient.delete(getTargetTableName(databaseAndTable.getTableName()), rowkeys);
    }

    /**
     *
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param values
     * @return
     * @throws Exception
     */
    @Override
    public long insert(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                             throws Exception {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false, snapshot);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable, snapshot);

        return obTableClient.insert(getTargetTableName(databaseAndTable.getTableName()), rowkeys, columns, values);
    }

    /**
     *
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param values
     * @return
     * @throws Exception
     */
    @Override
    public long replace(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                              throws Exception {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false, snapshot);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable, snapshot);

        return obTableClient.replace(getTargetTableName(databaseAndTable.getTableName()), rowkeys, columns, values);
    }

    /**
     *
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param values
     * @return
     * @throws Exception
     */
    @Override
    public long insertOrUpdate(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                                     throws Exception {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false, snapshot);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable, snapshot);

        return obTableClient.insertOrUpdate(getTargetTableName(databaseAndTable.getTableName()), rowkeys, columns,
            values);
    }

    /**
     *
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param values
     * @param withResult
     * @return
     * @throws Exception
     */
    @Override
    public Map<String, Object> increment(String tableName, Object[] rowkeys, String[] columns,
                                         Object[] values, boolean withResult) throws Exception {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false, snapshot);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable, snapshot);

        return obTableClient.increment(getTargetTableName(databaseAndTable.getTableName()), rowkeys, columns, values,
            withResult);
    }

    /**
     *
     * @param tableName
     * @param rowkeys
     * @param columns
     * @param values
     * @param withResult
     * @return
     * @throws Exception
     */
    @Override
    public Map<String, Object> append(String tableName, Object[] rowkeys, String[] columns,
                                      Object[] values, boolean withResult) throws Exception {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false, snapshot);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable, snapshot);

        return obTableClient.append(getTargetTableName(databaseAndTable.getTableName()), rowkeys, columns, values,
            withResult);
    }

    // @Override @Deprecated
    // public Map<String, Object> groupIncrement(String reqNo, String tableName, Object[] rowkeys, String[] columns,
    //                            Object[] values, ObTableGroupComparator cmp, boolean withResult) throws Exception {
    //     DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false);
    //     ObTableClient obTableClient = buildObTableClient(databaseAndTable);

    //     return obTableClient.groupIncrement(reqNo, getTargetTableName(databaseAndTable.getTableName()),
    //             rowkeys, columns, values, cmp, withResult);
    // }

    // @Override @Deprecated
    // public Map<Long, Long> groupIncCheck(String reqNo, String tableName, Object[] rowkeys) throws Exception {
    //     DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false);
    //     ObTableClient obTableClient = buildObTableClient(databaseAndTable);

    //     return obTableClient.groupIncCheck(reqNo, getTargetTableName(databaseAndTable.getTableName()), rowkeys);
    // }


    // @Override
    // public long queryAndUpdate(TableQuery query, String[] columns, Object[] values) throws Exception {
    //     DatabaseAndTable databaseAndTable = calculateDatabaseAndTable(query.getTableName(), query.getObTableQuery());
    //     ObTableClient obTableClient = buildObTableClient(databaseAndTable);
    //     DdsObTableClientQuery newQuery = new DdsObTableClientQuery(databaseAndTable.getTableName(), query.getObTableQuery());
    //     return obTableClient.queryAndUpdate(newQuery, columns, values);
    // }

    // @Override
    // public long queryAndAppend(TableQuery query, String[] columns, Object[] values) throws Exception {
    //     DatabaseAndTable databaseAndTable = calculateDatabaseAndTable(query.getTableName(), query.getObTableQuery());
    //     ObTableClient obTableClient = buildObTableClient(databaseAndTable);
    //     DdsObTableClientQuery newQuery = new DdsObTableClientQuery(databaseAndTable.getTableName(), query.getObTableQuery());
    //     return obTableClient.queryAndAppend(newQuery, columns, values);
    // }

    // @Override
    // public long queryAndIncrement(TableQuery query, String[] columns, Object[] values) throws Exception {
    //     DatabaseAndTable databaseAndTable = calculateDatabaseAndTable(query.getTableName(), query.getObTableQuery());
    //     ObTableClient obTableClient = buildObTableClient(databaseAndTable);
    //     DdsObTableClientQuery newQuery = new DdsObTableClientQuery(databaseAndTable.getTableName(), query.getObTableQuery());
    //     return obTableClient.queryAndIncrement(newQuery, columns, values);
    // }

    // @Override
    // public long queryAndDelete(TableQuery query) throws Exception {
    //     DatabaseAndTable databaseAndTable = calculateDatabaseAndTable(query.getTableName(), query.getObTableQuery());
    //     ObTableClient obTableClient = buildObTableClient(databaseAndTable);
    //     DdsObTableClientQuery newQuery = new DdsObTableClientQuery(databaseAndTable.getTableName(), query.getObTableQuery());
    //     return obTableClient.queryAndDelete(newQuery);
    // }

    /**
     *
     * @param request
     * @return
     * @throws Exception
     */
    @Override
    public ObPayload execute(ObTableAbstractOperationRequest request) throws Exception {

        checkStatus();

        // 在请求开始时获取快照，确保整个请求使用同一个快照，保证配置一致性
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }

        String tableName = request.getTableName();

        DatabaseAndTable databaseAndTable = null;
        if (request instanceof ObTableOperationRequest) {
            ObTableOperation operation = ((ObTableOperationRequest) request).getTableOperation();

            databaseAndTable = calculateDatabaseAndTable(tableName, operation, snapshot);

        } else if (request instanceof ObTableQueryRequest) {
            ObTableQuery tableQuery = ((ObTableQueryRequest) request).getTableQuery();
            databaseAndTable = calculateDatabaseAndTable(tableName, tableQuery, snapshot);

        } else if (request instanceof ObTableBatchOperationRequest) {
            List<ObTableOperation> operations = ((ObTableBatchOperationRequest) request)
                .getBatchOperation().getTableOperations();

            databaseAndTable = calculateDatabaseAndTable(tableName, operations, snapshot);
        } else if (request instanceof ObTableQueryAndMutateRequest) {
            ObTableQuery tableQuery = ((ObTableQueryAndMutateRequest) request)
                .getTableQueryAndMutate().getTableQuery();
            databaseAndTable = calculateDatabaseAndTable(tableName, tableQuery, snapshot);
            databaseAndTable.setReadOnly(false);
        } else {
            throw new FeatureNotSupportedException(
                "request type " + request.getClass().getSimpleName()
                        + "is not supported. make sure the correct version");
        }

        // 使用同一个快照获取客户端组，确保配置一致性
        Map<Integer, ObTableClientGroup> currentGroupDataSources = snapshot.getGroupDataSources();
        if (currentGroupDataSources == null) {
            logger.error("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
            throw new DistributeDispatchException("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
        }
        ObTableClientGroup obTableClientGroup = currentGroupDataSources.get(databaseAndTable
            .getDatabaseShardValue());

        if (obTableClientGroup == null) {
            logger.error("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
            throw new DistributeDispatchException("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
        }

        ObTableClient obTableClient = obTableClientGroup.select(!databaseAndTable.isReadOnly(),
            databaseAndTable.getElasticIndexValue());

        if (databaseAndTable.getRowKeyColumns() != null) {
            obTableClient.addRowKeyElement(databaseAndTable.getTableName(),
                databaseAndTable.getRowKeyColumns());
        }

        request.setTableName(getTargetTableName(databaseAndTable.getTableName()));

        return obTableClient.execute(request);
    }

    /**
     *
     * @param databaseAndTable
     * @return
     */
    public ObTableClient getObTable(DatabaseAndTable databaseAndTable) {
        // ✅ 修复：使用当前配置快照，确保请求内配置一致性
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        
        Map<Integer, ObTableClientGroup> groupDataSources = snapshot.getGroupDataSources();
        if (groupDataSources == null) {
            throw new DistributeDispatchException("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
        }
        
        ObTableClientGroup obTableClientGroup = groupDataSources.get(databaseAndTable
            .getDatabaseShardValue());

        if (obTableClientGroup == null) {
            throw new DistributeDispatchException("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
        }

        ObTableClient obTableClient = obTableClientGroup.select(!databaseAndTable.isReadOnly(),
            databaseAndTable.getElasticIndexValue());

        if (obTableClient == null) {
            throw new DistributeDispatchException("No ObTableClient available for shard: " + databaseAndTable.getDatabaseShardValue());
        }

        if (databaseAndTable.getRowKeyColumns() != null) {
            obTableClient.addRowKeyElement(databaseAndTable.getTableName(),
                databaseAndTable.getRowKeyColumns());
        }

        return obTableClient;
    }

    /**
     * Get ObTableClient with specific configuration snapshot (for internal use)
     * @param databaseAndTable
     * @param snapshot
     * @return
     */
    ObTableClient getObTableWithSnapshot(DatabaseAndTable databaseAndTable, VersionedConfigSnapshot snapshot) {
        Map<Integer, ObTableClientGroup> groupDataSources = snapshot.getGroupDataSources();
        if (groupDataSources == null) {
            throw new DistributeDispatchException("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
        }
        
        ObTableClientGroup obTableClientGroup = groupDataSources.get(databaseAndTable
            .getDatabaseShardValue());

        if (obTableClientGroup == null) {
            throw new DistributeDispatchException("No client group found for shard: " + databaseAndTable.getDatabaseShardValue());
        }

        ObTableClient obTableClient = obTableClientGroup.select(!databaseAndTable.isReadOnly(),
            databaseAndTable.getElasticIndexValue());

        if (obTableClient == null) {
            throw new DistributeDispatchException("No ObTableClient available for shard: " + databaseAndTable.getDatabaseShardValue());
        }

        if (databaseAndTable.getRowKeyColumns() != null) {
            obTableClient.addRowKeyElement(databaseAndTable.getTableName(),
                databaseAndTable.getRowKeyColumns());
        }

        return obTableClient;
    }

    private DatabaseAndTable calculateDatabaseAndTable(String tableName, Object[] rowKey,
                                                       boolean readOnly) {
        return calculateDatabaseAndTable(tableName, rowKey, readOnly, null);
    }

    private DatabaseAndTable calculateDatabaseAndTable(String tableName, Object[] rowKey,
                                                       boolean readOnly, VersionedConfigSnapshot snapshot) {

        LogicalTable logicalTable = distributeDispatcher.findLogicalTable(tableName, snapshot);

        String[] rowKeyColumns = logicalTable != null ? logicalTable.getRowKeyColumns() : null;

        DatabaseAndTable databaseAndTable = distributeDispatcher.resolveDatabaseAndTable(tableName,
            rowKeyColumns, rowKey, snapshot);

        if (logicalTable == null && rowKeyColumns == null) {
            rowKeyColumns = tableRowKeyElement.get(tableName);
        }

        databaseAndTable.setReadOnly(readOnly);
        databaseAndTable.setRowKeyColumns(rowKeyColumns);
        return databaseAndTable;
    }

    private DatabaseAndTable calculateDatabaseAndTable(String tableName, ObTableOperation operation) {
        return calculateDatabaseAndTable(tableName, operation, null);
    }

    private DatabaseAndTable calculateDatabaseAndTable(String tableName, ObTableOperation operation,
                                                       VersionedConfigSnapshot snapshot) {

        LogicalTable logicalTable = distributeDispatcher.findLogicalTable(tableName, snapshot);

        String[] rowKeyColumns = logicalTable != null ? logicalTable.getRowKeyColumns() : null;

        ObRowKey rowKeyObject = operation.getEntity().getRowKey();
        int rowKeySize = rowKeyObject.getObjs().size();
        Object[] rowKey = new Object[rowKeySize];
        for (int j = 0; j < rowKeySize; j++) {
            rowKey[j] = rowKeyObject.getObj(j).getValue();
        }

        DatabaseAndTable databaseAndTable = distributeDispatcher.resolveDatabaseAndTable(tableName,
            rowKeyColumns, rowKey, snapshot);

        databaseAndTable.setReadOnly(operation.isReadonly());
        databaseAndTable.setRowKeyColumns(rowKeyColumns);
        return databaseAndTable;
    }

    /**
     *
     * @param tableName
     * @param operations
     * @return
     */
    public DatabaseAndTable calculateDatabaseAndTable(String tableName,
                                                      List<ObTableOperation> operations) {
        return calculateDatabaseAndTable(tableName, operations, null);
    }

    public DatabaseAndTable calculateDatabaseAndTable(String tableName,
                                                      List<ObTableOperation> operations,
                                                      VersionedConfigSnapshot snapshot) {

        DatabaseAndTable databaseAndTable = null;

        for (ObTableOperation operation : operations) {
            DatabaseAndTable currentDatabaseAndTable = calculateDatabaseAndTable(tableName,
                operation, snapshot);
            currentDatabaseAndTable.setReadOnly(operation.isReadonly());
            if (databaseAndTable == null) {
                databaseAndTable = currentDatabaseAndTable;
            } else if (!databaseAndTable.isInSameShard(currentDatabaseAndTable)) {
                logger.error(LCD.convert("02-00018"), tableName, currentDatabaseAndTable, databaseAndTable);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table ["
                            + tableName
                            + "] 02-00018: Distribute Operation is not support yet. Different key has different route result, current route result ["
                            + currentDatabaseAndTable.getTableShardValue() +
                            "] and other route result [" + databaseAndTable.getTableShardValue() + "]");
            }

            databaseAndTable.setReadOnly(databaseAndTable.isReadOnly()
                                         && currentDatabaseAndTable.isReadOnly());
        }

        return databaseAndTable;
    }

    /**
     *
     * @param tableName
     * @param tableQuery
     * @return
     */
    public DatabaseAndTable calculateDatabaseAndTable(String tableName, ObTableQuery tableQuery) {
        return calculateDatabaseAndTable(tableName, tableQuery, null);
    }

    public DatabaseAndTable calculateDatabaseAndTable(String tableName, ObTableQuery tableQuery,
                                                      VersionedConfigSnapshot snapshot) {
        LogicalTable logicalTable = distributeDispatcher.findLogicalTable(tableName, snapshot);

        String[] rowKeyColumns = logicalTable != null ? logicalTable.getRowKeyColumns() : null;

        DatabaseAndTable databaseAndTable = null;
        Object[] start = null;
        Object[] end = null;

        if (tableQuery.getKeyRanges().isEmpty()) {
            logger.error(LCD.convert("02-00015"), tableName);
            throw new DistributeDispatchException(
                "OBKV-ROUTER Logical table [" + tableName
                        + "] 02-00015: Table Query found empty key range.");
        }

        for (ObNewRange rang : tableQuery.getKeyRanges()) {
            ObRowKey currentStartKey = rang.getStartKey();
            int currentStartKeySize = currentStartKey.getObjs().size();
            ObRowKey currentEndKey = rang.getEndKey();
            int currentEndKeySize = currentEndKey.getObjs().size();
            Object[] currentStart = new Object[currentStartKeySize];
            Object[] currentEnd = new Object[currentEndKeySize];
            for (int i = 0; i < currentStartKeySize; i++) {
                currentStart[i] = currentStartKey.getObj(i).getValue();
            }

            for (int i = 0; i < currentEndKeySize; i++) {
                currentEnd[i] = currentEndKey.getObj(i).getValue();
            }
            ObBorderFlag borderFlag = rang.getBorderFlag();

            List<DatabaseAndTable> currentRangeDatabaseAndTables = distributeDispatcher
                .resolveDatabaseAndTable(tableName, rowKeyColumns, currentStart,
                    borderFlag.isInclusiveStart(), currentEnd, borderFlag.isInclusiveEnd(), snapshot);

            //TODO Distribute Query is not support yet so that we restrict queries to one shard.

            if (currentRangeDatabaseAndTables.size() > 1) {
                logger.error(LCD.convert("02-00016"), tableName, currentStart, currentEnd,
                    currentRangeDatabaseAndTables);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table [" + tableName
                            + "] 02-00016: Distribute Query is not support yet . The range is ["
                            + Arrays.toString(currentStart) + "] ~ [" + Arrays.toString(currentEnd)
                            + "] and route result [" + currentRangeDatabaseAndTables + "]");
            }

            DatabaseAndTable currentDatabaseAndTable = currentRangeDatabaseAndTables.get(0);

            currentDatabaseAndTable.setReadOnly(true);
            currentDatabaseAndTable.setRowKeyColumns(rowKeyColumns);

            if (databaseAndTable == null) {
                databaseAndTable = currentDatabaseAndTable;
                start = currentStart;
                end = currentEnd;
            } else if (!databaseAndTable.equals(currentDatabaseAndTable)) {
                logger.error(LCD.convert("02-00017"), tableName, start, end, databaseAndTable,
                    currentStart, currentEnd, currentDatabaseAndTable);
                throw new DistributeDispatchException(
                    "OBKV-ROUTER Logical table ["
                            + tableName
                            + "] 02-00017: Distribute Query is not support yet . Different range has different route result. The range ["
                            + Arrays.toString(start) + "] ~ [" + Arrays.toString(end)
                            + "] found route result [" + databaseAndTable + "] and other range ["
                            + Arrays.toString(currentStart) + "] ~ [" + Arrays.toString(currentEnd)
                            + "] found route result [" + currentDatabaseAndTable + "]");
            }
        }

        return databaseAndTable;
    }

    /**
     *
     * @param tableNameString
     * @return
     */
    public String getTargetTableName(String tableNameString) {
        checkArgument(tableNameString != null, "tableNameString is null");
        if (LoadTestUtil.isLoadTestMode()) {
            if (this.runningMode == ObTableClient.RunningMode.HBASE) {
                return getHbaseTestLoadTargetTableName(tableNameString);
            } else {
                return getTestLoadTargetTableName(tableNameString);
            }
        }
        return tableNameString;
    }

    private String getTestLoadTargetTableName(String tableNameString) {
        String suffix = testLoadSuffix;
        if (suffix == null) {
            suffix = DEFAULT_TEST_LOAD_SUFFIX;
        }

        return tableNameString + suffix;
    }

    private String getHbaseTestLoadTargetTableName(String tableNameString) {
        String[] strs = tableNameString.split("\\$");

        String suffix = testLoadSuffix;
        if (suffix == null) {
            suffix = DEFAULT_TEST_LOAD_SUFFIX;
        }

        return strs[0] + suffix + DEFAULT_HBASE_FAMILY_SEP + strs[1];
    }

    private void checkArgument(boolean expression, Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    /**
     * 从Group配置中提取所需的dbkey集合
     */
    private Set<String> extractRequiredDbkeys(GroupClusterConfig groupClusterConfig) {
        Set<String> requiredDbkeys = new HashSet<>();
        
        for (GroupDataSourceConfig groupConfig : groupClusterConfig.getGroupCluster().values()) {
            GroupDataSourceWeight groupWeight = groupConfig.getGroupDataSourceWeight();
            if (groupWeight != null) {
                List<AtomDataSourceWeight> weights = groupWeight.getDataSourceReadWriteWeights();
                if (weights != null) {
                    for (AtomDataSourceWeight weight : weights) {
                        requiredDbkeys.add(weight.getDbkey());
                    }
                }
            }
        }
        
        return requiredDbkeys;
    }

    /**
     * 验证数据源是否准备好为Group提供服务
     * 确保Group配置中引用的所有dbkey都有对应的已初始化数据源
     */
    private void validateDataSourcesReadyForGroups(GroupClusterConfig groupClusterConfig,
                                                   Map<String, ObTableClient> atomDataSources) {
        Set<String> requiredDbkeys = extractRequiredDbkeys(groupClusterConfig);
        Set<String> availableDbkeys = atomDataSources.keySet();
        
        if (!availableDbkeys.containsAll(requiredDbkeys)) {
            Set<String> missingDbkeys = new HashSet<>(requiredDbkeys);
            missingDbkeys.removeAll(availableDbkeys);
            
            BOOT.error("CRITICAL: Some required data sources are missing. " +
                      "Missing: {}, Available: {}", missingDbkeys, availableDbkeys);
            throw new IllegalStateException("Required data sources not found: " + missingDbkeys);
        } else {
            BOOT.info("All required data sources are ready for Group initialization: {}", requiredDbkeys);
        }
    }

    /**
     *
     * @return
     */
    public String getAppName() {
        return appName;
    }

    /**
     *
     * @param appName
     */
    public void setAppName(String appName) {
        this.appName = appName;
    }

    /**
     *
     * @param timeoutMillis
     */
    public void setConfigFetchOnceTimeoutMillis(Long timeoutMillis) {
        this.configFetchOnceTimeoutMillis = timeoutMillis;
    }

    /**
     *
     * @return
     */
    public String getAppDsName() {
        return appDsName;
    }

    /**
     *
     * @param appDsName
     */
    public void setAppDsName(String appDsName) {
        this.appDsName = appDsName;
    }

    /**
     *
     * @return
     */
    public String getVersion() {
        return version;
    }

    /**
     *
     * @param version
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /**
     *
     * @return
     */
    public ObTableClient.RunningMode getRunningMode() {
        return runningMode;
    }

    /**
     *
     * @param runningMode
     */
    public void setRunningMode(ObTableClient.RunningMode runningMode) {
        this.runningMode = runningMode;
    }

    /**
     * Get OB server version from any available underlying ObTableClient
     * @return obVersion
     */
    public long getObVersion() {
        VersionedConfigSnapshot snapshot = currentConfigRef.get();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        
        // Try to get obVersion from groupDataSources first (prefer read-write client)
        Map<Integer, ObTableClientGroup> groupDataSources = snapshot.getGroupDataSources();
        if (groupDataSources != null && !groupDataSources.isEmpty()) {
            for (ObTableClientGroup group : groupDataSources.values()) {
                if (group != null) {
                    // Select a read-write client (readOnly=false)
                    ObTableClient client = group.select(false, 0);
                    if (client != null) {
                        return client.getObVersion();
                    }
                }
            }
        }
        
        // If no groupDataSources, try atomDataSources
        Map<String, ObTableClient> atomDataSources = snapshot.getAtomDataSources();
        if (atomDataSources != null && !atomDataSources.isEmpty()) {
            for (ObTableClient client : atomDataSources.values()) {
                if (client != null) {
                    return client.getObVersion();
                }
            }
        }
        
        // If no clients available, return 0 (will be handled by caller)
        return 0;
    }

    public Properties getTableClientProperty() {
        return tableClientProperty;
    }

    public void setTableClientProperty(Properties tableClientProperty) {
        this.tableClientProperty = tableClientProperty;
    }

    @Override
    public Update update(String tableName) {
        checkStatus();
        return new DdsUpdate(this, tableName);
    }

    @Override
    public Delete delete(String tableName) {
        checkStatus();
        return new DdsDelete(this, tableName);
    }

    @Override
    public Insert insert(String tableName) {
        checkStatus();
        return new DdsInsert(this, tableName);
    }

    @Override
    public Replace replace(String tableName) {
        checkStatus();
        return new DdsReplace(this, tableName);
    }

    @Override
    public InsertOrUpdate insertOrUpdate(String tableName) {
        checkStatus();
        return new DdsInsertOrUpdate(this, tableName);
    }

    @Override
    public Put put(String tableName) {
        checkStatus();
        return new DdsPut(this, tableName);
    }

    @Override
    public Increment increment(String tableName) {
        checkStatus();
        return new DdsIncrement(this, tableName);
    }

    @Override
    public Append append(String tableName) {
        checkStatus();
        return new DdsAppend(this, tableName);
    }

    @Override
    public BatchOperation batchOperation(String tableName) {
        return new DdsBatchOperation(this, tableName);
    }

    @Override
    public CheckAndInsUp checkAndInsUp(String tableName, ObTableFilter filter, InsertOrUpdate insUp,
            boolean checkExists) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndInsUp'");
    }

    @Override
    public CheckAndInsUp checkAndInsUp(String tableName, ObTableFilter filter, InsertOrUpdate insUp,
            boolean checkExists, boolean rollbackWhenCheckFailed) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndInsUp'");
    }

    public ReentrantLock getStatusLock() {
        return statusLock;
    }

    public void setStatusLock(ReentrantLock statusLock) {
        this.statusLock = statusLock;
    }

    /**
     * Get current configuration snapshot for internal use
     * @return current configuration snapshot
     */
    VersionedConfigSnapshot getCurrentConfigSnapshot() {
        return currentConfigRef.get();
    }

    /**
     * DDS version of Update that handles distributed dispatch
     */
    private static class DdsUpdate extends Update {
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsUpdate(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        @Override
        public MutationResult execute() throws Exception {
            if (null == getRowKey()) {
                throw new ObTableException("rowKey is null, please set rowKey before execute");
            }
            

            VersionedConfigSnapshot snapshot = ddsClient.getCurrentConfigSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("Configuration snapshot is not available");
            }
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false, snapshot);
            ObTableClient obTableClient = ddsClient.getObTableWithSnapshot(databaseAndTable, snapshot);
            
            // Create a new Update for the target ObTableClient
            Update targetUpdate = new Update(obTableClient, ddsClient.getTargetTableName(databaseAndTable.getTableName()));
            
            // Copy all settings from this DdsUpdate to the target Update
            targetUpdate.setRowKey(getRowKey());
            
            // Copy mutate columns and values
            String[] columns = getColumns();
            Object[] values = getValues();
            if (columns != null && values != null) {
                for (int i = 0; i < columns.length; i++) {
                    targetUpdate.addMutateColVal(MutationFactory.colVal(columns[i], values[i]));
                }
            }
            
            // Execute on the target client
            return targetUpdate.execute();
        }
    }

    /**
     * DDS version of Delete that handles distributed dispatch
     */
    private static class DdsDelete extends Delete {
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsDelete(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        @Override
        public MutationResult execute() throws Exception {
            if (null == getRowKey()) {
                throw new ObTableException("rowKey is null, please set rowKey before execute");
            }
            

            VersionedConfigSnapshot snapshot = ddsClient.getCurrentConfigSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("Configuration snapshot is not available");
            }
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, true, snapshot);
            ObTableClient obTableClient = ddsClient.getObTableWithSnapshot(databaseAndTable, snapshot);
            
            // Create a new Delete for the target ObTableClient
            Delete targetDelete = new Delete(obTableClient, ddsClient.getTargetTableName(databaseAndTable.getTableName()));
            
            // Copy all settings from this DdsDelete to the target Delete
            targetDelete.setRowKey(getRowKey());
            
            // Execute on the target client
            return targetDelete.execute();
        }
    }

    /**
     * DDS version of Insert that handles distributed dispatch
     */
    private static class DdsInsert extends Insert {
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsInsert(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        @Override
        public MutationResult execute() throws Exception {
            if (null == getRowKey()) {
                throw new ObTableException("rowKey is null, please set rowKey before execute");
            }
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            

            VersionedConfigSnapshot snapshot = ddsClient.getCurrentConfigSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("Configuration snapshot is not available");
            }
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false, snapshot);
            ObTableClient obTableClient = ddsClient.getObTableWithSnapshot(databaseAndTable, snapshot);
            
            // Create a new Insert for the target ObTableClient
            Insert targetInsert = new Insert(obTableClient, ddsClient.getTargetTableName(databaseAndTable.getTableName()));
            
            // Copy all settings from this DdsInsert to the target Insert
            targetInsert.setRowKey(getRowKey());
            
            // Copy mutate columns and values
            String[] columns = getColumns();
            Object[] values = getValues();
            if (columns != null && values != null) {
                for (int i = 0; i < columns.length; i++) {
                    targetInsert.addMutateColVal(MutationFactory.colVal(columns[i], values[i]));
                }
            }
            
            // Execute on the target client
            return targetInsert.execute();
        }
    }

    /**
     * DDS version of Replace that handles distributed dispatch
     */
    private static class DdsReplace extends Replace {
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsReplace(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        @Override
        public MutationResult execute() throws Exception {
            if (null == getRowKey()) {
                throw new ObTableException("rowKey is null, please set rowKey before execute");
            }
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            

            VersionedConfigSnapshot snapshot = ddsClient.getCurrentConfigSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("Configuration snapshot is not available");
            }
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false, snapshot);
            ObTableClient obTableClient = ddsClient.getObTableWithSnapshot(databaseAndTable, snapshot);
            
            // Create a new Replace for the target ObTableClient
            Replace targetReplace = new Replace(obTableClient, ddsClient.getTargetTableName(databaseAndTable.getTableName()));
            
            // Copy all settings from this DdsReplace to the target Replace
            targetReplace.setRowKey(getRowKey());
            
            // Copy mutate columns and values
            String[] columns = getColumns();
            Object[] values = getValues();
            if (columns != null && values != null) {
                for (int i = 0; i < columns.length; i++) {
                    targetReplace.addMutateColVal(MutationFactory.colVal(columns[i], values[i]));
                }
            }
            
            // Execute on the target client
            return targetReplace.execute();
        }
    }

    /**
     * DDS version of InsertOrUpdate that handles distributed dispatch
     */
    private static class DdsInsertOrUpdate extends InsertOrUpdate {
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsInsertOrUpdate(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        @Override
        public MutationResult execute() throws Exception {
            if (null == getRowKey()) {
                throw new ObTableException("rowKey is null, please set rowKey before execute");
            }
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            

            VersionedConfigSnapshot snapshot = ddsClient.getCurrentConfigSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("Configuration snapshot is not available");
            }
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false, snapshot);
            ObTableClient obTableClient = ddsClient.getObTableWithSnapshot(databaseAndTable, snapshot);
            
            // Create a new InsertOrUpdate for the target ObTableClient
            InsertOrUpdate targetInsertOrUpdate = new InsertOrUpdate(obTableClient, ddsClient.getTargetTableName(databaseAndTable.getTableName()));
            
            // Copy all settings from this DdsInsertOrUpdate to the target InsertOrUpdate
            targetInsertOrUpdate.setRowKey(getRowKey());
            
            // Copy mutate columns and values
            String[] columns = getColumns();
            Object[] values = getValues();
            if (columns != null && values != null) {
                for (int i = 0; i < columns.length; i++) {
                    targetInsertOrUpdate.addMutateColVal(MutationFactory.colVal(columns[i], values[i]));
                }
            }
            
            // Execute on the target client
            return targetInsertOrUpdate.execute();
        }
    }

    /**
     * DDS version of Put that handles distributed dispatch
     */
    private static class DdsPut extends Put {
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsPut(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        @Override
        public MutationResult execute() throws Exception {
            if (null == getRowKey()) {
                throw new ObTableException("rowKey is null, please set rowKey before execute");
            }
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            

            VersionedConfigSnapshot snapshot = ddsClient.getCurrentConfigSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("Configuration snapshot is not available");
            }
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false, snapshot);
            ObTableClient obTableClient = ddsClient.getObTableWithSnapshot(databaseAndTable, snapshot);
            
            // Create a new Put for the target ObTableClient
            Put targetPut = new Put(obTableClient, ddsClient.getTargetTableName(databaseAndTable.getTableName()));
            
            // Copy all settings from this DdsPut to the target Put
            targetPut.setRowKey(getRowKey());
            
            // Copy mutate columns and values
            String[] columns = getColumns();
            Object[] values = getValues();
            if (columns != null && values != null) {
                for (int i = 0; i < columns.length; i++) {
                    targetPut.addMutateColVal(MutationFactory.colVal(columns[i], values[i]));
                }
            }
            
            // Execute on the target client
            return targetPut.execute();
        }
    }

    /**
     * DDS version of Increment that handles distributed dispatch
     */
    private static class DdsIncrement extends Increment {
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsIncrement(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        @Override
        public MutationResult execute() throws Exception {
            if (null == getRowKey()) {
                throw new ObTableException("rowKey is null, please set rowKey before execute");
            }
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            

            VersionedConfigSnapshot snapshot = ddsClient.getCurrentConfigSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("Configuration snapshot is not available");
            }
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false, snapshot);
            ObTableClient obTableClient = ddsClient.getObTableWithSnapshot(databaseAndTable, snapshot);
            
            // Create a new Increment for the target ObTableClient
            Increment targetIncrement = new Increment(obTableClient, ddsClient.getTargetTableName(databaseAndTable.getTableName()));
            
            // Copy all settings from this DdsIncrement to the target Increment
            targetIncrement.setRowKey(getRowKey());
            
            // Copy mutate columns and values
            String[] columns = getColumns();
            Object[] values = getValues();
            if (columns != null && values != null) {
                for (int i = 0; i < columns.length; i++) {
                    targetIncrement.addMutateColVal(MutationFactory.colVal(columns[i], values[i]));
                }
            }
            
            // Execute on the target client
            return targetIncrement.execute();
        }
    }

    /**
     * DDS version of Append that handles distributed dispatch
     */
    private static class DdsAppend extends Append {
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsAppend(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        @Override
        public MutationResult execute() throws Exception {
            if (null == getRowKey()) {
                throw new ObTableException("rowKey is null, please set rowKey before execute");
            }
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            

            VersionedConfigSnapshot snapshot = ddsClient.getCurrentConfigSnapshot();
            if (snapshot == null) {
                throw new IllegalStateException("Configuration snapshot is not available");
            }
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false, snapshot);
            ObTableClient obTableClient = ddsClient.getObTableWithSnapshot(databaseAndTable, snapshot);
            
            // Create a new Append for the target ObTableClient
            Append targetAppend = new Append(obTableClient, ddsClient.getTargetTableName(databaseAndTable.getTableName()));
            
            // Copy all settings from this DdsAppend to the target Append
            targetAppend.setRowKey(getRowKey());
            
            // Copy mutate columns and values
            String[] columns = getColumns();
            Object[] values = getValues();
            if (columns != null && values != null) {
                for (int i = 0; i < columns.length; i++) {
                    targetAppend.addMutateColVal(MutationFactory.colVal(columns[i], values[i]));
                }
            }
            
            // Execute on the target client
            return targetAppend.execute();
        }
    }

    /**
     * DDS version of BatchOperation that handles distributed dispatch
     */
    private static class DdsBatchOperation extends BatchOperation {
        @SuppressWarnings("unused")
        private final DdsObTableClient ddsClient;
        @SuppressWarnings("unused")
        private final String tableName;

        public DdsBatchOperation(DdsObTableClient ddsClient, String tableName) {
            super(null, tableName);
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }

        /**
         * Override to handle DdsObTableClient
         */
        @Override
        protected long getObVersionFromClient() {
            return ddsClient.getObVersion();
        }
    }

}

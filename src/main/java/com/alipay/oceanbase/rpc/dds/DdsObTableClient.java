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

import com.alibaba.fastjson.JSON;
import com.alipay.common.tracer.util.LoadTestUtil;
import com.alipay.oceanbase.rpc.Lifecycle;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.OperationExecuteAble;
import com.alipay.oceanbase.rpc.exception.DistributeDispatchException;
import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.exception.ObTableCloseException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.groupupdate.ObTableGroupComparator;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObBorderFlag;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.dds.config.DistributeConfigHandler;
import com.alipay.oceanbase.rpc.dds.config.DistributeDynamicHandler;
import com.alipay.oceanbase.rpc.dds.group.ObTableClientGroup;
import com.alipay.oceanbase.rpc.dds.rule.DatabaseAndTable;
import com.alipay.oceanbase.rpc.dds.rule.DistributeDispatcher;
import com.alipay.oceanbase.rpc.dds.rule.LogicalTable;
import com.alipay.oceanbase.rpc.table.AbstractTable;
import com.alipay.oceanbase.rpc.table.ConcurrentTask;
import com.alipay.oceanbase.rpc.table.ConcurrentTaskExecutor;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.sofa.common.thread.SofaThreadPoolExecutor;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.group.AtomDataSourceWeight;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceWeight;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.rpc.property.Property.DDS_CONFIG_FATCH_TIMEOUT;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;

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
    private ObTableClient.RunningMode        runningMode;
    private long                             configFetchOnceTimeoutMillis = DDS_CONFIG_FATCH_TIMEOUT.getDefaultLong();

    private String                           testLoadSuffix;
    public static final String               DEFAULT_TEST_LOAD_SUFFIX     = "_t";
    public static final String               DEFAULT_HBASE_FAMILY_SEP     = "$";

    private Map<String, ObTableClient>       atomDataSources;
    private Map<Integer, ObTableClientGroup> groupDataSources;

    private DistributeDispatcher             distributeDispatcher;

    private volatile boolean                 initialized                  = false;
    private volatile boolean                 closed                       = false;
    private ReentrantLock                    statusLock                   = new ReentrantLock();

    private Map<String, String[]>            tableRowKeyElement = new ConcurrentHashMap<>();
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
            DistributeDynamicHandler dynamicHandler = new DistributeDynamicHandler();
            configFetchOnceTimeoutMillis = parseToLong(DDS_CONFIG_FATCH_TIMEOUT.getKey(), configFetchOnceTimeoutMillis);
            DistributeConfigHandler distributeConfigHandler = new DistributeConfigHandler(appName,
                appDsName, version, configFetchOnceTimeoutMillis, dynamicHandler);
            distributeConfigHandler.init();
            initDistributeRule(distributeConfigHandler);
            initDistributeDataSource(distributeConfigHandler.getGroupClusterConfig(),
                distributeConfigHandler.getExtendedDataSourceConfigs());
            initialized = true;
        } catch (Throwable t) {
            logger.error("DdsObTableClient init failed", t);
            throw new RuntimeException(t);
        } finally {
            statusLock.unlock();
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

            if (atomDataSources != null) {
                Exception throwException = null;
                List<String> exceptionClient = new ArrayList<String>();
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
                if (exceptionClient.size() > 0) {
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

    private void initDistributeRule(DistributeConfigHandler distributeConfigHandler) throws Exception {
        this.distributeDispatcher = new DistributeDispatcher(distributeConfigHandler);
        distributeDispatcher.init();
    }

    private void initDistributeDataSource(GroupClusterConfig groupClusterConfig,
                                          Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs)
                                                                                                          throws Exception {
        this.atomDataSources = initAtomDataSourceConcurrent(extendedDataSourceConfigs,
                this.runningMode, this.tableClientProperty);
        this.groupDataSources = initGroupDataSource(groupClusterConfig, atomDataSources);
    }

    private Map<Integer, ObTableClientGroup> initGroupDataSource(GroupClusterConfig groupClusterConfig,
                                                                Map<String, ObTableClient> atomDataSources)
                                                                                                            throws Exception {
        Map<Integer, ObTableClientGroup> groupDataSources = new ConcurrentHashMap<Integer, ObTableClientGroup>();

        for (GroupDataSourceConfig config : groupClusterConfig.getGroupCluster().values()) {
            String groupKey = config.getGroupKey();
            String weightString = config.getWeightString();
            GroupDataSourceWeight groupDataSourceWeight = config.getGroupDataSourceWeight();
            Map<String, ObTableClient> atomDataSourceInGroup = new HashMap<String, ObTableClient>();
            for (AtomDataSourceWeight weight : groupDataSourceWeight
                .getDataSourceReadWriteWeights()) {
                String dbkey = weight.getDbkey();
                ObTableClient client = atomDataSources.get(dbkey);
                atomDataSourceInGroup.put(dbkey, client);
            }
            ObTableClientGroup obTableClientGroup = new ObTableClientGroup(groupKey, weightString,
                groupDataSourceWeight, atomDataSourceInGroup);
            obTableClientGroup.init();
            groupDataSources.put(obTableClientGroup.getGroupIndex(), obTableClientGroup);
        }

        return groupDataSources;
    }

    private Map<String, ObTableClient> initAtomDataSourceConcurrent(Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs,
                                                          ObTableClient.RunningMode runningMode, Properties props)
            throws Exception {
        BOOT.info("begin initAtomDataSourceConcurrent {}", this.appDsName);
        Map<String, ObTableClient> atomDataSources = new ConcurrentHashMap<String, ObTableClient>();

        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ThreadPoolExecutor executorService = new SofaThreadPoolExecutor(extendedDataSourceConfigs.size(), extendedDataSourceConfigs.size(),
                0L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(), threadFactory);
        final ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(executorService,
                extendedDataSourceConfigs.size());

        String appName = this.appName;
        String appDsName = this.appDsName;
        final Map<Object, Object> context = ThreadLocalMap.getContextMap();
        for (Map.Entry<String, ExtendedDataSourceConfig> configEntry : extendedDataSourceConfigs
                .entrySet()) {
            executor.execute(new ConcurrentTask() {
                /**
                * Do task.
                */
                @Override
                public void doTask() {
                    try {
                        logger.info("initAtomDataSourceConcurrent configEntry {}", configEntry);
                        ThreadLocalMap.transmitContextMap(context);

                        String dbkey = configEntry.getKey();
                        ExtendedDataSourceConfig config = configEntry.getValue();
                        ObTableClient obTableClient = new ObTableClient();
                        logger.info("initAtomDataSourceConcurrent FullUserName {}, dbkey {}, password {}, jdbcUrl {}", config.getUsername(), dbkey, config.getPassword(), config.getJdbcUrl());
                        obTableClient.setFullUserName(config.getUsername());
                        obTableClient.setParamURL(config.getJdbcUrl());
                        obTableClient.setPassword(config.getPassword());
                        obTableClient.setSysUserName("proxyro");
                        obTableClient.setSysPassword("3u^0kCdpE");
                        obTableClient.setAppName(appName);
                        obTableClient.setAppDataSourceName(appDsName);
                        obTableClient.setProperties(props);

                        if (runningMode != null) {
                            obTableClient.setRunningMode(runningMode);
                        }

                        obTableClient.init();
                        atomDataSources.put(dbkey, obTableClient);
                    } catch (Exception e) {
                        BOOT.error("initAtomDataSourceConcurrent meet Exception", e);
                        executor.collectExceptions(e);
                    } finally {
                        ThreadLocalMap.reset();
                    }
                }
            });
        }

        long estimate = 3000000L  * 1000L * 1000L;
        try {
            while (estimate > 0) {
                long nanos = System.nanoTime();
                try {
                    executor.waitComplete(1, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    throw new ObTableUnexpectedException(
                            "initAtomDataSourceConcurrent Execute interrupted", e);
                }

                if (executor.getThrowableList().size() > 0) {
                    throw new ObTableUnexpectedException("initAtomDataSourceConcurrent Execute Error",
                            executor.getThrowableList().get(0));
                }

                if (executor.isComplete()) {
                    break;
                }

                estimate = estimate - (System.nanoTime() - nanos);
            }
        } finally {
            executor.stop();
        }

        if (executor.getThrowableList().size() > 0) {
            throw new ObTableUnexpectedException("initAtomDataSourceConcurrent Execute Error", executor
                    .getThrowableList().get(0));
        }

        if (!executor.isComplete()) {
            throw new ObTableUnexpectedException("initAtomDataSourceConcurrent Execute timeout for 50min");
        }

        BOOT.info("finish initAtomDataSourceConcurrent {}", this.appDsName);
        return atomDataSources;
    }

    private Map<String, ObTableClient> initAtomDataSource(Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs,
                                                          ObTableClient.RunningMode runningMode)
                                                                                                throws Exception {
        getLogger().info("begin initAtomDataSource {}", this.appDsName);
        Map<String, ObTableClient> atomDataSources = new ConcurrentHashMap<String, ObTableClient>();
        for (Map.Entry<String, ExtendedDataSourceConfig> configEntry : extendedDataSourceConfigs
            .entrySet()) {

            String dbkey = configEntry.getKey();
            ExtendedDataSourceConfig config = configEntry.getValue();
            ObTableClient obTableClient = new ObTableClient();
            obTableClient.setFullUserName(config.getUsername());
            obTableClient.setParamURL(config.getJdbcUrl());
            obTableClient.setPassword(config.getPassword());
            obTableClient.setAppName(this.appName);

            if (runningMode != null) {
                obTableClient.setRunningMode(runningMode);
            }

            obTableClient.init();
            atomDataSources.put(dbkey, obTableClient);
        }

        getLogger().info("finish initAtomDataSource {}", this.appDsName);
        return atomDataSources;
    }

    @Override
    public TableEntry getOrRefreshTableEntry(final String tableName, final Integer groupID, final boolean refresh,
                                            final boolean waitForRefresh) throws Exception {
        getLogger().info("DDS getOrRefreshTableEntry for {}, atomDataSources size {}, keys {}", tableName, atomDataSources.size(), JSON.toJSONString(atomDataSources.keySet()));
        ObTableClientGroup obTableClientGroup = groupDataSources.get(groupID);
        ObTableClient obTableClient = obTableClientGroup.select(true, 0);

        try {
            obTableClient.getOrRefreshTableEntry(tableName, refresh, waitForRefresh);
        } catch (Throwable t) {
            logger.error("getOrRefreshTableEntry execption", t);
        }

        return null;
    }

    @Override
    public TableEntry getOrRefreshTableEntry(final String tableName, final boolean refresh,
                                            final boolean waitForRefresh) throws Exception {
        getLogger().info("DDS getOrRefreshTableEntry for {}, atomDataSources size {}, keys {}", tableName, atomDataSources.size(), JSON.toJSONString(atomDataSources.keySet()));
        for (Map.Entry<String, ObTableClient> entry: atomDataSources.entrySet()) {
            try {
                entry.getValue().getOrRefreshTableEntry(tableName, refresh, waitForRefresh);
            } catch (Throwable t) {
                logger.error("getOrRefreshTableEntry execption", t);
            }
        }

        return null;
    }


    public void setRuntimeBatchExecutor(ExecutorService runtimeBatchExecutor) {
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

    private DatabaseAndTable getDatabaseAndTable(String tableName, Object[] rowkeys, boolean readOnly){
        checkStatus();
        return calculateDatabaseAndTable(tableName, rowkeys, readOnly);
    }

    private ObTableClient buildObTableClient(DatabaseAndTable databaseAndTable){
        ObTableClientGroup obTableClientGroup = groupDataSources.get(databaseAndTable
                .getDatabaseShardValue());

        ObTableClient obTableClient = obTableClientGroup.select(!databaseAndTable.isReadOnly(),
                databaseAndTable.getElasticIndexValue());

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
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, true);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable);

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
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable);

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
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, true);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable);

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
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable);

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

        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable);

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

        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable);

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
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable);

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
        DatabaseAndTable databaseAndTable = getDatabaseAndTable(tableName, rowkeys, false);
        ObTableClient obTableClient = buildObTableClient(databaseAndTable);

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

        String tableName = request.getTableName();

        DatabaseAndTable databaseAndTable = null;
        if (request instanceof ObTableOperationRequest) {
            ObTableOperation operation = ((ObTableOperationRequest) request).getTableOperation();

            databaseAndTable = calculateDatabaseAndTable(tableName, operation);

        } else if (request instanceof ObTableQueryRequest) {
            ObTableQuery tableQuery = ((ObTableQueryRequest) request).getTableQuery();
            databaseAndTable = calculateDatabaseAndTable(tableName, tableQuery);

        } else if (request instanceof ObTableBatchOperationRequest) {
            List<ObTableOperation> operations = ((ObTableBatchOperationRequest) request)
                .getBatchOperation().getTableOperations();

            databaseAndTable = calculateDatabaseAndTable(tableName, operations);
        } else if (request instanceof ObTableQueryAndMutateRequest) {
            ObTableQuery tableQuery = ((ObTableQueryAndMutateRequest) request)
                .getTableQueryAndMutate().getTableQuery();
            databaseAndTable = calculateDatabaseAndTable(tableName, tableQuery);
            databaseAndTable.setReadOnly(false);
        } else {
            throw new FeatureNotSupportedException(
                "request type " + request.getClass().getSimpleName()
                        + "is not supported. make sure the correct version");
        }

        ObTableClientGroup obTableClientGroup = groupDataSources.get(databaseAndTable
            .getDatabaseShardValue());

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

        ObTableClientGroup obTableClientGroup = groupDataSources.get(databaseAndTable
            .getDatabaseShardValue());

        ObTableClient obTableClient = obTableClientGroup.select(!databaseAndTable.isReadOnly(),
            databaseAndTable.getElasticIndexValue());

        if (databaseAndTable.getRowKeyColumns() != null) {
            obTableClient.addRowKeyElement(databaseAndTable.getTableName(),
                databaseAndTable.getRowKeyColumns());
        }

        return obTableClient;
    }

    private DatabaseAndTable calculateDatabaseAndTable(String tableName, Object[] rowKey,
                                                       boolean readOnly) {

        LogicalTable logicalTable = distributeDispatcher.findLogicalTable(tableName);

        String[] rowKeyColumns = logicalTable != null ? logicalTable.getRowKeyColumns() : null;

        DatabaseAndTable databaseAndTable = distributeDispatcher.resolveDatabaseAndTable(tableName,
            rowKeyColumns, rowKey);

        if (logicalTable == null && rowKeyColumns == null) {
            rowKeyColumns = tableRowKeyElement.get(tableName);
        }

        databaseAndTable.setReadOnly(readOnly);
        databaseAndTable.setRowKeyColumns(rowKeyColumns);
        return databaseAndTable;
    }

    private DatabaseAndTable calculateDatabaseAndTable(String tableName, ObTableOperation operation) {

        LogicalTable logicalTable = distributeDispatcher.findLogicalTable(tableName);

        String[] rowKeyColumns = logicalTable != null ? logicalTable.getRowKeyColumns() : null;

        ObRowKey rowKeyObject = operation.getEntity().getRowKey();
        int rowKeySize = rowKeyObject.getObjs().size();
        Object[] rowKey = new Object[rowKeySize];
        for (int j = 0; j < rowKeySize; j++) {
            rowKey[j] = rowKeyObject.getObj(j).getValue();
        }

        DatabaseAndTable databaseAndTable = distributeDispatcher.resolveDatabaseAndTable(tableName,
            rowKeyColumns, rowKey);

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

        DatabaseAndTable databaseAndTable = null;

        for (ObTableOperation operation : operations) {
            DatabaseAndTable currentDatabaseAndTable = calculateDatabaseAndTable(tableName,
                operation);
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
        LogicalTable logicalTable = distributeDispatcher.findLogicalTable(tableName);

        String[] rowKeyColumns = logicalTable != null ? logicalTable.getRowKeyColumns() : null;

        DatabaseAndTable databaseAndTable = null;
        Object[] start = null;
        Object[] end = null;

        if (tableQuery.getKeyRanges().size() == 0) {
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
                    borderFlag.isInclusiveStart(), currentEnd, borderFlag.isInclusiveEnd());

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
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false);
            ObTableClient obTableClient = ddsClient.buildObTableClient(databaseAndTable);
            
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
            
            // Convert rowKey to Object array for DDS dispatch
            Object[] rowkeys = getRowKey().getValues();
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, true);
            ObTableClient obTableClient = ddsClient.buildObTableClient(databaseAndTable);
            
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
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false);
            ObTableClient obTableClient = ddsClient.buildObTableClient(databaseAndTable);
            
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
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false);
            ObTableClient obTableClient = ddsClient.buildObTableClient(databaseAndTable);
            
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
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false);
            ObTableClient obTableClient = ddsClient.buildObTableClient(databaseAndTable);
            
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
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false);
            ObTableClient obTableClient = ddsClient.buildObTableClient(databaseAndTable);
            
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
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false);
            ObTableClient obTableClient = ddsClient.buildObTableClient(databaseAndTable);
            
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
            
            // Get target database and table through DDS dispatcher
            DatabaseAndTable databaseAndTable = ddsClient.getDatabaseAndTable(tableName, rowkeys, false);
            ObTableClient obTableClient = ddsClient.buildObTableClient(databaseAndTable);
            
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
        private final DdsObTableClient ddsClient;
        private final String tableName;

        public DdsBatchOperation(DdsObTableClient ddsClient, String tableName) {
            super();
            this.ddsClient = ddsClient;
            this.tableName = tableName;
        }
    }

}

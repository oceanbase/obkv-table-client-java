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

import com.alibaba.fastjson.JSON;
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
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.rpc.constant.Constants.*;
import static com.alipay.oceanbase.rpc.location.LocationUtil.*;
import static com.alipay.oceanbase.rpc.location.model.ObServerRoute.STRONG_READ;
import static com.alipay.oceanbase.rpc.location.model.TableEntry.HBASE_ROW_KEY_ELEMENT;
import static com.alipay.oceanbase.rpc.location.model.partition.ObPartIdCalculator.*;
import static com.alipay.oceanbase.rpc.property.Property.*;
import static com.alipay.oceanbase.rpc.protocol.payload.ResultCodes.OB_ERR_KV_ROUTE_ENTRY_EXPIRE;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType.*;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;

public class ObTableClient extends AbstractObTableClient implements Lifecycle {
    private static final Logger                               logger                                  = getLogger(ObTableClient.class);

    private static final String                               usernameSeparators                      = ":;-;.";

    private AtomicInteger                                     tableEntryRefreshContinuousFailureCount = new AtomicInteger(
                                                                                                          0);
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

    private volatile OcpModel                                 ocpModel                                = new OcpModel();

    /*
     * ServerAddr(all) -> ObTableConnection
     */
    private volatile ConcurrentHashMap<ObServerAddr, ObTable> tableRoster                             = null;

    /*
     * current tenant server address order by priority desc
     * <p>
     * be careful about concurrency when change the element
     */
    private final ServerRoster                                serverRoster                            = new ServerRoster();

    private volatile RunningMode                              runningMode                             = RunningMode.NORMAL;

    /*
     * TableName -> TableEntry
     */
    private Map<String, TableEntry>                           tableLocations                          = new ConcurrentHashMap<String, TableEntry>();

    /*
     * TableName -> ODPTableEntry
     */
    private Map<String, TableEntry>                           ODPTableLocations                       = new ConcurrentHashMap<String, TableEntry>();

    /*
     * TableName -> ObIndexinfo
     */
    private Map<String, ObIndexInfo>                          indexinfos                              = new ConcurrentHashMap<String, ObIndexInfo>();

    private ConcurrentHashMap<String, Lock>                   refreshIndexInfoLocks                   = new ConcurrentHashMap<String, Lock>();

    /*
     * TableName -> rowKey element
     */
    private Map<String, Map<String, Integer>>                 tableRowKeyElement                      = new ConcurrentHashMap<String, Map<String, Integer>>();
    private boolean                                           retryOnChangeMasterTimes                = true;
    /*
     * TableName -> Failures/Lock
     */
    private ConcurrentHashMap<String, AtomicLong>             tableContinuousFailures                 = new ConcurrentHashMap<String, AtomicLong>();

    private ConcurrentHashMap<String, Lock>                   refreshTableLocks                       = new ConcurrentHashMap<String, Lock>();

    private ConcurrentHashMap<String, Lock>                   fetchODPPartitionLocks                  = new ConcurrentHashMap<String, Lock>();

    private Lock                                              refreshMetadataLock                     = new ReentrantLock();

    private volatile long                                     lastRefreshMetadataTimestamp;

    private volatile boolean                                  initialized                             = false;
    private volatile boolean                                  closed                                  = false;
    private ReentrantLock                                     statusLock                              = new ReentrantLock();

    private String                                            currentIDC;
    private ObReadConsistency                                 readConsistency                         = ObReadConsistency.STRONG;
    private ObRoutePolicy                                     obRoutePolicy                           = ObRoutePolicy.IDC_ORDER;

    private boolean                                           odpMode                                 = false;

    private String                                            odpAddr                                 = "127.0.0.1";

    private int                                               odpPort                                 = 2883;

    private ObTable                                           odpTable                                = null;
    // tableGroup <-> Table
    private ConcurrentHashMap<String, Lock>                   TableGroupCacheLocks                    = new ConcurrentHashMap<String, Lock>();
    private ConcurrentHashMap<String, String>                 TableGroupCache                         = new ConcurrentHashMap<String, String>();              // tableGroup -> Table
    private ConcurrentHashMap<String, String>                 TableGroupInverted                      = new ConcurrentHashMap<String, String>();              // Table -> tableGroup

    private RouteTableRefresher routeTableRefresher;

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
            // 5. run fresh table task
            routeTableRefresher = new RouteTableRefresher(this);
            routeTableRefresher.start();
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
            if (routeTableRefresher != null) {
                routeTableRefresher.finish();
            }
            if (tableRoster != null) {
                Exception throwException = null;
                List<ObServerAddr> exceptionObServers = new ArrayList<ObServerAddr>();
                for (Map.Entry<ObServerAddr, ObTable> entry : tableRoster.entrySet()) {
                    try {
                        entry.getValue().close();
                    } catch (Exception e) {
                        // do not throw exception immediately
                        BOOT.error(LCD.convert("01-00004"), entry.getKey(), e);
                        RUNTIME.error(LCD.convert("01-00004"), entry.getKey(), e);
                        throwException = e;
                        exceptionObServers.add(entry.getKey());
                    }
                }
                if (exceptionObServers.size() > 0) {
                    StringBuilder sb = new StringBuilder();
                    sb.append("following ob servers [");
                    for (int i = 0; i < exceptionObServers.size(); i++) {
                        if (i != 0) {
                            sb.append(",");
                        }
                        sb.append(exceptionObServers.get(i));
                    }
                    sb.append("] close error.");
                    throw new ObTableCloseException(sb.toString(), throwException);
                }
            }
            if (odpTable != null) {
                odpTable.close();
            }
        } finally {
            BOOT.info("ObTableClient is closed");
            statusLock.unlock();
        }
    }

    public RouteTableRefresher getRouteTableRefresher() {
        return routeTableRefresher;
    }

    public Map<String, TableEntry> getTableLocations() {
        return tableLocations;
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

        if (odpMode) {
            try {
                odpTable = new ObTable.Builder(odpAddr, odpPort) //
                    .setLoginInfo(tenantName, fullUserName, password, database) //
                    .setProperties(getProperties()).setConfigs(TableConfigs).build();
            } catch (Exception e) {
                logger
                    .warn(
                        "The addr{}:{} failed to put into table roster, the node status may be wrong, Ignore",
                        odpAddr, odpPort);
                throw e;
            }
            return;
        }

        this.ocpModel = loadOcpModel(paramURL, dataSourceName, rsListAcquireConnectTimeout,
            rsListAcquireReadTimeout, rsListAcquireTryTimes, rsListAcquireRetryInterval);

        List<ObServerAddr> servers = new ArrayList<ObServerAddr>();
        ConcurrentHashMap<ObServerAddr, ObTable> tableRoster = new ConcurrentHashMap<ObServerAddr, ObTable>();

        TableEntryKey rootServerKey = new TableEntryKey(clusterName, tenantName,
            OCEANBASE_DATABASE, ALL_DUMMY_TABLE);

        List<ObServerAddr> rsList = ocpModel.getObServerAddrs();
        BOOT.info("{} success to get rsList, paramURL: {}, rsList: {}，idc2Region: {}",
            this.database, paramURL, JSON.toJSON(rsList), JSON.toJSON(ocpModel.getIdc2Region()));

        TableEntry tableEntry = loadTableEntryRandomly(rsList,//
            rootServerKey,//
            tableEntryAcquireConnectTimeout,//
            tableEntryAcquireSocketTimeout, sysUA, initialized);
        BOOT.info("{} success to get tableEntry with rootServerKey all_dummy_tables {}",
            this.database, JSON.toJSON(tableEntry));

        List<ReplicaLocation> replicaLocations = tableEntry.getTableLocation()
            .getReplicaLocations();
        BOOT.info("{} success to get replicaLocation {}", this.database,
            JSON.toJSON(replicaLocations));

        for (ReplicaLocation replicaLocation : replicaLocations) {
            ObServerInfo info = replicaLocation.getInfo();
            ObServerAddr addr = replicaLocation.getAddr();
            if (!info.isActive()) {
                BOOT.warn("will not init location {} because status is {}", addr.toString(),
                    info.getStatus());
                continue;
            }

            // 忽略初始化建连失败，否则client会初始化失败，导致应用无法启动的问题
            // 初始化建连失败(可能性较小)，如果后面这台server恢复，数据路由失败，就会重新刷新metadata
            // 在失败100次后(RUNTIME_CONTINUOUS_FAILURE_CEILING)，重新刷新建连
            // 本地cache 1小时超时后(SERVER_ADDRESS_CACHING_TIMEOUT)，重新刷新建连
            // 应急可以直接observer切主
            try {
                ObTable obTable = new ObTable.Builder(addr.getIp(), addr.getSvrPort()) //
                    .setLoginInfo(tenantName, userName, password, database) //
                    .setProperties(getProperties()).setConfigs(TableConfigs).build();
                tableRoster.put(addr, obTable);
                servers.add(addr);
            } catch (Exception e) {
                BOOT.warn(
                    "The addr{}:{} failed to put into table roster, the node status may be wrong, Ignore",
                    addr.getIp(), addr.getSvrPort());
                RUNTIME.warn("initMetadata meet exception", e);
                e.printStackTrace();
            }
        }
        if (servers.isEmpty()) {
            BOOT.error("{} failed to connect any replicaLocation server: {}", this.database,
                JSON.toJSON(replicaLocations));
            throw new Exception("failed to connect any replicaLocation server");
        }

        BOOT.info("{} success to build server connection {}", this.database, JSON.toJSON(servers));
        this.tableRoster = tableRoster;
        this.serverRoster.reset(servers);

        // Get Server LDC info for weak read consistency.
        if (StringUtil.isEmpty(currentIDC)) {
            currentIDC = ZoneUtil.getCurrentIDC();
        }
        String regionFromOcp = ocpModel.getIdc2Region(currentIDC);
        BOOT.info("{} success get currentIDC {}, regionFromOcp {}", this.database, currentIDC,
            regionFromOcp);

        List<ObServerLdcItem> ldcServers = getServerLdc(serverRoster,
            tableEntryAcquireConnectTimeout, tableEntryAcquireSocketTimeout,
            serverAddressPriorityTimeout, serverAddressCachingTimeout, sysUA);

        this.serverRoster.resetServerLdc(ObServerLdcLocation.buildLdcLocation(ldcServers,
            currentIDC, regionFromOcp));

        if (BOOT.isInfoEnabled()) {
            BOOT.info("{} finish refresh serverRoster: {}", this.database,
                JSON.toJSON(serverRoster));
            BOOT.info("finish initMetadata for all tables for database {}", this.database);
        }

        this.lastRefreshMetadataTimestamp = System.currentTimeMillis();
    }

    public boolean isOdpMode() {
        return odpMode;
    }

    public void setOdpMode(boolean odpMode) {
        this.odpMode = odpMode;
    }

    public ObTable getOdpTable() {
        return this.odpTable;
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

        abstract T execute(ObPair<Long, ObTableParam> obTable) throws Exception;

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
        boolean needRefreshTableEntry = false;
        boolean needRenew = false;
        boolean needFetchAllRouteInfo = false;
        int tryTimes = 0;
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
            ObPair<Long, ObTableParam> obPair = null;
            try {
                if (odpMode) {
                    obPair = getODPTableWithRowKeyValue(tableName, callback.getRowKey(), needRenew);
                } else {
                    obPair = getTable(tableName, callback.getRowKey(),
                        needRefreshTableEntry, tableEntryRefreshIntervalWait,
                        needFetchAllRouteInfo, route);
                }
                T t = callback.execute(obPair);
                resetExecuteContinuousFailureCount(tableName);
                return t;
            } catch (Exception ex) {
                RUNTIME.error("execute while meet exception", ex);
                if (odpMode) {
                    if ((tryTimes - 1) < runtimeRetryTimes) {
                        if (ex instanceof ObTableException) {
                            logger
                                .warn(
                                    "execute while meet Exception, errorCode: {} , errorMsg: {}, try times {}",
                                    ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                    tryTimes);
                            // if the cause is that ODP partition meta have expired, try to fetch new one
                            if (ex instanceof ObTablePartitionChangeException
                                && ((ObTablePartitionChangeException) ex).getErrorCode() == OB_ERR_KV_ROUTE_ENTRY_EXPIRE.errorCode) {
                                needRenew = true;
                            } else {
                                throw ex;
                            }
                        } else {
                            logger.warn("execute while meet Exception, errorMsg: {}, try times {}",
                                ex.getMessage(), tryTimes);
                            throw ex;
                        }
                    } else {
                        RUNTIME.error("retry failed with exception", ex);
                        throw ex;
                    }
                } else {
                    if (ex instanceof ObTableReplicaNotReadableException) {
                        if (obPair != null && (tryTimes - 1) < runtimeRetryTimes) {
                            logger.warn("retry when replica not readable: {}", ex.getMessage());
                            if (!odpMode) {
                                route.addToBlackList(obPair.getRight().getObTable().getIp());
                            }
                        } else {
                            logger.warn("exhaust retry when replica not readable: {}",
                                ex.getMessage());
                            RUNTIME.error("replica not readable", ex);
                            throw ex;
                        }
                    } else if (ex instanceof ObTableException
                               && ((ObTableException) ex).isNeedRefreshTableEntry()) {
                        needRefreshTableEntry = true;

                        logger
                            .warn(
                                "refresh table while meet Exception needing refresh, errorCode: {}, errorMsg: {}",
                                ((ObTableException) ex).getErrorCode(), ex.getMessage());
                        if (retryOnChangeMasterTimes && (tryTimes - 1) < runtimeRetryTimes) {
                            logger
                                .warn(
                                    "retry while meet Exception needing refresh, errorCode: {} , errorMsg: {},retry times {}",
                                    ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                    tryTimes);
                            if (ex instanceof ObTableNeedFetchAllException) {
                                needFetchAllRouteInfo = true;
                                getOrRefreshTableEntry(tableName, true, true, true);
                                // reset failure count while fetch all route info
                                this.resetExecuteContinuousFailureCount(tableName);
                            }
                        } else {
                            calculateContinuousFailure(tableName, ex.getMessage());
                            throw ex;
                        }
                    } else {
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
        private final List<ObNewRange> keyRanges;

        OperationExecuteCallback(Row rowKey, List<ObNewRange> keyRanges) {
            this.rowKey = rowKey;
            this.keyRanges = keyRanges;
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

        abstract T execute(ObPair<Long, ObTableParam> obTable) throws Exception;

        /*
         * Get row key.
         */
        public Row getRowKey() {
            return rowKey;
        }

        /*
         * Get key ranges.
         */
        public List<ObNewRange> getKeyRanges() {
            return keyRanges;
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
        boolean needRefreshTableEntry = false;
        boolean needRenew = false;
        int tryTimes = 0;
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
            ObPair<Long, ObTableParam> obPair = null;
            try {
                if (odpMode) {
                    obPair = getODPTableWithRowKey(tableName, callback.getRowKey(), needRenew);
                } else {
                    if (null != callback.getRowKey()) {
                        // in the case of retry, the location always needs to be refreshed here
                        if (tryTimes > 1) {
                            TableEntry entry = getOrRefreshTableEntry(tableName, false, false, false);
                            Long partId = getPartition(entry, callback.getRowKey());
                            refreshTableLocationByTabletId(entry, tableName, getTabletIdByPartId(entry, partId));
                        }
                        // using row key
                        obPair = getTable(tableName, callback.getRowKey(), needRefreshTableEntry, tableEntryRefreshIntervalWait, false, route);
                    } else if (null != callback.getKeyRanges()) {
                        // using scan range
                        obPair = getTable(tableName, new ObTableQuery(),
                            callback.getKeyRanges());
                    } else {
                        throw new ObTableException("RowKey or scan range is null");
                    }
                }
                T t = callback.execute(obPair);
                resetExecuteContinuousFailureCount(tableName);
                return t;
            } catch (Exception ex) {
                RUNTIME.error("execute while meet exception", ex);
                if (odpMode) {
                    if ((tryTimes - 1) < runtimeRetryTimes) {
                        if (ex instanceof ObTableException) {
                            logger
                                .warn(
                                    "execute while meet Exception, errorCode: {} , errorMsg: {}, try times {}",
                                    ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                    tryTimes);
                            // if the cause is that ODP partition meta have expired, try to fetch new one
                            if (ex instanceof ObTablePartitionChangeException
                                && ((ObTablePartitionChangeException) ex).getErrorCode() == OB_ERR_KV_ROUTE_ENTRY_EXPIRE.errorCode) {
                                needRenew = true;
                            } else {
                                throw ex;
                            }
                        } else {
                            logger.warn(
                                "execute while meet Exception, exception: {}, try times {}", ex,
                                tryTimes);
                            throw ex;
                        }
                    } else {
                        RUNTIME.error("retry failed with exception", ex);
                        throw ex;
                    }
                } else {
                    if (ex instanceof ObTableReplicaNotReadableException) {
                        if (obPair != null && (tryTimes - 1) < runtimeRetryTimes) {
                            logger.warn("retry when replica not readable: {}", ex.getMessage());
                            if (!odpMode) {
                                route.addToBlackList(obPair.getRight().getObTable().getIp());
                            }
                        } else {
                            logger.warn("exhaust retry when replica not readable: {}",
                                ex.getMessage());
                            RUNTIME.error("replica not readable", ex);
                            throw ex;
                        }
                    } else if (ex instanceof ObTableException
                               && ((ObTableException) ex).isNeedRefreshTableEntry()) {
                        // if the problem is the lack of row key name, throw directly
                        if (tableRowKeyElement.get(tableName) == null) {
                            throw ex;
                        }
                        needRefreshTableEntry = true;

                        logger
                            .warn(
                                "refresh table while meet Exception needing refresh, errorCode: {}, errorMsg: {}",
                                ((ObTableException) ex).getErrorCode(), ex.getMessage());
                        if (retryOnChangeMasterTimes && (tryTimes - 1) < runtimeRetryTimes) {
                            logger
                                .warn(
                                    "retry while meet Exception needing refresh, errorCode: {} , errorMsg: {},retry times {}",
                                    ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                    tryTimes);
                            if (ex instanceof ObTableNeedFetchAllException) {
                                getOrRefreshTableEntry(tableName, needRefreshTableEntry, isTableEntryRefreshIntervalWait(), true);
                                // reset failure count while fetch all route info
                                this.resetExecuteContinuousFailureCount(tableName);
                            }
                        } else {
                            calculateContinuousFailure(tableName, ex.getMessage());
                            throw ex;
                        }
                    } else {
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
            getOrRefreshTableEntry(tableName, true, isTableEntryRefreshIntervalWait(), true);
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
     * <p>
     * 1. cannot find table from tables, need refresh tables
     * 2. server list refresh failed: {see com.alipay.oceanbase.obproxy.resource.ObServerStateProcessor#MAX_REFRESH_FAILURE}
     *
     * @throws Exception if fail
     */
    public void syncRefreshMetadata() throws Exception {

        if (System.currentTimeMillis() - lastRefreshMetadataTimestamp < metadataRefreshInterval) {
            logger
                .warn(
                    "try to lock metadata refreshing, it has refresh  at: {}, dataSourceName: {}, url: {}",
                    lastRefreshMetadataTimestamp, dataSourceName, paramURL);
            return;
        }
        boolean acquired = refreshMetadataLock.tryLock(metadataRefreshLockTimeout,
            TimeUnit.MILLISECONDS);

        if (!acquired) {
            // TODO exception should be classified
            String errMsg = "try to lock metadata refreshing timeout " + "dataSource:"
                            + dataSourceName + " + refresh timeout:" + tableEntryRefreshLockTimeout
                            + ".";
            RUNTIME.error(errMsg);
            throw new ObTableGetException(errMsg);
        }

        try {

            if (System.currentTimeMillis() - lastRefreshMetadataTimestamp < metadataRefreshInterval) {
                logger.warn("it has refresh metadata at: {}, dataSourceName: {}, url: {}",
                    lastRefreshMetadataTimestamp, dataSourceName, paramURL);
                return;
            }
            if (logger.isInfoEnabled()) {
                logger.info("start refresh metadata, ts: {}, dataSourceName: {}, url: {}",
                    lastRefreshMetadataTimestamp, dataSourceName, paramURL);
            }

            this.ocpModel = loadOcpModel(paramURL, //
                dataSourceName,//
                rsListAcquireConnectTimeout,//
                rsListAcquireReadTimeout,//
                rsListAcquireTryTimes, //
                rsListAcquireRetryInterval);

            TableEntryKey allDummyKey = new TableEntryKey(clusterName, tenantName,
                OCEANBASE_DATABASE, ALL_DUMMY_TABLE);

            List<ObServerAddr> rsList = ocpModel.getObServerAddrs();

            TableEntry tableEntry = loadTableEntryRandomly(rsList,//
                allDummyKey,//
                tableEntryAcquireConnectTimeout,//
                tableEntryAcquireSocketTimeout, sysUA, initialized);

            List<ReplicaLocation> replicaLocations = tableEntry.getTableLocation()
                .getReplicaLocations();

            // update new ob table

            List<ObServerAddr> servers = new ArrayList<ObServerAddr>();

            for (ReplicaLocation replicaLocation : replicaLocations) {
                ObServerAddr addr = replicaLocation.getAddr();
                ObServerInfo info = replicaLocation.getInfo();
                if (!info.isActive()) {
                    logger.warn("will not refresh location {} because status is {} stop time {}",
                        addr.toString(), info.getStatus(), info.getStopTime());
                    continue;
                }

                servers.add(addr);

                if (tableRoster.containsKey(addr)) { // has ob table addr, continue
                    continue;
                }

                ObTable obTable = new ObTable.Builder(addr.getIp(), addr.getSvrPort()) //
                    .setLoginInfo(tenantName, userName, password, database) //
                    .setProperties(getProperties()).setConfigs(getTableConfigs())
                    .build();
                ObTable oldObTable = tableRoster.putIfAbsent(addr, obTable); // not control concurrency
                logger.warn("add new table addr, {}", addr.toString());
                if (oldObTable != null) { // maybe create two ob table concurrently, close current ob table
                    obTable.close();
                }
            }

            // clean useless ob table
            for (ObServerAddr addr : tableRoster.keySet()) {
                if (servers.contains(addr)) {
                    continue;
                }
                ObTable table = this.tableRoster.remove(addr);
                logger.warn("remove useless table addr, {}", addr.toString());
                if (table != null) {
                    table.close();
                }
            }
            this.serverRoster.reset(servers);
            // Get Server LDC info for weak read consistency.

            List<ObServerLdcItem> ldcServers = getServerLdc(serverRoster,
                tableEntryAcquireConnectTimeout, tableEntryAcquireSocketTimeout,
                serverAddressPriorityTimeout, serverAddressCachingTimeout, sysUA);

            // reset Server LDC location.
            String regionFromOcp = ocpModel.getIdc2Region(currentIDC);
            this.serverRoster.resetServerLdc(ObServerLdcLocation.buildLdcLocation(ldcServers,
                currentIDC, regionFromOcp));

            if (logger.isInfoEnabled()) {
                logger.info("finish refresh serverRoster: {}", JSON.toJSON(serverRoster));
            }
            this.lastRefreshMetadataTimestamp = System.currentTimeMillis();
        } finally {
            refreshMetadataLock.unlock();
            logger.warn("finish refresh all ob servers, ts: {}, dataSourceName: {}, url: {}",
                lastRefreshMetadataTimestamp, dataSourceName, paramURL);
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
        String indexTableName = dataTableName;
        if (indexName != null && !indexName.isEmpty() && !indexName.equalsIgnoreCase("PRIMARY")) {
            String tmpTableName = constructIndexTableName(dataTableName, indexName);
            if (tmpTableName == null) {
                throw new ObTableException("index table name is null");
            }
            ObIndexInfo indexInfo = getOrRefreshIndexInfo(tmpTableName, forceRefreshIndexInfo);
            if (indexInfo == null) {
                throw new ObTableException("index info is null, indexTableName:" + tmpTableName);
            }
            if (indexInfo.getIndexType().isGlobalIndex()) {
                indexTableName = tmpTableName;
                if (scanRangeColumns.isEmpty()) {
                    throw new ObTableException(
                        "query by global index need add all index keys in order, indexTableName:"
                                + indexTableName);
                } else {
                    addRowKeyElement(indexTableName,
                        scanRangeColumns.toArray(new String[scanRangeColumns.size()]));
                }
            }
        }
        return indexTableName;
    }

    @Override
    public void setRpcExecuteTimeout(int rpcExecuteTimeout) {
        this.properties.put(RPC_EXECUTE_TIMEOUT.getKey(), String.valueOf(rpcExecuteTimeout));
        this.rpcExecuteTimeout = rpcExecuteTimeout;
        if (null != tableRoster) {
            for (ObTable obTable : tableRoster.values()) {
                if (obTable != null) {
                    obTable.setObTableExecuteTimeout(rpcExecuteTimeout);
                }
            }
        }
        if (null != odpTable) {
            odpTable.setObTableExecuteTimeout(rpcExecuteTimeout);
        }
    }

    public String constructIndexTableName(final String dataTableName, final String indexName)
                                                                                             throws Exception {
        // construct index table name
        TableEntry entry = tableLocations.get(dataTableName);
        Long dataTableId = null;
        try {
            if (entry == null) {
                ObServerAddr addr = serverRoster.getServer(serverAddressPriorityTimeout,
                    serverAddressCachingTimeout);
                dataTableId = getTableIdFromRemote(addr, sysUA, tableEntryAcquireConnectTimeout,
                    tableEntryAcquireSocketTimeout, tenantName, database, dataTableName);
            } else {
                dataTableId = entry.getTableId();
            }
        } catch (Exception e) {
            RUNTIME.error("get index table name exception", e);
            throw e;
        }
        return "__idx_" + dataTableId + "_" + indexName;
    }

    public ObIndexInfo getOrRefreshIndexInfo(final String indexTableName, boolean forceRefresh)
                                                                                               throws Exception {

        ObIndexInfo indexInfo = indexinfos.get(indexTableName);
        if (!forceRefresh && indexInfo != null) {
            return indexInfo;
        }
        Lock tempLock = new ReentrantLock();
        Lock lock = refreshIndexInfoLocks.putIfAbsent(indexTableName, tempLock);
        lock = (lock == null) ? tempLock : lock;
        boolean acquired = lock.tryLock(tableEntryRefreshLockTimeout, TimeUnit.MILLISECONDS);
        if (!acquired) {
            String errMsg = "try to lock index infos refreshing timeout " + "dataSource:"
                            + dataSourceName + " ,indexTableName:" + indexTableName + " , timeout:"
                            + tableEntryRefreshLockTimeout + ".";
            RUNTIME.error(errMsg);
            throw new ObTableEntryRefreshException(errMsg);
        }
        try {
            indexInfo = indexinfos.get(indexTableName);
            if (!forceRefresh && indexInfo != null) {
                return indexInfo;
            } else {
                logger.info("index info is not exist, create new index info, indexTableName: {}",
                    indexTableName);
                int serverSize = serverRoster.getMembers().size();
                int refreshTryTimes = tableEntryRefreshTryTimes > serverSize ? serverSize
                    : tableEntryRefreshTryTimes;
                for (int i = 0; i < refreshTryTimes; i++) {
                    ObServerAddr serverAddr = serverRoster.getServer(serverAddressPriorityTimeout,
                        serverAddressCachingTimeout);
                    indexInfo = getIndexInfoFromRemote(serverAddr, sysUA,
                        tableEntryAcquireConnectTimeout, tableEntryAcquireSocketTimeout,
                        indexTableName);
                    if (indexInfo != null) {
                        indexinfos.put(indexTableName, indexInfo);
                    } else {
                        RUNTIME.error("get index info from remote is null, indexTableName: {}",
                            indexTableName);
                    }
                }
                return indexInfo;
            }
        } catch (Exception e) {
            RUNTIME.error("getOrRefresh index info meet exception", e);
            throw e;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get or refresh table entry.
     * @param tableName table name
     * @param refresh is re-fresh
     * @param waitForRefresh wait re-fresh
     * @return this
     * @throws Exception if fail
     */
    public TableEntry getOrRefreshTableEntry(final String tableName, final boolean refresh,
                                             final boolean waitForRefresh) throws Exception {
        return getOrRefreshTableEntry(tableName, refresh, waitForRefresh, false);
    }

    /**
     * Get or refresh table entry.
     * @param tableName table name
     * @param refresh is re-fresh
     * @param waitForRefresh wait re-fresh
     * @param fetchAll fetch all data from server if needed
     * @return this
     * @throws Exception if fail
     */
    public TableEntry getOrRefreshTableEntry(final String tableName, final boolean refresh,
                                             final boolean waitForRefresh, boolean fetchAll)
                                                                                            throws Exception {

        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        TableEntry tableEntry = tableLocations.get(tableName);
        // attempt the cached data and try best to avoid lock
        if (tableEntry != null) {
            //if the refresh is false indicates that user tolerate not the latest data
            if (!refresh) {
                return tableEntry;
            }
            // avoid unnecessary lock
            long punishInterval = (long) (tableEntryRefreshIntervalBase * Math.pow(2,
                -serverRoster.getMaxPriority()));
            punishInterval = Math.min(punishInterval, tableEntryRefreshIntervalCeiling);
            long current = System.currentTimeMillis();
            long interval = current - tableEntry.getRefreshTimeMills();
            long fetchAllInterval = current - tableEntry.getRefreshAllTimeMills();
            if ((fetchAll && (fetchAllInterval < punishInterval))
                || (!fetchAll && (interval < punishInterval))) {
                if (waitForRefresh) {
                    long toHoldTime = punishInterval - interval;
                    logger
                        .info(
                            "punish table entry {} : table entry refresh time {} punish interval {} current time {}. wait for refresh times {}",
                            tableName, tableEntry.getRefreshTimeMills(), punishInterval, current,
                            toHoldTime);
                    try {
                        // may have more elegant method ?
                        Thread.sleep(toHoldTime);
                    } catch (InterruptedException e) {
                        RUNTIME.error(LCD.convert("01-00018"), tableName, punishInterval, e);
                        throw new ObTableUnexpectedException("waiting for table entry " + tableName
                                                             + " punish interval " + punishInterval
                                                             + " is interrupted.");
                    }
                } else {
                    return tableEntry;
                }
            }
        }

        Lock tempLock = new ReentrantLock();
        Lock lock = refreshTableLocks.putIfAbsent(tableName, tempLock);
        lock = (lock == null) ? tempLock : lock; // check the first lock

        // attempt lock the refreshing action, avoiding concurrent refreshing
        // use the time-out mechanism, avoiding the rpc hanging up
        boolean acquired = lock.tryLock(tableEntryRefreshLockTimeout, TimeUnit.MILLISECONDS);

        if (!acquired) {
            String errMsg = "try to lock table-entry refreshing timeout " + "dataSource:"
                            + dataSourceName + " ,tableName:" + tableName + ", refresh:" + refresh
                            + " , timeout:" + tableEntryRefreshLockTimeout + ".";
            RUNTIME.error(errMsg);
            throw new ObTableEntryRefreshException(errMsg);
        }

        try {
            tableEntry = tableLocations.get(tableName);

            if (tableEntry != null) {
                // the server roster is ordered by priority
                long punishInterval = (long) (tableEntryRefreshIntervalBase * Math.pow(2,
                    -serverRoster.getMaxPriority()));
                punishInterval = Math.min(punishInterval, tableEntryRefreshIntervalCeiling);
                // control refresh frequency less than 100 milli second
                // just in case of connecting to OB Server failed or change master
                long interval = System.currentTimeMillis() - tableEntry.getRefreshTimeMills();
                long fetchAllInterval = System.currentTimeMillis()
                                        - tableEntry.getRefreshAllTimeMills();
                if ((fetchAll && (fetchAllInterval < punishInterval))
                    || (!fetchAll && (interval < punishInterval))) {
                    return tableEntry;
                }
            }

            if (tableEntry == null || refresh) {// not exist or need refresh, create new table entry
                if (logger.isInfoEnabled()) {
                    if (tableEntry == null) {
                        logger.info("tableEntry not exist, create new table entry, tablename: {}",
                            tableName);
                    } else {
                        logger.info(
                            "tableEntry need refresh, create new table entry, tablename: {}",
                            tableName);
                    }
                }

                int serverSize = serverRoster.getMembers().size();
                int refreshTryTimes = Math.min(tableEntryRefreshTryTimes, serverSize);

                for (int i = 0; i < refreshTryTimes; i++) {
                    try {
                        return refreshTableEntry(tableEntry, tableName, fetchAll);
                    } catch (ObTableNotExistException e) {
                        RUNTIME.error("getOrRefreshTableEntry meet exception", e);
                        throw e;
                    } catch (ObTableServerCacheExpiredException e) {
                        RUNTIME.error("getOrRefreshTableEntry meet exception", e);

                        if (logger.isInfoEnabled()) {
                            logger.info("server addr is expired and it will refresh metadata.");
                        }
                        syncRefreshMetadata();
                        tableEntryRefreshContinuousFailureCount.set(0);
                    } catch (ObTableEntryRefreshException e) {
                        RUNTIME.error("getOrRefreshTableEntry meet exception", e);
                        // if the problem is the lack of row key name, throw directly
                        if (tableRowKeyElement.get(tableName) == null) {
                            throw e;
                        }

                        if (tableEntryRefreshContinuousFailureCount.incrementAndGet() > tableEntryRefreshContinuousFailureCeiling) {
                            logger.error(LCD.convert("01-00019"),
                                tableEntryRefreshContinuousFailureCeiling);
                            syncRefreshMetadata();
                            tableEntryRefreshContinuousFailureCount.set(0);
                        }
                    } catch (Throwable t) {
                        RUNTIME.error("getOrRefreshTableEntry meet exception", t);
                        throw t;
                    }
                }
                // failure reach the try times may all the server change
                if (logger.isInfoEnabled()) {
                    logger
                        .info(
                            "refresh table entry has tried {}-times failure and will sync refresh metadata",
                            refreshTryTimes);
                }
                syncRefreshMetadata();
                return refreshTableEntry(tableEntry, tableName);
            }
            return tableEntry;
        } finally {
            lock.unlock();
        }
    }

    /**
     * 刷新 table entry 元数据
     * @param tableEntry
     * @param tableName
     * @return
     * @throws ObTableEntryRefreshException
     */
    private TableEntry refreshTableEntry(TableEntry tableEntry, String tableName)
                                                                                 throws ObTableEntryRefreshException {
        return refreshTableEntry(tableEntry, tableName, false);
    }

    public TableEntry refreshTableLocationByTabletId(TableEntry tableEntry, String tableName, Long tabletId) throws ObTableAuthException {
        TableEntryKey tableEntryKey = new TableEntryKey(clusterName, tenantName, database, tableName);
        try {
            if (tableEntry == null) {
                throw new ObTableEntryRefreshException("Table entry is null, tableName=" + tableName);
            }
            long lastRefreshTime = tableEntry.getPartitionEntry().getPartitionInfo(tabletId).getLastUpdateTime();
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastRefreshTime < tableEntryRefreshIntervalCeiling) {
                return tableEntry;
            }
            tableEntry = loadTableEntryLocationWithPriority(serverRoster, tableEntryKey, tableEntry, tabletId,
                    tableEntryAcquireConnectTimeout, tableEntryAcquireSocketTimeout,
                    serverAddressPriorityTimeout, serverAddressCachingTimeout, sysUA);

            tableEntry.prepareForWeakRead(serverRoster.getServerLdcLocation());

        } catch (ObTableNotExistException | ObTableServerCacheExpiredException e) {
            RUNTIME.error("RefreshTableEntry encountered an exception", e);
            throw e;
        } catch (Exception e) {
            String errorMsg = String.format("Failed to get table entry. Key=%s, TabletId=%d, message=%s", tableEntryKey, tabletId, e.getMessage());
            RUNTIME.error(LCD.convert("01-00020"), tableEntryKey, tableEntry, e);
            throw new ObTableEntryRefreshException(errorMsg, e);
        }

        tableLocations.put(tableName, tableEntry);
        tableEntryRefreshContinuousFailureCount.set(0);

        if (logger.isInfoEnabled()) {
            logger.info("Refreshed table entry. DataSource: {}, TableName: {}, Key: {}, Entry: {}",
                    dataSourceName, tableName, tableEntryKey, JSON.toJSON(tableEntry));
        }

        return tableEntry;
    }

    /**
     * 刷新 table entry 元数据
     * @param tableEntry
     * @param tableName
     * @param fetchAll
     * @return
     * @throws ObTableEntryRefreshException
     */
    private TableEntry refreshTableEntry(TableEntry tableEntry, String tableName, boolean fetchAll)
                                                                                                   throws ObTableEntryRefreshException {
        TableEntryKey tableEntryKey = new TableEntryKey(clusterName, tenantName, database,
            tableName);
        try {
            // if table entry is exist we just need to refresh table locations
            if (tableEntry != null && !fetchAll) {
                if (ObGlobal.obVsnMajor() >= 4) {
                    // do nothing
                } else {
                    // 3.x still proactively refreshes all locations
                    tableEntry = loadTableEntryLocationWithPriority(serverRoster, //
                            tableEntryKey,//
                            tableEntry,//
                            tableEntryAcquireConnectTimeout,//
                            tableEntryAcquireSocketTimeout,//
                            serverAddressPriorityTimeout, //
                            serverAddressCachingTimeout, sysUA);
                }
            } else {
                // if table entry is not exist we should fetch partition info and table locations
                tableEntry = loadTableEntryWithPriority(serverRoster, //
                    tableEntryKey,//
                    tableEntryAcquireConnectTimeout,//
                    tableEntryAcquireSocketTimeout,//
                    serverAddressPriorityTimeout,//
                    serverAddressCachingTimeout, sysUA);
                if (tableEntry.isPartitionTable()) {
                    switch (runningMode) {
                        case HBASE:
                            tableRowKeyElement.put(tableName, HBASE_ROW_KEY_ELEMENT);
                            tableEntry.setRowKeyElement(HBASE_ROW_KEY_ELEMENT);
                            break;
                        case NORMAL:
                            Map<String, Integer> rowKeyElement = tableRowKeyElement.get(tableName);
                            if (rowKeyElement != null) {
                                tableEntry.setRowKeyElement(rowKeyElement);
                            } else {
                                RUNTIME
                                    .error("partition table must add row key element name for table: "
                                           + tableName + " with table entry key: " + tableEntryKey);
                                throw new ObTableEntryRefreshException(
                                    "partition table must add row key element name for table: "
                                            + tableName + " with table entry key: " + tableEntryKey);
                            }
                    }
                    tableEntry.prepare();
                }
            }
            // prepare the table entry for weak read.
            tableEntry.prepareForWeakRead(serverRoster.getServerLdcLocation());
        } catch (ObTableNotExistException e) {
            RUNTIME.error("refreshTableEntry meet exception", e);
            throw e;
        } catch (ObTableServerCacheExpiredException e) {
            RUNTIME.error("refreshTableEntry meet exception", e);
            throw e;
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00020"), tableEntryKey, tableEntry, e);
            throw new ObTableEntryRefreshException(String.format(
                "failed to get table entry key=%s original tableEntry=%s ", tableEntryKey,
                tableEntry), e);
        }
        tableLocations.put(tableName, tableEntry);
        if (fetchAll) {
            tableEntry.setRefreshAllTimeMills(System.currentTimeMillis());
        }
        tableEntryRefreshContinuousFailureCount.set(0);
        if (logger.isInfoEnabled()) {
            logger.info(
                "refresh table entry, dataSource: {}, tableName: {}, refresh: {} key:{} entry:{} ",
                dataSourceName, tableName, true, tableEntryKey, JSON.toJSON(tableEntry));
        }
        return tableEntry;
    }

    /**
     * 根据 tableGroup 获取其中一个tableName
     * physicalTableName Complete table from table group
     * @param physicalTableName 
     * @param tableGroupName
     * @return
    * @throws Exception
     */
    private String refreshTableNameByTableGroup(String physicalTableName, String tableGroupName)
                                                                                                throws Exception {
        TableEntryKey tableEntryKey = new TableEntryKey(clusterName, tenantName, database,
            tableGroupName);
        String oldTableName = physicalTableName;
        try {
            physicalTableName = loadTableNameWithGroupName(serverRoster, //
                tableEntryKey,//
                tableEntryAcquireConnectTimeout,//
                tableEntryAcquireSocketTimeout,//
                serverAddressPriorityTimeout,//
                serverAddressCachingTimeout, sysUA);
        } catch (ObTableNotExistException e) {
            RUNTIME.error("refreshTableNameByTableGroup from tableGroup meet exception", e);
            throw e;
        } catch (ObTableServerCacheExpiredException e) {
            RUNTIME.error("refreshTableEntry from tableGroup meet exception", e);
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
                    "get table name from tableGroup, dataSource: {}, tableName: {}, refresh: {} key:{} realTableName:{} ",
                    dataSourceName, tableGroupName, true, tableEntryKey, physicalTableName);
        }
        return physicalTableName;
    }

    /**
     * 根据 rowkey 获取分区 id
     * @param tableEntry
     * @param row
     * @return
     */
    private long getPartition(TableEntry tableEntry, Row row) {
        // non partition
        if (!tableEntry.isPartitionTable()
            || tableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_ZERO) {
            return 0L;
        }
        if (tableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_ONE) {
            return tableEntry.getPartitionInfo().getFirstPartDesc().getPartId(row);
        }

        Long partId1 = tableEntry.getPartitionInfo().getFirstPartDesc().getPartId(row);
        Long partId2 = tableEntry.getPartitionInfo().getSubPartDesc().getPartId(row);
        return generatePartId(partId1, partId2);
    }

    /*
     * Get logicId(partition id in 3.x) from giving range
     */
    private List<Long> getPartitionsForLevelTwo(TableEntry tableEntry, Row startRow,
                                                boolean startIncluded, Row endRow,
                                                boolean endIncluded) throws Exception {
        if (tableEntry.getPartitionInfo().getLevel() != ObPartitionLevel.LEVEL_TWO) {
            RUNTIME.error("getPartitionsForLevelTwo need ObPartitionLevel LEVEL_TWO");
            throw new Exception("getPartitionsForLevelTwo need ObPartitionLevel LEVEL_TWO");
        }

        List<Long> partIds1 = tableEntry.getPartitionInfo().getFirstPartDesc()
            .getPartIds(startRow, startIncluded, endRow, endIncluded);
        List<Long> partIds2 = tableEntry.getPartitionInfo().getSubPartDesc()
            .getPartIds(startRow, startIncluded, endRow, endIncluded);

        List<Long> partIds = new ArrayList<Long>();
        if (partIds1.isEmpty()) {
            // do nothing
        } else if (partIds1.size() == 1) {
            long firstPartId = partIds1.get(0);
            for (Long partId2 : partIds2) {
                partIds.add(generatePartId(firstPartId, partId2));
            }
        } else {
            // construct all sub partition idx
            long subPartNum = tableEntry.getPartitionInfo().getSubPartDesc().getPartNum();
            List<Long> subPartIds = new ArrayList<Long>();
            for (long i = 0; i < subPartNum; i++) {
                subPartIds.add(i);
            }
            partIds2 = Collections.unmodifiableList(subPartIds);

            for (Long partId1 : partIds1) {
                for (Long partId2 : partIds2) {
                    partIds.add(generatePartId(partId1, partId2));
                }
            }
        }

        return partIds;
    }

    /**
     *
     * @param tableEntry
     * @param partId accept logic id (partId partitionId in 3.x)
     * @param route
     * @return
     */
    private ObPair<Long, ReplicaLocation> getPartitionReplica(TableEntry tableEntry, long partId,
                                                              ObServerRoute route) {
        return new ObPair<Long, ReplicaLocation>(partId, getPartitionLocation(tableEntry, partId,
            route));
    }

    /**
     *
     * @param tableEntry
     * @param partId accept logic id (partId partitionId in 3.x)
     * @param route
     * @return
     */
    private ReplicaLocation getPartitionLocation(TableEntry tableEntry, long partId,
                                                 ObServerRoute route) {
        long tabletId = getTabletIdByPartId(tableEntry, partId);
        return tableEntry.getPartitionEntry().getPartitionLocationWithTabletId(tabletId)
            .getReplica(route);

    }

    private ReplicaLocation getPartitionLocation(ObPartitionLocationInfo obPartitionLocationInfo,
                                                 ObServerRoute route) {
        return obPartitionLocationInfo.getPartitionLocation().getReplica(route);
    }

    /**
     *
     * @param tableName table want to get
     * @param rowKey row key
     * @param refresh whether to refresh
     * @param waitForRefresh whether wait for refresh
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    public ObPair<Long, ObTableParam> getTable(String tableName, Object[] rowKey,
                                                       boolean refresh, boolean waitForRefresh)
                                                                                               throws Exception {
        ObServerRoute route = getRoute(false);
        return getTable(tableName, rowKey, refresh, waitForRefresh, route);
    }

    /**
     *
     * @param tableName table want to get
     * @param rowKey row key
     * @param refresh whether to refresh
     * @param waitForRefresh whether wait for refresh
     * @param route ObServer route
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    public ObPair<Long, ObTableParam> getTable(String tableName, Object[] rowKey, boolean refresh,
                                               boolean waitForRefresh, ObServerRoute route)
                                                                                           throws Exception {
        return getTable(tableName, rowKey, refresh, waitForRefresh, false, route);
    }

    private ObPair<Long, ObTableParam> getTable(String tableName, Object[] rowKey,
                                                                 boolean refresh,
                                                                 boolean waitForRefresh,
                                                                 boolean needFetchAll,
                                                                 ObServerRoute route)
                                                                                     throws Exception {
        TableEntry tableEntry = getOrRefreshTableEntry(tableName, refresh, waitForRefresh,
            needFetchAll);
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
            for (int i = 0; i < rowKey.length; ++i) {
                if (i < curTableRowKeyNames.size()) {
                    row.add(curTableRowKeyNames.get(i), rowKey[i]);
                } else { // the rowKey element in the table only contain partition key(s) or the input row key has redundant elements
                    break;
                }
            }
        }

        long partId = getPartition(tableEntry, row); // partition id in 3.x, origin partId in 4.x, logicId
        if (refresh) {
            refreshTableLocationByTabletId(tableEntry, tableName, getTabletIdByPartId(tableEntry, partId));
        }
        return getTableInternal(tableName, tableEntry, partId, waitForRefresh, route);
    }

    /**
     *
     * @param tableName table want to get
     * @param rowKey row key values
     * @param needRenew flag to re-fetch partition meta information
     * @return ODP ObPair of partId and table
     * @throws Exception exception
     */
    public ObPair<Long, ObTableParam> getODPTableWithRowKeyValue(String tableName, Object[] rowKey,
                                                                 boolean needRenew)
                                                                                   throws Exception {
        TableEntry odpTableEntry = getOrFetchODPPartitionMeta(tableName, needRenew);
        Row row = new Row();
        if (odpTableEntry.isPartitionTable()
            && odpTableEntry.getPartitionInfo().getLevel() != ObPartitionLevel.LEVEL_ZERO) {
            List<String> curTableRowKeyNames = new ArrayList<String>();
            Map<String, Integer> tableRowKeyEle = getRowKeyElement(tableName);
            if (tableRowKeyEle != null) {
                curTableRowKeyNames = new ArrayList<String>(tableRowKeyEle.keySet());
            }
            if (curTableRowKeyNames.isEmpty()) {
                throw new IllegalArgumentException("Please make sure add row key elements");
            }

            // match the correct key to its row key
            for (int i = 0; i < rowKey.length; ++i) {
                if (i < curTableRowKeyNames.size()) {
                    row.add(curTableRowKeyNames.get(i), rowKey[i]);
                } else { // the rowKey element in the table only contain partition key(s) or the input row key has redundant elements
                    break;
                }
            }
        }
        long partId = getPartition(odpTableEntry, row);
        return getODPTableInternal(odpTableEntry, partId);
    }

    /**
     * For mutation (queryWithFilter)
     * @param tableName table want to get
     * @param query query
     * @param keyRanges key
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    public ObPair<Long, ObTableParam> getTable(String tableName, ObTableQuery query, List<ObNewRange> keyRanges)
                                                                                      throws Exception {
        Map<Long, ObTableParam> partIdMapObTable = new HashMap<Long, ObTableParam>();
        for (ObNewRange rang : keyRanges) {
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
            List<ObPair<Long, ObTableParam>> pairList = getTables(tableName, query, start,
                    borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd(), false,
                    false);
            for (ObPair<Long, ObTableParam> pair : pairList) {
                partIdMapObTable.put(pair.getLeft(), pair.getRight());
            }
        }

        if (partIdMapObTable.size() > 1) {
            throw new ObTablePartitionConsistentException(
                    "query and mutate must be a atomic operation");
        } else if (partIdMapObTable.size() < 1) {
            throw new ObTableException("could not find part id of range");
        }

        ObPair<Long, ObTableParam> ans = null;
        for (Long partId: partIdMapObTable.keySet()) {
            ans = new ObPair<>(partId, partIdMapObTable.get(partId));
        }
        return ans;
    }

    /**
     * For mutation execute
     * @param tableName table want to get
     * @param rowKey row key with column names
     * @param refresh whether to refresh
     * @param waitForRefresh whether wait for refresh
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    public ObPair<Long, ObTableParam> getTable(String tableName, Row rowKey,
                                                               boolean refresh,
                                                               boolean waitForRefresh)
                                                                                      throws Exception {
        return getTable(tableName, rowKey, refresh, waitForRefresh, false,
            getRoute(false));
    }

    /**
     * For mutation execute
     * @param tableName table want to get
     * @param rowKey row key with column names
     * @param refresh whether to refresh
     * @param waitForRefresh whether wait for refresh
     * @param needFetchAll whether to fetch all
     * @param route ObServer route
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    private ObPair<Long, ObTableParam> getTable(String tableName,
                                                                        Row rowKey,
                                                                        boolean refresh,
                                                                        boolean waitForRefresh,
                                                                        boolean needFetchAll,
                                                                        ObServerRoute route)
                                                                                            throws Exception {
        TableEntry tableEntry = getOrRefreshTableEntry(tableName, refresh, waitForRefresh,
            needFetchAll);
        long partId = getPartition(tableEntry, rowKey); // partition id in 3.x, origin partId in 4.x, logicId

        return getTableInternal(tableName, tableEntry, partId, waitForRefresh, route);
    }

    /**
     * For mutation execute to get ODP table param
     * @param tableName table want to get
     * @param rowKey row key with column names
     * @return ODP ObPair of partId and table
     * @throws Exception exception
     */
    private ObPair<Long, ObTableParam> getODPTableWithRowKey(String tableName, Row rowKey,
                                                             boolean needRenew) throws Exception {
        TableEntry odpTableEntry = getOrFetchODPPartitionMeta(tableName, needRenew);
        long partId = getPartition(odpTableEntry, rowKey);
        return getODPTableInternal(odpTableEntry, partId);
    }

    /**
     * get addr by pardId
     * @param tableName table want to get
     * @param partId partId of table (logicId, partition id in 3.x)
     * @param refresh whether to refresh
     * @param waitForRefresh whether wait for refresh
     * @param needFetchAll flag to fetch all
     * @param route ObServer route
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    public ObPair<Long, ObTableParam> getTableWithPartId(String tableName, long partId,
                                                         boolean refresh, boolean waitForRefresh,
                                                         boolean needFetchAll, ObServerRoute route)
                                                                                                   throws Exception {
        TableEntry tableEntry = getOrRefreshTableEntry(tableName, refresh, waitForRefresh,
            needFetchAll);
        return getTableInternal(tableName, tableEntry, partId, waitForRefresh, route);
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

        for (Map.Entry<ObServerAddr, ObTable> entry: tableRoster.entrySet()){
            if (Objects.equals(entry.getKey().getIp(), addr.getIp()) && Objects.equals(entry.getKey().getSvrPort(), addr.getSvrPort())){
                return entry.getValue();
            }
        }
       // If the node address does not exist, a new table is created
       return addTable(addr);
    }

    public ObTable addTable(ObServerAddr addr){

        try {
            logger.info("server from response not exist in route cache, server ip {}, port {} , execute add Table.", addr.getIp(), addr.getSvrPort());
            ObTable obTable = new ObTable.Builder(addr.getIp(), addr.getSvrPort()) //
                    .setLoginInfo(tenantName, userName, password, database) //
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

    /**
     * get ObTableParam by ODP partId
     * @param tableName table want to get
     * @param partId partId of table (logicId, partition id in 3.x)
     * @param needRenew flag to renew ODP partition meta
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    public ObPair<Long, ObTableParam> getODPTableWithPartId(String tableName, long partId,
                                                            boolean needRenew) throws Exception {
        TableEntry odpTableEntry = getOrFetchODPPartitionMeta(tableName, needRenew);
        return getODPTableInternal(odpTableEntry, partId);
    }

    /**
     * get addr from table entry by partId
     * @param tableName table want to get
     * @param tableEntry tableEntry
     * @param partId partId of table (logicId, partition id in 3.x)
     * @param waitForRefresh whether wait for refresh
     * @param route ObServer route
     * @return ObPair of partId and table
     * @throws Exception exception
     */
    public ObPair<Long, ObTableParam> getTableInternal(String tableName, TableEntry tableEntry,
                                                       long partId, boolean waitForRefresh,
                                                       ObServerRoute route) throws Exception {
        ReplicaLocation replica = null;
        long tabletId = getTabletIdByPartId(tableEntry, partId);
        long partitionId = partId;
        ObPartitionLocationInfo obPartitionLocationInfo = null;
        if (ObGlobal.obVsnMajor() >= 4) {

            obPartitionLocationInfo = getOrRefreshPartitionInfo(tableEntry, tableName, tabletId);
    
            replica = getPartitionLocation(obPartitionLocationInfo, route);
        } else {
            if (tableEntry.isPartitionTable()
                    && null != tableEntry.getPartitionInfo().getSubPartDesc()) {
                partitionId = ObPartIdCalculator.getPartIdx(partId, tableEntry
                        .getPartitionInfo().getSubPartDesc().getPartNum());
            }
            ObPair<Long, ReplicaLocation> partitionReplica = getPartitionReplica(tableEntry, partitionId,
                    route);
            replica = partitionReplica.getRight();
        }
        if (replica == null) {
            RUNTIME.error("Cannot get replica by partId: " + partId);
            throw new ObTableGetException("Cannot get replica by partId: " + partId);
        }
        ObServerAddr addr = replica.getAddr();
        ObTable obTable = tableRoster.get(addr);

        if (obTable == null || addr.isExpired(serverAddressCachingTimeout)) {
            if (obTable == null) {
                logger.warn("Cannot get ObTable by addr {}, refreshing metadata.", addr);
                syncRefreshMetadata();
            }
            if (addr.isExpired(serverAddressCachingTimeout)) {
                logger.info("Server addr {} is expired, refreshing tableEntry.", addr);
            }
            
            if (ObGlobal.obVsnMajor() >= 4) {
                obPartitionLocationInfo = getOrRefreshPartitionInfo(tableEntry, tableName, tabletId);
                replica = getPartitionLocation(obPartitionLocationInfo, route);
            } else {
                tableEntry = getOrRefreshTableEntry(tableName, true, waitForRefresh, false);
                replica = getPartitionReplica(tableEntry, partitionId, route).getRight();
            }
            
            addr = replica.getAddr();
            obTable = tableRoster.get(addr);

            if (obTable == null) {
                RUNTIME.error("Cannot get table by addr: " + addr);
                throw new ObTableGetException("Cannot get table by addr: " + addr);
            }
        }
        ObTableParam param = createTableParam(obTable, tableEntry, obPartitionLocationInfo, partId, tabletId);
        if (ObGlobal.obVsnMajor() >= 4) {
        } else {
            param.setPartId(partId);
            param.setTableId(tableEntry.getTableId());
            param.setPartitionId(partId);
        }
        addr.recordAccess();
        return new ObPair<>(tabletId, param);
    }

    private ObPartitionLocationInfo getOrRefreshPartitionInfo(TableEntry tableEntry,
                                                              String tableName, long tabletId)
                                                                                              throws Exception {
        ObPartitionLocationInfo obPartitionLocationInfo = tableEntry.getPartitionEntry()
            .getPartitionInfo(tabletId);
        if (!obPartitionLocationInfo.initialized.get()) {
            if (ObGlobal.obVsnMajor() >= 4) {
                tableEntry = refreshTableLocationByTabletId(tableEntry, tableName, tabletId);
            }
            obPartitionLocationInfo = tableEntry.getPartitionEntry().getPartitionInfo(tabletId);
            obPartitionLocationInfo.initializationLatch.await();
        }
        return obPartitionLocationInfo;
    }

    private ObTableParam createTableParam(ObTable obTable, TableEntry tableEntry,
                                          ObPartitionLocationInfo obPartitionLocationInfo,
                                          long partId, long tabletId) {
        ObTableParam param = new ObTableParam(obTable);
        param.setPartId(partId);
        if (ObGlobal.obVsnMajor() >= 4 && tableEntry != null) {
            param.setLsId(obPartitionLocationInfo.getTabletLsId());
        }
        param.setTableId(tableEntry.getTableId());
        param.setPartitionId(tabletId);
        return param;
    }

    private ObPair<Long, ObTableParam> getODPTableInternal(TableEntry odpTableEntry, long partId) {
        ObTable obTable = odpTable;
        ObTableParam param = new ObTableParam(obTable);
        param.setPartId(partId);
        long tabletId = partId;
        if (ObGlobal.obVsnMajor() >= 4) {
            long partIdx = odpTableEntry.getPartIdx(partId);
            tabletId = odpTableEntry.isPartitionTable() ? odpTableEntry.getPartitionInfo()
                    .getPartTabletIdMap().get(partIdx) : partId;
            param.setLsId(odpTableEntry.getPartitionEntry().getLsId(tabletId));
        }
        param.setTableId(odpTableEntry.getTableId());
        // real partition(tablet) id
        param.setPartitionId(tabletId);
        return new ObPair<Long, ObTableParam>(partId, param);
    }

    /**
     * 根据 start-end 获取 partition id 和 addr
     * @param tableEntry
     * @param startRow
     * @param startIncluded
     * @param endRow
     * @param endIncluded
     * @param route
     * @return
     * @throws Exception
     */
    private List<ObPair<Long, ReplicaLocation>> getPartitionReplica(TableEntry tableEntry,
                                                                    String tableName,
                                                                    Row startRow,
                                                                    boolean startIncluded,
                                                                    Row endRow,
                                                                    boolean endIncluded,
                                                                    ObServerRoute route) throws Exception {
        List<ObPair<Long, ReplicaLocation>> replicas = new ArrayList<>();

        if (!tableEntry.isPartitionTable() || tableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_ZERO) {
            long tabletId = getTabletIdByPartId(tableEntry, 0L);
            ObPartitionLocationInfo locationInfo = getOrRefreshPartitionInfo(tableEntry, tableName, tabletId);
            replicas.add(new ObPair<>(tabletId, getPartitionLocation(locationInfo, route)));
            return replicas;
        }

        ObPartitionLevel partitionLevel = tableEntry.getPartitionInfo().getLevel();
        List<Long> partIds = getPartitionTablePartitionIds(tableEntry, startRow, startIncluded, endRow, endIncluded, partitionLevel);

        if (ObGlobal.obVsnMajor() >= 4) {
            for (Long partId : partIds) {
                long tabletId = getTabletIdByPartId(tableEntry, partId);
                ObPartitionLocationInfo locationInfo = getOrRefreshPartitionInfo(tableEntry, tableName, tabletId);
                replicas.add(new ObPair<>(tabletId, getPartitionLocation(locationInfo, route)));
            }
        } else {
            for (Long partId : partIds) {
                long partitionId = partId;
                if (tableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_TWO) {
                    partitionId = ObPartIdCalculator.getPartIdx(partId, tableEntry
                            .getPartitionInfo().getSubPartDesc().getPartNum());
                }
                replicas.add(new ObPair<Long, ReplicaLocation>(partId, getPartitionLocation(
                        tableEntry, partitionId, route)));
            }
        }

        return replicas;
    }

    private List<Long> getPartitionTablePartitionIds(TableEntry tableEntry,
                                                     Row startRow, boolean startIncluded,
                                                     Row endRow, boolean endIncluded,
                                                     ObPartitionLevel level)
                                                                                                 throws Exception {
        if (level == ObPartitionLevel.LEVEL_ONE) {
            return tableEntry.getPartitionInfo().getFirstPartDesc()
                .getPartIds(startRow, startIncluded, endRow, endIncluded);
        } else if (level == ObPartitionLevel.LEVEL_TWO) {
            return getPartitionsForLevelTwo(tableEntry, startRow, startIncluded,
                    endRow, endIncluded);
        } else {
            RUNTIME.error("not allowed bigger than level two");
            throw new ObTableGetException("not allowed bigger than level two");
        }
    }

    public long getTabletIdByPartId(TableEntry tableEntry, Long partId) {
        if (ObGlobal.obVsnMajor() >= 4 && tableEntry.isPartitionTable()) {
            ObPartitionInfo partInfo = tableEntry.getPartitionInfo();
            Map<Long, Long> tabletIdMap = partInfo.getPartTabletIdMap();
            long partIdx = tableEntry.getPartIdx(partId);
            return tabletIdMap.getOrDefault(partIdx, partId);
        }
        return partId;
    }

    /**
     * 根据 ODPTableEntry 获取 logicIds
     * @param odpTableEntry
     * @param startRow
     * @param startIncluded
     * @param endRow
     * @param endIncluded
     * @return partIds
     * @throws Exception
     */
    private List<Long> getOdpPartIds(TableEntry odpTableEntry, Row startRow, boolean startIncluded,
                                     Row endRow, boolean endIncluded) throws Exception {
        if (!odpTableEntry.isPartitionTable()
                || odpTableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_ZERO) {
            List<Long> partIds = new ArrayList<Long>();
            partIds.add(0L);
            return partIds;
        } else if (odpTableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_ONE) {
            return odpTableEntry.getPartitionInfo().getFirstPartDesc()
                    .getPartIds(startRow, startIncluded, endRow, endIncluded);
        } else if (odpTableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_TWO) {
            return getPartitionsForLevelTwo(odpTableEntry, startRow, startIncluded, endRow,
                    endIncluded);
        } else {
            RUNTIME.error("not allowed bigger than level two");
            throw new ObTableGetException("not allowed bigger than level two");
        }
    }

    /**
     * 根据 start-end 获取 partition ids 和 addrs
     * @param tableName table want to get
     * @param query query
     * @param start start key
     * @param startInclusive whether include start key
     * @param end end key
     * @param endInclusive whether include end key
     * @param refresh whether to refresh
     * @param waitForRefresh whether wait for refresh
     * @return list of ObPair of partId(logicId) and table obTableParams
     * @throws Exception exception
     */
    public List<ObPair<Long, ObTableParam>> getTables(String tableName, ObTableQuery query,
                                                      Object[] start, boolean startInclusive,
                                                      Object[] end, boolean endInclusive,
                                                      boolean refresh, boolean waitForRefresh)
                                                                                              throws Exception {
        return getTables(tableName, query, start, startInclusive, end, endInclusive, refresh,
            waitForRefresh, getRoute(false));
    }

    /**
     * 根据 start-end 获取 partition id 和 addr
     * @param tableName table want to get
     * @param query query
     * @param start start key
     * @param startInclusive whether include start key
     * @param end end key
     * @param endInclusive whether include end key
     * @param refresh whether to refresh
     * @param waitForRefresh whether wait for refresh
     * @param route server route
     * @return list of ObPair of partId(logicId) and tableParam
     * @throws Exception exception
     */
    public List<ObPair<Long, ObTableParam>> getTables(String tableName, ObTableQuery query,
                                                      Object[] start, boolean startInclusive,
                                                      Object[] end, boolean endInclusive,
                                                      boolean refresh, boolean waitForRefresh,
                                                      ObServerRoute route) throws Exception {

        // 1. get TableEntry information
        TableEntry tableEntry = getOrRefreshTableEntry(tableName, refresh, waitForRefresh, false);

        List<String> scanRangeColumns = query.getScanRangeColumns();
        if (scanRangeColumns == null || scanRangeColumns.isEmpty()) {
            Map<String, Integer> tableEntryRowKeyElement = getRowKeyElement(tableName);
            if (tableEntryRowKeyElement != null) {
                scanRangeColumns = new ArrayList<String>(tableEntryRowKeyElement.keySet());
            }
        }
        // 2. get replica location
        // partIdWithReplicaList -> List<pair<logicId(partition id in 3.x), replica>>
        if (start.length != end.length) {
            throw new IllegalArgumentException("length of start key and end key is not equal");
        }

        Row startRow = new Row();
        Row endRow = new Row();
        // ensure the format of column names and values if the current table is a table with partition
        if (tableEntry.isPartitionTable()) {
            if ((scanRangeColumns == null || scanRangeColumns.isEmpty()) && start.length == 1
                && start[0] instanceof ObObj && ((ObObj) start[0]).isMinObj() && end.length == 1
                && end[0] instanceof ObObj && ((ObObj) end[0]).isMaxObj()) {
                // for getPartition to query all partitions
                scanRangeColumns = new ArrayList<String>(Collections.nCopies(start.length,
                    "partition"));
            }
            // scanRangeColumn may be longer than start/end in prefix scanning situation
            if (scanRangeColumns == null || scanRangeColumns.size() < start.length) {
                throw new IllegalArgumentException(
                    "length of key and scan range columns do not match, please use addRowKeyElement or set scan range columns");
            }
            for (int i = 0; i < start.length; i++) {
                startRow.add(scanRangeColumns.get(i), start[i]);
                endRow.add(scanRangeColumns.get(i), end[i]);
            }
        }

        List<ObPair<Long, ReplicaLocation>> partIdWithReplicaList = getPartitionReplica(tableEntry, tableName,
            startRow, startInclusive, endRow, endInclusive, route);

        // obTableParams -> List<Pair<logicId, obTableParams>>
        List<ObPair<Long, ObTableParam>> obTableParams = new ArrayList<ObPair<Long, ObTableParam>>();
        for (ObPair<Long, ReplicaLocation> partIdWithReplica : partIdWithReplicaList) {
            long partId = partIdWithReplica.getLeft();
            ReplicaLocation replica = partIdWithReplica.getRight();
            ObServerAddr addr = replica.getAddr();
            ObTable obTable = tableRoster.get(addr);
            boolean addrExpired = addr.isExpired(serverAddressCachingTimeout);
            if (addrExpired || obTable == null) {
                logger
                    .warn(
                        "server address {} is expired={} or can not get ob table. So that will sync refresh metadata",
                        addr, addrExpired);
                syncRefreshMetadata();
                tableEntry = getOrRefreshTableEntry(tableName, true, waitForRefresh, false);
                replica = getPartitionLocation(tableEntry, partId, route);
                addr = replica.getAddr();
                obTable = tableRoster.get(addr);
            }

            if (obTable == null) {
                RUNTIME.error("cannot get table by addr: " + addr);
                throw new ObTableGetException("cannot get table by addr: " + addr);
            }

            ObTableParam param = new ObTableParam(obTable);
            param.setPartId(partId);
            if (ObGlobal.obVsnMajor() >= 4) {
                long partIdx = tableEntry.getPartIdx(partId);
                partId = tableEntry.isPartitionTable() ? tableEntry.getPartitionInfo()
                    .getPartTabletIdMap().get(partIdx) : partId;
                param.setLsId(tableEntry.getPartitionEntry().getLsId(partId));
            }

            param.setTableId(tableEntry.getTableId());
            // real partition(tablet) id
            param.setPartitionId(partId);

            addr.recordAccess();
            obTableParams.add(new ObPair<Long, ObTableParam>(partIdWithReplica.getLeft(), param));
        }

        return obTableParams;
    }

    public List<ObPair<Long, ObTableParam>> getOdpTables(String tableName, ObTableQuery query,
                                                         Object[] start, boolean startInclusive,
                                                         Object[] end, boolean endInclusive, boolean needRenew)
            throws Exception {
        List<ObPair<Long, ObTableParam>> obTableParams = new ArrayList<ObPair<Long, ObTableParam>>();
        TableEntry odpTableEntry = getOrFetchODPPartitionMeta(tableName, needRenew);

        List<String> scanRangeColumns = query.getScanRangeColumns();
        if (scanRangeColumns == null || scanRangeColumns.isEmpty()) {
            Map<String, Integer> tableEntryRowKeyElement = getRowKeyElement(tableName);
            if (tableEntryRowKeyElement != null) {
                scanRangeColumns = new ArrayList<String>(tableEntryRowKeyElement.keySet());
            }
        }
        // 2. get replica location
        // partIdWithReplicaList -> List<pair<logicId(partition id in 3.x), replica>>
        if (start.length != end.length) {
            throw new IllegalArgumentException("length of start key and end key is not equal");
        }

        Row startRow = new Row();
        Row endRow = new Row();
        // ensure the format of column names and values if the current table is a table with partition
        if (odpTableEntry.isPartitionTable()
                && odpTableEntry.getPartitionInfo().getLevel() != ObPartitionLevel.LEVEL_ZERO) {
            if ((scanRangeColumns == null || scanRangeColumns.isEmpty()) && start.length == 1
                    && start[0] instanceof ObObj && ((ObObj) start[0]).isMinObj() && end.length == 1
                    && end[0] instanceof ObObj && ((ObObj) end[0]).isMaxObj()) {
                // for getPartition to query all partitions
                scanRangeColumns = new ArrayList<String>(Collections.nCopies(start.length,
                        "partition"));
            }
            // scanRangeColumn may be longer than start/end in prefix scanning situation
            if (scanRangeColumns == null || scanRangeColumns.size() < start.length) {
                throw new IllegalArgumentException(
                        "length of key and scan range columns do not match, please use addRowKeyElement or set scan range columns");
            }
            for (int i = 0; i < start.length; i++) {
                startRow.add(scanRangeColumns.get(i), start[i]);
                endRow.add(scanRangeColumns.get(i), end[i]);
            }
        }

        List<Long> partIds = getOdpPartIds(odpTableEntry, startRow, startInclusive, endRow,
                endInclusive);
        for (Long partId : partIds) {
            ObTable obTable = odpTable;
            ObTableParam param = new ObTableParam(obTable);
            param.setPartId(partId);
            Long tabletId = partId;
            if (ObGlobal.obVsnMajor() >= 4) {
                long partIdx = odpTableEntry.getPartIdx(partId);
                tabletId = odpTableEntry.isPartitionTable() ? odpTableEntry.getPartitionInfo()
                        .getPartTabletIdMap().get(partIdx) : partId;
                param.setLsId(odpTableEntry.getPartitionEntry().getLsId(tabletId));
            }
            param.setTableId(odpTableEntry.getTableId());
            // real partition(tablet) id
            param.setPartitionId(tabletId);
            obTableParams.add(new ObPair<Long, ObTableParam>(partId, param));
        }

        return obTableParams;
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
        String physicalTableName = TableGroupCache.get(tableGroupName); // tableGroup -> Table
        // get tableName from cache
        if (physicalTableName != null && !refresh) {
            return physicalTableName;
        }

        // not find in cache, should get tableName from observer
        Lock tempLock = new ReentrantLock();
        Lock lock = TableGroupCacheLocks.putIfAbsent(tableGroupName, tempLock);
        lock = (lock == null) ? tempLock : lock; // check the first lock

        // attempt lock the refreshing action, avoiding concurrent refreshing
        // use the time-out mechanism, avoiding the rpc hanging up
        boolean acquired = lock.tryLock(metadataRefreshLockTimeout, TimeUnit.MILLISECONDS);

        if (!acquired) {
            String errMsg = "try to lock tableGroup inflect timeout " + "dataSource:"
                            + dataSourceName + " ,tableName:" + tableGroupName + " , timeout:"
                            + metadataRefreshLockTimeout + ".";
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
                    return refreshTableNameByTableGroup(physicalTableName, tableGroupName);
                } catch (ObTableNotExistException e) {
                    RUNTIME.error("getOrRefreshTableName from TableGroup meet exception", e);
                    throw e;
                } catch (ObTableServerCacheExpiredException e) {
                    RUNTIME.error("getOrRefreshTableName from TableGroup meet exception", e);

                    if (logger.isInfoEnabled()) {
                        logger.info("server addr is expired and it will refresh metadata.");
                    }
                    syncRefreshMetadata();
                } catch (Throwable t) {
                    RUNTIME.error("getOrRefreshTableName from TableGroup meet exception", t);
                    throw t;
                }
                // failure reach the try times may all the server change
                if (logger.isInfoEnabled()) {
                    logger.info("refresh table Name from TableGroup failure");
                }
            }
            return newPhyTableName;
        } finally {
            lock.unlock();
        }
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
            public Map<String, Object> execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
            public Long execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
     * @param keyRanges scan range
     * @param columns columns name to update
     * @param values new values
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload updateWithResult(final String tableName, final Row rowKey,
                                      final List<ObNewRange> keyRanges, final String[] columns,
                                      final Object[] values) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, keyRanges) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTableParam tableParam = obPair.getRight();
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
            public Long execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
     * @param keyRanges scan range
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload deleteWithResult(final String tableName, final Row rowKey,
                                      final List<ObNewRange> keyRanges) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, keyRanges) {

                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTableParam tableParam = obPair.getRight();
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
            public Long execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
     * @param keyRanges scan range
     * @param columns columns name to insert
     * @param values new values
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload insertWithResult(final String tableName, final Row rowKey,
                                      final List<ObNewRange> keyRanges, final String[] columns,
                                      final Object[] values) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, keyRanges) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTableParam tableParam = obPair.getRight();
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
                    public Map<String, Object> execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                        long TableTime = System.currentTimeMillis();
                        ObTableParam tableParam = obPair.getRight();
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
     * @param keyRanges scan range
     * @param columns columns name to put
     * @param values new values
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload putWithResult(final String tableName, final Row rowKey,
                                   final List<ObNewRange> keyRanges, final String[] columns,
                                   final Object[] values) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, keyRanges) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTableParam tableParam = obPair.getRight();
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
            public Long execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
     * @param keyRanges scan range
     * @param columns columns name to replace
     * @param values new values
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload replaceWithResult(final String tableName, final Row rowKey,
                                       final List<ObNewRange> keyRanges, final String[] columns,
                                       final Object[] values) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, keyRanges) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTableParam tableParam = obPair.getRight();
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
            public Long execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
     * @param keyRanges scan range
     * @param columns columns name to InsertOrUpdate
     * @param values new values
     * @param usePut use put or not
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload insertOrUpdateWithResult(final String tableName, final Row rowKey,
                                              final List<ObNewRange> keyRanges,
                                              final String[] columns, final Object[] values,
                                              boolean usePut) throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, keyRanges) {
                /**
                 * Execute.
                 */
                @Override
                public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTableParam tableParam = obPair.getRight();
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
             * @param obPair
             * @return
             * @throws Exception
             */
            @Override
            public Map<String, Object> execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
     * @param keyRanges scan range
     * @param columns columns name to increment
     * @param values new values
     * @param withResult whether to bring back result
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload incrementWithResult(final String tableName, final Row rowKey,
                                         final List<ObNewRange> keyRanges, final String[] columns,
                                         final Object[] values, final boolean withResult)
                                                                                         throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, keyRanges) {
                /**
                 *
                 * @param obPair
                 * @return
                 * @throws Exception
                 */
                @Override
                public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTableParam tableParam = obPair.getRight();
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
            public Map<String, Object> execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long getTableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
     * @param keyRanges scan range
     * @param columns columns name to append
     * @param values new values
     * @param withResult whether to bring back row result
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload appendWithResult(final String tableName, final Row rowKey,
                                      final List<ObNewRange> keyRanges, final String[] columns,
                                      final Object[] values, final boolean withResult)
                                                                                      throws Exception {
        final long start = System.currentTimeMillis();
        return execute(tableName,
            new OperationExecuteCallback<ObPayload>(rowKey, keyRanges) {
                @Override
                public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                    long TableTime = System.currentTimeMillis();
                    ObTableParam tableParam = obPair.getRight();
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
        ObPair<Long, ObTableParam> obPair = null;
        if (odpMode) {
            obPair = getODPTableWithRowKey(tableName, rowKey, refresh);
        } else {
            if (refresh) {
                obPair = getTable(tableName, rowKey, true, true, true, getRoute(false));
            } else {
                obPair = getTable(tableName, rowKey, false, false, false, getRoute(false));
            }
        }
        ObTableParam tableParam = obPair.getRight();
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
        if (odpMode) {
            List<ObPair<Long, ObTableParam>> allTables = getOdpTables(tableName, new ObTableQuery(), new Object[]{ ObObj.getMin() }, true,
                        new Object[]{ ObObj.getMax() }, true, refresh);
            for (ObPair<Long, ObTableParam> table : allTables) {
                ObTableParam tableParam = table.getRight();
                Partition partition = new Partition(tableParam.getPartitionId(), table.getLeft(), tableParam.getTableId(),
                        tableParam.getObTable().getIp(), tableParam.getObTable().getPort(), tableParam.getLsId());
                partitions.add(partition);
            }
        } else {
            List<ObPair<Long, ObTableParam>> allTables;
            if (refresh) {
                // List<ObPair<logic partId, obTableParam>>
                allTables = getTables(tableName, new ObTableQuery(), new Object[]{ ObObj.getMin() }, true,
                        new Object[]{ ObObj.getMax() }, true, true, true, getRoute(false));
            } else {
                // List<ObPair<logic partId, obTableParam>>
                allTables = getTables(tableName, new ObTableQuery(), new Object[]{ ObObj.getMin() }, true,
                        new Object[]{ ObObj.getMax() }, true, false, false, getRoute(false));
            }
            for (ObPair<Long, ObTableParam> table : allTables) {
                ObTableParam tableParam = table.getRight();
                Partition partition = new Partition(tableParam.getPartitionId(), tableParam.getPartId(), tableParam.getTableId(),
                        tableParam.getObTable().getIp(), tableParam.getObTable().getPort(), tableParam.getLsId());
                partitions.add(partition);
            }
        }
        return partitions;
    }

    /**
     * fetch ODP partition meta information
     * @param tableName table name to query
     * @param needRenew flag to force ODP to fetch the latest partition meta information
     * @return TableEntry ODPTableEntry
     * @throws Exception Exception
     */
    private TableEntry getOrFetchODPPartitionMeta(String tableName, boolean needRenew)
                                                                                      throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        TableEntry odpTableEntry = ODPTableLocations.get(tableName);
        Long lastOdpRefreshTimeMills = null;
        Long reFetchInterval = 500L;

        // already have odpTableEntry
        if (odpTableEntry != null) {
            lastOdpRefreshTimeMills = odpTableEntry.getOdpRefreshTimeMills();
            // if no need to fetch new meta, directly return
            if (!needRenew) {
                return odpTableEntry;
            }
        }
        Lock tmpLock = new ReentrantLock();
        Lock lock = fetchODPPartitionLocks.putIfAbsent(tableName, tmpLock);
        lock = (lock == null) ? tmpLock : lock;
        // attempt lock the refreshing action, avoiding concurrent refreshing
        // use the time-out mechanism, avoiding the rpc hanging up
        boolean acquired = lock.tryLock(ODPTableEntryRefreshLockTimeout, TimeUnit.MILLISECONDS);

        if (!acquired) {
            String errMsg = "try to lock odpTable-entry refreshing timeout " + "dataSource:"
                            + dataSourceName + " ,tableName:" + tableName + " , timeout:"
                            + ODPTableEntryRefreshLockTimeout + ".";
            RUNTIME.error(errMsg);
            throw new ObTableEntryRefreshException(errMsg);
        }

        if (ODPTableLocations.get(tableName) != null) {
            odpTableEntry = ODPTableLocations.get(tableName);
            long interval = System.currentTimeMillis() - odpTableEntry.getRefreshTimeMills();
            // do not fetch partition meta if and only if the refresh interval is less than 0.5 seconds
            // and no need to fore renew
            if (interval < reFetchInterval) {
                if (!needRenew) {
                    lock.unlock();
                    return odpTableEntry;
                }
                Thread.sleep(reFetchInterval - interval);
            }
        }

        boolean forceRenew = needRenew;
        boolean done = false;
        int retryTime = 0;
        try {
            do {
                try {
                    ObFetchPartitionMetaRequest request = ObFetchPartitionMetaRequest.getInstance(
                        ObFetchPartitionMetaType.GET_PARTITION_META.getIndex(), tableName,
                        clusterName, tenantName, database, forceRenew,
                        odpTable.getObTableOperationTimeout()); // TODO: timeout setting need to be verified
                    ObPayload result = odpTable.execute(request);
                    checkObFetchPartitionMetaResult(lastOdpRefreshTimeMills, request, result);
                    ObFetchPartitionMetaResult obFetchPartitionMetaResult = (ObFetchPartitionMetaResult) result;
                    odpTableEntry = obFetchPartitionMetaResult.getTableEntry();
                    TableEntryKey key = new TableEntryKey(clusterName, tenantName, database,
                        tableName);
                    odpTableEntry.setTableEntryKey(key);
                    if (odpTableEntry.isPartitionTable()) {
                        switch (runningMode) {
                            case HBASE:
                                tableRowKeyElement.put(tableName, HBASE_ROW_KEY_ELEMENT);
                                odpTableEntry.setRowKeyElement(HBASE_ROW_KEY_ELEMENT);
                                break;
                            case NORMAL:
                                Map<String, Integer> rowKeyElement = tableRowKeyElement.get(tableName);
                                if (rowKeyElement != null) {
                                    odpTableEntry.setRowKeyElement(rowKeyElement);
                                } else {
                                    RUNTIME.error("partition table must has row key element key ="
                                            + key);
                                    throw new ObTableEntryRefreshException(
                                            "partition table must has row key element key ="
                                                    + key);
                                }
                        }
                    }
                    ODPTableLocations.put(tableName, odpTableEntry);
                    done = true;
                } catch (Exception ex) {
                    RUNTIME.error("Fetching ODP partition meta meet exception", ex);
                    if (tableRowKeyElement.get(tableName) == null) {
                        // if the error is missing row key element, directly throw
                        throw ex;
                    }
                    if (ex instanceof ObTableException) {
                        forceRenew = true; // force ODP to fetch the latest partition meta
                        retryTime++;
                    } else {
                        throw ex;
                    }
                }
            } while (!done && retryTime < 3);
            return odpTableEntry;
        } finally {
            lock.unlock();
        }
    }

    private void checkObFetchPartitionMetaResult(Long lastOdpRefreshTimeMills,
                                                 ObFetchPartitionMetaRequest request,
                                                 ObPayload result) {
        if (result == null) {
            RUNTIME.error("client get unexpected NULL result");
            throw new ObTableException("client get unexpected NULL result");
        }

        if (!(result instanceof ObFetchPartitionMetaResult)) {
            RUNTIME.error("client get unexpected result: " + result.getClass().getName());
            throw new ObTableException("client get unexpected result: "
                                       + result.getClass().getName());
        }

        if (lastOdpRefreshTimeMills != null) {
            if (lastOdpRefreshTimeMills >= ((ObFetchPartitionMetaResult) result).getCreateTime()) {
                throw new ObTableException("client get outdated result from ODP");
            }
        }

    }

    /**
     * execute mutation with filter
     * @param tableQuery table query
     * @param rowKey row key which want to mutate
     * @param keyRanges scan range
     * @param operation table operation
     * @param withResult whether to bring back result
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload mutationWithFilter(final TableQuery tableQuery, final Row rowKey,
                                        final List<ObNewRange> keyRanges,
                                        final ObTableOperation operation, final boolean withResult)
                                                                                                   throws Exception {
        return mutationWithFilter(tableQuery, rowKey, keyRanges, operation, withResult, false,
            false);
    }

    /**
     * execute mutation with filter
     * @param tableQuery table query
     * @param rowKey row key which want to mutate
     * @param keyRanges scan range
     * @param operation table operation
     * @param withResult whether to bring back result
     * @param checkAndExecute whether execute check and execute instead of query and mutate
     * @param checkExists whether to check exists or not
     * @return execute result
     * @throws Exception exception
     */
    public ObPayload mutationWithFilter(final TableQuery tableQuery, final Row rowKey,
                                        final List<ObNewRange> keyRanges,
                                        final ObTableOperation operation, final boolean withResult,
                                        final boolean checkAndExecute, final boolean checkExists)
                                                                                                 throws Exception {
        final long start = System.currentTimeMillis();
        if (tableQuery != null && tableQuery.getObTableQuery().getKeyRanges().isEmpty()) {
            // fill a whole range if no range is added explicitly.
            tableQuery.getObTableQuery().addKeyRange(ObNewRange.getWholeRange());
        }
        return execute(tableQuery.getTableName(), new OperationExecuteCallback<ObPayload>(
            rowKey, keyRanges) {
            /**
             * Execute.
             */
            @Override
            public ObPayload execute(ObPair<Long, ObTableParam> obPair) throws Exception {
                long TableTime = System.currentTimeMillis();
                ObTableParam tableParam = obPair.getRight();
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
            getRouteTableRefresher().addTableIfAbsent(tableName, true);
            getRouteTableRefresher().triggerRefreshTable();
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
            return new ObClusterTableQuery(tableQuery).asyncExecuteInternal();
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
                int maxRetries = getRuntimeRetryTimes(); // Define the maximum number of retries
                int tryTimes = 0;
                long startExecute = System.currentTimeMillis();
                boolean needRefreshTableEntry = false;
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
                            List<ObPair<Long, ObTableParam>> pairList = getTables(request.getTableName(),
                                    tableQuery, start, borderFlag.isInclusiveStart(), end,
                                    borderFlag.isInclusiveEnd(), needRefreshTableEntry, isTableEntryRefreshIntervalWait());
                            for (ObPair<Long, ObTableParam> pair : pairList) {
                                partIdMapObTable.put(pair.getLeft(), pair.getRight());
                            }
                        }

                        // Check if partIdMapObTable size is greater than 1
                        if (partIdMapObTable.size() > 1) {
                            throw new ObTablePartitionConsistentException(
                                    "query and mutate must be a atomic operation");
                        }
                        // Proceed with the operation
                        Map.Entry<Long, ObTableParam> entry = partIdMapObTable.entrySet().iterator().next();
                        ObTableParam tableParam = entry.getValue();
                        request.setTableId(tableParam.getTableId());
                        request.setPartitionId(tableParam.getPartitionId());
                        request.setTimeout(tableParam.getObTable().getObTableOperationTimeout());
                        ObTable obTable = tableParam.getObTable();

                        // Attempt to execute the operation
                        return executeWithRetry(obTable, request, request.getTableName());
                    } catch (Exception ex) {
                        tryTimes++;
                        if (ex instanceof ObTableException && ((ObTableException) ex).isNeedRefreshTableEntry()) {
                            needRefreshTableEntry = true;
                            logger.warn(
                                    "tablename:{} partition id:{} batch ops refresh table while meet ObTableMasterChangeException, errorCode: {}",
                                    request.getTableName(), request.getPartitionId(), ((ObTableException) ex).getErrorCode(), ex);

                            if (isRetryOnChangeMasterTimes() && tryTimes <= maxRetries) {
                                logger.warn(
                                        "tablename:{} partition id:{} batch ops retry while meet ObTableMasterChangeException, errorCode: {} , retry times {}",
                                        request.getTableName(), request.getPartitionId(), ((ObTableException) ex).getErrorCode(),
                                        tryTimes, ex);

                                if (ex instanceof ObTableNeedFetchAllException) {
                                    // Refresh table info
                                    getOrRefreshTableEntry(request.getTableName(), needRefreshTableEntry, isTableEntryRefreshIntervalWait(), true);
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

    @Override
    public String toString() {
        return "ObTableClient {\n serverRoster = " + serverRoster.getMembers()
               + ", \n serverIdc = " + serverRoster.getServerLdcLocation()
               + ", \n tableLocations = " + tableLocations + ", \n tableRoster = " + tableRoster
               + ", \n ocpModel = " + ocpModel + "\n}\n";
    }

    public ConcurrentHashMap<String, String> getTableGroupInverted() {
        return TableGroupInverted;
    }

    public ConcurrentHashMap<String, String> getTableGroupCache() {
        return TableGroupCache;
    }

    /**
     * get table route fail than clear table group message
     * @param tableGroupName table group name that need to delete
     */
    public void eraseTableGroupFromCache(String tableGroupName) {
        // clear table group cache
        TableGroupInverted.remove(TableGroupCache.get(tableGroupName));
        TableGroupCache.remove(tableGroupName);
        TableGroupCacheLocks.remove(tableGroupName);
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
        TableEntry tableEntry = getOrRefreshTableEntry(tableName, true, false, false);

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
        TableEntry tableEntry = getOrRefreshTableEntry(tableName, true, false, false);

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
}

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
import com.alipay.oceanbase.rpc.location.LocationUtil;
import com.alipay.oceanbase.rpc.location.model.partition.*;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObBorderFlag;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.table.ObTableServerCapacity;
import com.alipay.oceanbase.rpc.util.StringUtil;
import com.alipay.oceanbase.rpc.util.ZoneUtil;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.rpc.constant.Constants.*;
import static com.alipay.oceanbase.rpc.location.LocationUtil.*;
import static com.alipay.oceanbase.rpc.location.model.partition.ObPartIdCalculator.generatePartId;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;
import static java.lang.String.format;

public class TableRoute {
    private static final Logger       logger                 = getLogger(TableRoute.class);
    private static final ObjectMapper objectMapper           = new ObjectMapper();
    private final ObTableClient       tableClient;
    private final ObUserAuth          sysUA;                                               // user and password to access route table
    private final ServerRoster        serverRoster           = new ServerRoster();         // all servers which contain current tenant
    private long                      clusterVersion         = -1;
    private volatile long             lastRefreshMetadataTimestamp;
    private volatile ConfigServerInfo configServerInfo       = new ConfigServerInfo();     // rslist and IDC
    private volatile TableRoster      tableRoster            = new TableRoster();          // table mean connection pool here
    private TableLocations            tableLocations         = null;                       // map[tableName, TableEntry]
    private TableLocations            odpTableLocations      = null;                       // for parition handle
    private IndexLocations            indexLocations         = null;                       // global index location
    private TableGroupCache           tableGroupCache        = null;
    private OdpInfo                   odpInfo                = null;
    private RouteTableRefresher       routeRefresher         = null;

    public final Lock                 refreshTableRosterLock = new ReentrantLock();

    public TableRoute(ObTableClient tableClient, ObUserAuth sysUA) {
        this.tableClient = tableClient;
        this.sysUA = sysUA;
        if (tableClient.isOdpMode()) {
            odpTableLocations = new TableLocations(tableClient);
        } else {
            tableLocations = new TableLocations(tableClient);
            indexLocations = new IndexLocations(tableClient);
            tableGroupCache = new TableGroupCache(tableClient);
        }
    }

    public void close() throws ObTableCloseException {
        if (routeRefresher != null) {
            routeRefresher.close();
        }
        tableRoster.closeRoster();
    }

    public void setConfigServerInfo(ConfigServerInfo configServerInfo) {
        this.configServerInfo = configServerInfo;
    }

    /**
     * get tableEntry by tableName,
     * this methods will guarantee the tableEntry is not null
     * */
    public TableEntry getTableEntry(String tableName) throws Exception {
        TableEntry tableEntry;
        tableEntry = tableLocations.getTableEntry(tableName);
        if (tableEntry == null) {
            tableEntry = refreshMeta(tableName);
        }
        return tableEntry;
    }

    /**
     * erase the tableEntry cached in tableLocations
     * */
    public void eraseTableEntry(String tableName) {
        tableLocations.eraseTableEntry(tableName);
    }

    /**
     * get ODP ObTable Connection
     * */
    public ObTable getOdpTable() {
        return odpInfo == null ? null : odpInfo.getObTable();
    }

    /**
     * get ObTable Connection by server address
     * */
    public ObTable getTable(ObServerAddr addr) {
        return tableRoster.getTable(addr);
    }

    public TableRoster getTableRoster() {
        return tableRoster;
    }

    public ServerRoster getServerRoster() {
        return serverRoster;
    }

    public ConfigServerInfo getConfigServerInfo() {
        return configServerInfo;
    }

    public Map<String, TableEntry> getTableLocations() {
        return tableLocations.getLocations();
    }

    public long getLastRefreshMetadataTimestamp() {
        return lastRefreshMetadataTimestamp;
    }

    @VisibleForTesting
    public ObTable getFirstObTable() {
        if (tableClient.isOdpMode()) {
            return odpInfo.getObTable();
        }
        return tableRoster.getTables().entrySet().iterator().next().getValue();
    }

    public ObTableServerCapacity getServerCapacity() {
        if (tableClient.isOdpMode()) {
            if (odpInfo == null || odpInfo.getObTable() == null) {
                throw new IllegalStateException("client is not initialized and obTable is empty");
            }
            return odpInfo.getObTable().getServerCapacity();
        } else {
            if (tableRoster == null || tableRoster.getTables().isEmpty()) {
                throw new IllegalStateException("client is not initialized and obTable is empty");
            }
            Iterator<ObTable> iterator = tableRoster.getTables().values().iterator();
            ObTable firstObTable = iterator.next();
            return firstObTable.getServerCapacity();
        }
    }

    public void buildOdpInfo(String odpAddr, int odpPort, ObTableClient.RunningMode runningMode)
                                                                                       throws Exception {
        this.odpInfo = new OdpInfo(odpAddr, odpPort);
        this.odpInfo.buildOdpTable(tableClient.getTenantName(), tableClient.getFullUserName(),
            tableClient.getPassword(), tableClient.getDatabase(), runningMode,
            tableClient.getProperties(), tableClient.getTableConfigs());
    }

    /**
     * load rsList from rootService
     * */
    public ConfigServerInfo loadConfigServerInfo() throws Exception {
        this.configServerInfo = LocationUtil.loadConfigServerInfo(tableClient.getParamURL(),
            tableClient.getDataSourceName(), tableClient.getRsListAcquireConnectTimeout(),
            tableClient.getRsListAcquireReadTimeout(), tableClient.getRsListAcquireTryTimes(),
            tableClient.getRsListAcquireRetryInterval());
        return configServerInfo;
    }

    /**
     * init tableRoster and serverRoster
     * tableRoster stores all observer connection belongs to the current tenant
     * serverRoster stores all observer address and LDC information for weak-reading
     * */
    public void initRoster(TableEntryKey rootServerKey, boolean initialized,
                           ObTableClient.RunningMode runningMode) throws Exception {
        List<ObServerAddr> servers = new ArrayList<ObServerAddr>();
        ConcurrentHashMap<ObServerAddr, ObTable> addr2Table = new ConcurrentHashMap<ObServerAddr, ObTable>();
        List<ObServerAddr> rsList = configServerInfo.getRsList();
        BOOT.info("{} success to get rsList, paramURL: {}, rsList: {}，idc2Region: {}",
            tableClient.getDatabase(), configServerInfo.getParamURL(), objectMapper.writeValueAsString(rsList),
                objectMapper.writeValueAsString(configServerInfo.getIdc2Region()));

        TableEntry tableEntry = null;
        int retryMaxTimes = rsList.size();
        int retryTimes = 0;
        boolean success = false;
        Exception exception = null;
        while (!success && retryTimes < retryMaxTimes) {
            try {
                ObServerAddr obServerAddr = rsList.get(retryTimes);
                tableEntry = loadTableEntryRandomly(obServerAddr,//
                    rootServerKey,//
                    tableClient.getTableEntryAcquireConnectTimeout(),//
                    tableClient.getTableEntryAcquireSocketTimeout(), sysUA, initialized);
                BOOT.info("{} success to get tableEntry with rootServerKey all_dummy_tables {}",
                    tableClient.getDatabase(), objectMapper.writeValueAsString(tableEntry));
                success = true;
            } catch (ObTableEntryRefreshException e) {
                if (e.isConnectInactive()) {
                    logger
                        .warn(
                            "current server addr is invalid but rsList is not updated, ip: {}, sql port: {}",
                            rsList.get(retryTimes).getIp(), rsList.get(retryTimes).getSqlPort());
                    retryTimes++;
                    exception = e;
                } else {
                    throw e;
                }
            }
        } // end while
        if (!success) {
            BOOT.error("all rs servers are not available, rootServerKey:{}, rsList: {}",
                rootServerKey, rsList);
            throw new ObTableUnexpectedException("all rs servers are not available", exception);
        }
        List<ReplicaLocation> replicaLocations = tableEntry.getTableLocation()
            .getReplicaLocations();
        BOOT.info("{} success to get replicaLocation {}", tableClient.getDatabase(),
                objectMapper.writeValueAsString(replicaLocations));

        for (ReplicaLocation replicaLocation : replicaLocations) {
            ObServerInfo info = replicaLocation.getInfo();
            ObServerAddr addr = replicaLocation.getAddr();
            if (!info.isActive()) {
                BOOT.warn("will not init location {} because status is {}", addr.toString(),
                    info.getStatus());
                continue;
            }

            try {
                ObTable obTable = new ObTable.Builder(addr.getIp(), addr.getSvrPort())
                    .setLoginInfo(tableClient.getTenantName(), tableClient.getUserName(),
                        tableClient.getPassword(), tableClient.getDatabase(),
                        tableClient.getClientType(runningMode))
                    .setProperties(tableClient.getProperties())
                    .setConfigs(tableClient.getTableConfigs()).setObServerAddr(addr).build();
                addr2Table.put(addr, obTable);
                servers.add(addr);
            } catch (Exception e) {
                BOOT.warn("initMetadata meet exception", e);
                throw e;
            }
        }

        BOOT.info("{} success to build server connection {}", tableClient.getDatabase(),
            objectMapper.writeValueAsString(servers));
        this.tableRoster = TableRoster.getInstanceOf(tableClient.getTenantName(),
            tableClient.getUserName(), tableClient.getPassword(), tableClient.getDatabase(),
            tableClient.getClientType(runningMode), tableClient.getProperties(),
            tableClient.getTableConfigs());
        this.tableRoster.setTables(addr2Table);
        this.serverRoster.reset(servers);

        // Get Server LDC info for weak read consistency.
        if (StringUtil.isEmpty(tableClient.getCurrentIDC())) {
            tableClient.setCurrentIDC(ZoneUtil.getCurrentIDC());
        }
        String regionFromOcp = configServerInfo.getIdc2Region(tableClient.getCurrentIDC());
        BOOT.info("{} success get currentIDC {}, regionFromOcp {}", tableClient.getDatabase(),
            tableClient.getCurrentIDC(), regionFromOcp);

        success = false;
        retryMaxTimes = servers.size();
        retryTimes = 0;
        List<ObServerLdcItem> ldcServers = null;
        while (!success && retryTimes < retryMaxTimes) {
            try {
                ldcServers = LocationUtil.getServerLdc(serverRoster,
                    tableClient.getTableEntryAcquireConnectTimeout(),
                    tableClient.getTableEntryAcquireSocketTimeout(),
                    tableClient.getServerAddressPriorityTimeout(), sysUA);
                success = true;
            } catch (ObTableEntryRefreshException e) {
                if (e.isConnectInactive()) {
                    logger.warn("current server addr is invalid but not updated, retryTimes: {}",
                        retryTimes);
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        } // end while
        if (!success) {
            BOOT.error("all tenant servers are not available, tenant: {}, rsList: {}",
                rootServerKey.getTenantName(), rsList);
            throw new ObTableUnexpectedException("all tenant servers are not available");
        }
        this.serverRoster.resetServerLdc(ObServerLdcLocation.buildLdcLocation(ldcServers,
            tableClient.getCurrentIDC(), regionFromOcp));
        if (BOOT.isInfoEnabled()) {
            BOOT.info("{} finish refresh serverRoster: {}", tableClient.getDatabase(),
                objectMapper.writeValueAsString(serverRoster));
            BOOT.info("finish initMetadata for all tables for database {}",
                tableClient.getDatabase());
        }
        // record last refresh meta time
        this.lastRefreshMetadataTimestamp = System.currentTimeMillis();
    }

    public void launchRouteRefresher() {
        routeRefresher = new RouteTableRefresher(tableClient, sysUA);
        routeRefresher.start();
    }

    public void removeObServer(ObServerAddr addr) {
        logger.debug("remove useless table addr, {}", addr.toString());
        ConcurrentHashMap<ObServerAddr, ObTable> tables = this.tableRoster.getTables();
        ObTable table = tables.remove(addr);
        if (table != null) {
            table.close();
        }
        List<ObServerAddr> servers = this.serverRoster.getMembers();
        servers.remove(addr);
    }

    /**
     * refresh all ob server synchronized, it will not refresh if last refresh time is 1 min ago
     * @param newRsList new root servers
     * situations need to refresh:
     * 1. cannot find table from tables, need refresh tables
     * 2. cannot create JDBC connection by one of the servers
     * 3. sql execution timeout or meet exceptions
     *
     * @throws Exception if fail
     */
    public void refreshRosterByRsList(List<ObServerAddr> newRsList) throws Exception {
        if (logger.isInfoEnabled()) {
            logger.info("start refresh metadata, dataSourceName: {}, url: {}",
                configServerInfo.getLocalFile(), configServerInfo.getParamURL());
        }

        TableEntryKey allDummyKey = new TableEntryKey(tableClient.getClusterName(),
            tableClient.getTenantName(), OCEANBASE_DATABASE, ALL_DUMMY_TABLE);

        TableEntry tableEntry = null;
        int retryMaxTimes = newRsList.size();
        int retryTimes = 0;
        boolean success = false;
        while (!success && retryTimes < retryMaxTimes) {
            try {
                ObServerAddr obServerAddr = newRsList.get(retryTimes);
                tableEntry = loadTableEntryRandomly(obServerAddr,//
                    allDummyKey,//
                    tableClient.getTableEntryAcquireConnectTimeout(),//
                    tableClient.getTableEntryAcquireSocketTimeout(), sysUA, true);
                success = true;
            } catch (ObTableEntryRefreshException e) {
                if (e.isConnectInactive()) {
                    logger
                        .warn(
                            "current server addr is invalid but rsList is not updated, ip: {}, sql port: {}",
                            newRsList.get(retryTimes).getIp(), newRsList.get(retryTimes)
                                .getSqlPort());
                    retryTimes++;
                } else {
                    throw e;
                }
            }
        } // end while
        if (!success) {
            logger.error("all rs servers are not available, rootServerKey:{}, rsList: {}",
                allDummyKey, newRsList);
            throw new ObTableUnexpectedException("all rs servers are not available");
        }

        // 1. get tenant server address to renew ObTable roster
        List<ReplicaLocation> replicaLocations = tableEntry.getTableLocation()
            .getReplicaLocations();
        // update new ob table and get new server address
        List<ObServerAddr> servers = tableRoster.refreshTablesAndGetNewServers(replicaLocations);
        serverRoster.reset(servers);

        // 2. Get Server LDC info for weak read consistency.
        success = false;
        retryMaxTimes = servers.size();
        retryTimes = 0;
        List<ObServerLdcItem> ldcServers = null;
        while (!success && retryTimes < retryMaxTimes) {
            try {
                ldcServers = LocationUtil.getServerLdc(serverRoster,
                    tableClient.getTableEntryAcquireConnectTimeout(),
                    tableClient.getTableEntryAcquireSocketTimeout(),
                    tableClient.getServerAddressPriorityTimeout(), sysUA);
                success = true;
            } catch (ObTableEntryRefreshException e) {
                if (e.isConnectInactive()) {
                    retryTimes++;
                    logger.warn("current server addr is invalid but not updated, retryTimes: {}",
                        retryTimes);
                } else {
                    throw e;
                }
            }
        } // end while
        if (!success) {
            logger.error("all tenant servers are not available, tenant: {}, serverRoster: {}",
                allDummyKey.getTenantName(), objectMapper.writeValueAsString(serverRoster));
            throw new ObTableUnexpectedException("all tenant servers are not available");
        }

        // 3. reset Server LDC location.
        String regionFromOcp = configServerInfo.getIdc2Region(tableClient.getCurrentIDC());
        serverRoster.resetServerLdc(ObServerLdcLocation.buildLdcLocation(ldcServers,
            tableClient.getCurrentIDC(), regionFromOcp));

        if (logger.isInfoEnabled()) {
            logger.info("finish refresh serverRoster: {}, servers num: {}",
                objectMapper.writeValueAsString(serverRoster), servers.size());
        }
        lastRefreshMetadataTimestamp = System.currentTimeMillis();
    }

    /*------------------------------------------------------------------------Single Operation Routing------------------------------------------------------------------------*/

    /**
     * get global index table name by the index and the table name
     * */
    public String getIndexTableName(final String tableName, final String indexName,
                                    List<String> scanRangeColumns, boolean forceRefreshIndexInfo)
                                                                                                 throws Exception {
        String indexTableName = tableName;
        if (indexName != null && !indexName.isEmpty() && !indexName.equalsIgnoreCase("PRIMARY")) {
            String tmpTableName = constructIndexTableName(tableName, indexName);
            if (tmpTableName == null) {
                throw new ObTableException("index table name is null");
            }
            ObIndexInfo indexInfo = indexLocations.getOrRefreshIndexInfo(tmpTableName,
                forceRefreshIndexInfo, serverRoster, sysUA);
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
                    tableClient.addRowKeyElement(indexTableName,
                        scanRangeColumns.toArray(new String[scanRangeColumns.size()]));
                }
            }
        }
        return indexTableName;
    }

    private String constructIndexTableName(String tableName, String indexName) throws Exception {
        // construct index table name
        TableEntry entry = getTableEntry(tableName);
        Long dataTableId = null;
        try {
            if (entry == null) {
                ObServerAddr addr = serverRoster.getServer(tableClient
                    .getServerAddressPriorityTimeout());
                dataTableId = getTableIdFromRemote(addr, sysUA,
                    tableClient.getTableEntryAcquireConnectTimeout(),
                    tableClient.getTableEntryAcquireSocketTimeout(), tableClient.getTenantName(),
                    tableClient.getDatabase(), tableName);
            } else {
                dataTableId = entry.getTableId();
            }
        } catch (Exception e) {
            RUNTIME.error("get index table name exception", e);
            throw e;
        }
        return "__idx_" + dataTableId + "_" + indexName;
    }

    /**
     * refresh the specific table's tablet meta information,
     * like part_num, part_level, etc.
     * */
    public TableEntry refreshMeta(String tableName) throws Exception {
        long runtimeMaxWait = tableClient.getRuntimeMaxWait();
        int retryTime = 0;
        long start = System.currentTimeMillis();
        while (true) {
            long costMillis = System.currentTimeMillis() - start;
            if (costMillis > runtimeMaxWait) {
                throw new ObTableTimeoutExcetion("it has tried " + retryTime
                                                 + " times and it has waited " + costMillis
                                                 + "/ms which exceeds response timeout "
                                                 + runtimeMaxWait + "/ms");
            }
            try {
                return tableLocations.refreshMeta(tableName, serverRoster, sysUA);
            } catch (ObTableTryLockTimeoutException e) {
                // if try lock timeout, need to retry
                logger.warn("wait to try lock to timeout when refresh table meta, tryTimes: {}",
                    retryTime, e);
                retryTime++;
            }
        }
    }

    /**
     * refresh the tablet replica location of the specific table
     * */
    public TableEntry refreshPartitionLocation(String tableName, long tabletId, TableEntry entry)
                                                                                                 throws Exception {
        TableEntry tableEntry = entry == null ? tableLocations.getTableEntry(tableName) : entry;
        try {
            tableEntry = tableLocations.refreshPartitionLocation(tableEntry, tableName, tabletId,
                serverRoster, sysUA);
            validCachedObTableStatus(tableName, tableEntry, tabletId, tableClient.getRoute(false));
            return tableEntry;
        } catch (ObTableGetException e) {
            logger
                .warn(
                    "refresh partition location meets tableEntry not initialized exception, tableName: {}",
                    tableName);
            if (e.getMessage().contains("Need to fetch meta")) {
                tableEntry = refreshMeta(tableName);
                tableEntry = tableLocations.refreshPartitionLocation(tableEntry, tableName, tabletId,
                        serverRoster, sysUA);
                validCachedObTableStatus(tableName, tableEntry, tabletId, tableClient.getRoute(false));
                return tableEntry;
            }
            throw e;
        } catch (Throwable t) {
            logger.error(
                "refresh partition location meets exception, tableName: {}, error message: {}",
                tableName, t.getMessage());
            throw t;
        }
    }

    public TableEntry refreshTabletLocationBatch(String tableName) throws Exception {
        TableEntry tableEntry = tableLocations.getTableEntry(tableName);
        try {
            tableEntry = tableLocations.refreshTabletLocationBatch(tableEntry, tableName,
                serverRoster, sysUA);
            Long[] tablets = getTabletsFromTableEntry(tableEntry);
            for (long tablet : tablets) {
                validCachedObTableStatus(tableName, tableEntry, tablet, tableClient.getRoute(false));
            }
            return tableEntry;
        } catch (ObTableGetException e) {
            logger
                .warn(
                    "refresh location in batch meets tableEntry not initialized exception, tableName: {}",
                    tableName);
            if (e.getMessage().contains("Need to fetch meta")) {
                tableEntry = refreshMeta(tableName);
                tableEntry = tableLocations.refreshTabletLocationBatch(tableEntry, tableName,
                    serverRoster, sysUA);
                Long[] tablets = getTabletsFromTableEntry(tableEntry);
                for (long tablet : tablets) {
                    validCachedObTableStatus(tableName, tableEntry, tablet, tableClient.getRoute(false));
                }
                return tableEntry;
            }
            throw e;
        } catch (Throwable t) {
            logger.error(
                    "refresh location in batch meets exception, tableName: {}, error message: {}",
                    tableName, t.getMessage());
            throw t;
        }
    }

    public void refreshTabletLocationForAtomicQuery(String tableName, ObTableQuery query, boolean isHKV) throws Exception {
        Map<Long, ObTableParam> partIdParamMap = getPartIdParamMapForQuery(tableName, query.getScanRangeColumns(), query.getKeyRanges());
        if (isHKV) {
            // for HBase process, if distributed function is enabled, no need to do routing refresh
            boolean isDistributedSupported = getServerCapacity().isSupportDistributedExecute();
            if (partIdParamMap.size() > 1 && !isDistributedSupported) {
                throw new ObTablePartitionConsistentException(
                        "query and mutate must be a atomic operation");
            } else if (isDistributedSupported) {
                return;
            }
        } else {
            // for table process, distributed function is not supported yet, need to refresh routing
            // for now only support to query single tablet
            if (partIdParamMap.size() > 1) {
                throw new ObTablePartitionConsistentException(
                        "query and mutate must be a atomic operation");
            } else if (partIdParamMap.isEmpty()) {
                throw new ObTableException("could not find part id of range");
            }
        }
        Map.Entry<Long, ObTableParam> entry = partIdParamMap.entrySet().iterator().next();
        TableEntry tableEntry = getTableEntry(tableName);
        long tabletId = entry.getValue().getTabletId();
        refreshPartitionLocation(tableName, tabletId, tableEntry);
    }

    private Long[] getTabletsFromTableEntry(TableEntry tableEntry) {
        Long[] tablets = null;
        if (tableEntry.isPartitionTable()) {
            tablets = tableEntry.getPartitionInfo().getPartTabletIdMap().values()
                    .toArray(new Long[0]);
        } else {
            tablets = new Long[]{0L};
        }
        return tablets;
    }

    private void validCachedObTableStatus(String tableName, TableEntry tableEntry, long tabletId, ObServerRoute route) throws Exception {
        ObPartitionLocationInfo obPartitionLocationInfo = getOrRefreshPartitionInfo(tableEntry, tableName, tabletId);
        if (obPartitionLocationInfo.getPartitionLocation() == null) {
            throw new ObTableNotExistException(
                    "partition location is null after refresh, table: { " + tableName
                            + " } may not exist");
        }
        ReplicaLocation replica = getPartitionLocation(obPartitionLocationInfo, route);
        ObServerAddr addr = replica.getAddr();
        ObTable obTable = tableRoster.getTable(addr);
        if (obTable != null) {
            obTable.setValid();
        }
    }
    /**
     * get TableParam by tableName and rowkey
     * @param tableName tableName
     * @param rowkey row key or partition key names and values
     * @return ObTableParam tableParam
     * */
    public ObTableParam getTableParam(String tableName, Row rowkey) throws Exception {
        ObServerRoute route = tableClient.getRoute(false);
        return getTableParamWithRoute(tableName, rowkey, route);
    }

    public ObTableParam getTableParamWithRoute(String tableName, Row rowkey, ObServerRoute route)
                                                                                                 throws Exception {
        TableEntry tableEntry = getTableEntry(tableName);
        if (tableEntry == null) {
            logger.error("tableEntry is null, tableName: {}", tableName);
            throw new ObTableEntryRefreshException("tableEntry is null, tableName: " + tableName);
        }
        long partId = getPartId(tableEntry, rowkey);
        return getTableInternal(tableName, tableEntry, partId, route);
    }

    /**
     * get TableParam by tableName and rowkeys in batch
     * @param tableName tableName
     * @param rowkeys list of row key or partition key names and values
     * @return ObTableParam tableParam
     * */
    public List<ObTableParam> getTableParams(String tableName, List<Row> rowkeys) throws Exception {
        TableEntry tableEntry = getTableEntry(tableName);
        if (tableEntry == null) {
            logger.error("tableEntry is null, tableName: {}", tableName);
            throw new ObTableEntryRefreshException("tableEntry is null, tableName: " + tableName);
        }

        List<ObTableParam> params = new ArrayList<>();
        ObServerRoute route = tableClient.getRoute(false);
        for (Row rowkey : rowkeys) {
            long partId = getPartId(tableEntry, rowkey);
            ObTableParam param = null;
            param = getTableInternal(tableName, tableEntry, partId, route);
            params.add(param);
        }
        return params;
    }

    public long getTabletIdByPartId(TableEntry tableEntry, Long partId) {
        if (tableEntry.isPartitionTable()) {
            ObPartitionInfo partInfo = tableEntry.getPartitionInfo();
            Map<Long, Long> tabletIdMap = partInfo.getPartTabletIdMap();
            long partIdx = tableEntry.getPartIdx(partId);
            return tabletIdMap.getOrDefault(partIdx, partId);
        }
        return partId;
    }

    /**
     * 根据 rowkey 获取分区 id
     * @param tableEntry
     * @param row
     * @return logic id of tablet
     */
    public long getPartId(TableEntry tableEntry, Row row) {
        // non partition
        if (!tableEntry.isPartitionTable()
            || tableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_ZERO) {
            return 0L;
        } else if (tableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_ONE) {
            return tableEntry.getPartitionInfo().getFirstPartDesc().getPartId(row);
        }
        Long partId1 = tableEntry.getPartitionInfo().getFirstPartDesc().getPartId(row);
        Long partId2 = tableEntry.getPartitionInfo().getSubPartDesc().getPartId(row);
        return generatePartId(partId1, partId2);
    }

    private ObPartitionLocationInfo getOrRefreshPartitionInfo(TableEntry tableEntry,
                                                              String tableName, long tabletId)
                                                                                              throws Exception {
        ObPartitionLocationInfo obPartitionLocationInfo = tableEntry.getPartitionEntry()
            .getPartitionInfo(tabletId);
        if (!obPartitionLocationInfo.initialized.get()) {
            tableEntry = refreshPartitionLocation(tableName, tabletId, tableEntry);
            obPartitionLocationInfo = tableEntry.getPartitionEntry().getPartitionInfo(tabletId);
            obPartitionLocationInfo.initializationLatch.await();
        }
        return obPartitionLocationInfo;
    }

    public Map<Long, ObTableParam> getPartIdParamMapForQuery(String tableName, List<String> scanRangeColumns,
                                                              List<ObNewRange> keyRanges) throws Exception {
        Map<Long, ObTableParam> parIdParamMapObTable = new HashMap<Long, ObTableParam>();
        for (ObNewRange keyRange : keyRanges) {
            ObRowKey startKey = keyRange.getStartKey();
            int startKeySize = startKey.getObjs().size();
            ObRowKey endKey = keyRange.getEndKey();
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
            ObBorderFlag borderFlag = keyRange.getBorderFlag();
            List<ObTableParam> paramList = getTablesInternal(tableName, scanRangeColumns, start,
                    borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd(),
                    tableClient.getRoute(false));
            for (ObTableParam param : paramList) {
                parIdParamMapObTable.put(param.getPartId(), param);
            }
        }
        return parIdParamMapObTable;
    }

    /**
     * get addr by partId
     * @param tableName table want to get
     * @param partId tabletId of table (real tablet id in 4.x)
     * @param route ObServer route
     * @return ObTableParam table information for execution
     * @throws Exception exception
     */
    public ObTableParam getTableWithPartId(String tableName, long partId, ObServerRoute route)
                                                                                              throws Exception {
        TableEntry tableEntry = getTableEntry(tableName);
        return getTableInternal(tableName, tableEntry, partId, route);
    }

    /**
     * get addr from table entry by partId
     * @param tableName table want to get
     * @param tableEntry tableEntry
     * @param partId logicId of tablet
     * @param route ObServer route
     * @return ObTableParam table information for execution
     * @throws Exception exception
     */
    private ObTableParam getTableInternal(String tableName, TableEntry tableEntry, long partId,
                                          ObServerRoute route) throws Exception {
        ReplicaLocation replica = null;
        long tabletId = getTabletIdByPartId(tableEntry, partId);
        ObPartitionLocationInfo obPartitionLocationInfo = null;
        obPartitionLocationInfo = getOrRefreshPartitionInfo(tableEntry, tableName, tabletId);
        if (obPartitionLocationInfo.getPartitionLocation() == null) {
            throw new ObTableNotExistException(
                "partition location is null after refresh, table: { " + tableName
                        + " } may not exist");
        }
        replica = getPartitionLocation(obPartitionLocationInfo, route);
        /**
         * Normally, getOrRefreshPartitionInfo makes sure that a thread only continues if it finds the leader
         * during a route refresh. But sometimes, there might not be a leader yet. In this case, the thread
         * is released, and since it can't get the replica, it throws a no master exception.
         */
        if (replica == null && obPartitionLocationInfo.getPartitionLocation().getLeader() == null) {
            RUNTIME.error(LCD.convert("01-00028"), tableEntry.getPartitionEntry(), tableEntry);
            RUNTIME.error(format(
                "partition=%d has no leader partitionEntry=%s original tableEntry=%s", tabletId,
                tableEntry.getPartitionEntry(), tableEntry));
            throw new ObTablePartitionNoMasterException(format(
                "partition=%d has no leader partitionEntry=%s original tableEntry=%s", tabletId,
                tableEntry.getPartitionEntry(), tableEntry));
        }

        if (replica == null) {
            RUNTIME.error("Cannot get replica by tableName: {}, tabletId: {}", tableName, tabletId);
            throw new ObTableGetException("Cannot get replica by tabletId: " + tabletId);
        }
        int retryTimes = 0;
        ObServerAddr addr = replica.getAddr();
        ObTable obTable = tableRoster.getTable(addr);
        while ((obTable == null) && retryTimes < 2) {
            ++retryTimes;
            // need to refresh table roster to ensure the current roster is the latest
            tableClient.syncRefreshMetadata(true);
            // the addr is wrong, need to refresh location
            if (logger.isInfoEnabled()) {
                logger.info("Cannot get ObTable by addr {}, refreshing metadata, tryTimes: {}.", addr, retryTimes);
            }
            // refresh tablet location based on the latest roster, in case that some of the observers have been killed
            // and used the old location
            tableEntry = refreshPartitionLocation(tableName, tabletId, tableEntry);
            obPartitionLocationInfo = getOrRefreshPartitionInfo(tableEntry, tableName, tabletId);
            replica = getPartitionLocation(obPartitionLocationInfo, route);

            if (replica == null) {
                RUNTIME.error("Cannot get replica by tabletId: " + tabletId);
                throw new ObTableGetException("Cannot get replica by tabletId: " + tabletId);
            }
            addr = replica.getAddr();
            obTable = tableRoster.getTable(addr);
        }
        if (obTable == null) {
            RUNTIME.error("cannot get table by addr: " + addr);
            throw new ObTableGetException("obTable is null, addr is: " + addr.getIp() + ":"
                                          + addr.getSvrPort());
        }
        ObTableParam param = createTableParam(obTable, tableEntry, obPartitionLocationInfo, partId,
            tabletId);
        addr.recordAccess();
        return param;
    }

    private ReplicaLocation getPartitionLocation(ObPartitionLocationInfo obPartitionLocationInfo,
                                                 ObServerRoute route) {
        return obPartitionLocationInfo.getPartitionLocation().getReplica(route);
    }

    private ObTableParam createTableParam(ObTable obTable, TableEntry tableEntry,
                                          ObPartitionLocationInfo obPartitionLocationInfo,
                                          long partId, long tabletId) {
        if (tableEntry == null) {
            throw new ObTableUnexpectedException(
                "create table param meets unexpected exception, tableEntry is null");
        }
        ObTableParam param = new ObTableParam(obTable);
        param.setPartId(partId);
        param.setLsId(obPartitionLocationInfo.getTabletLsId());
        param.setTableId(tableEntry.getTableId());
        param.setPartitionId(tabletId);
        return param;
    }

    /*------------------------------------------------------------------------Query Routing------------------------------------------------------------------------*/

    /**
     * For mutation (queryWithFilter)
     * @param tableName table want to get
     * @param scanRangeColumns row key column names
     * @param keyRange row key range
     * @return table params
     * @throws Exception exception
     */
    public ObTableParam getTableParam(String tableName, List<String> scanRangeColumns,
                                      ObNewRange keyRange) throws Exception {
        Map<Long, ObTableParam> tabletIdIdMapObTable = new HashMap<Long, ObTableParam>();
        ObRowKey startKey = keyRange.getStartKey();
        int startKeySize = startKey.getObjs().size();
        ObRowKey endKey = keyRange.getEndKey();
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
        ObBorderFlag borderFlag = keyRange.getBorderFlag();
        List<ObTableParam> paramList = getTablesInternal(tableName, scanRangeColumns, start,
            borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd(),
            tableClient.getRoute(false));
        for (ObTableParam param : paramList) {
            tabletIdIdMapObTable.put(param.getTabletId(), param);
        }
        // for now only support to query single tablet
        if (tabletIdIdMapObTable.size() > 1) {
            throw new ObTablePartitionConsistentException(
                "query and mutate must be a atomic operation");
        } else if (tabletIdIdMapObTable.size() < 1) {
            throw new ObTableException("could not find part id of range");
        }
        ObTableParam ans = null;
        for (Long tabletId : tabletIdIdMapObTable.keySet()) {
            ans = tabletIdIdMapObTable.get(tabletId);
        }
        return ans;
    }

    /**
     * For mutation (queryWithFilter)
     * @param tableName table want to get
     * @param scanRangeColumns row key column names
     * @param keyRanges row key ranges
     * @return table param
     * @throws Exception exception
     */
    public ObTableParam getTableParam(String tableName, List<String> scanRangeColumns,
                                      List<ObNewRange> keyRanges) throws Exception {
        Map<Long, ObTableParam> partIdIdMapObTable = getPartIdParamMapForQuery(
                tableName, scanRangeColumns, keyRanges);
        // for now only support to query single tablet
        if (partIdIdMapObTable.size() > 1) {
            throw new ObTablePartitionConsistentException(
                "query and mutate must be a atomic operation");
        } else if (partIdIdMapObTable.isEmpty()) {
            throw new ObTableException("could not find part id of range");
        }
        ObTableParam ans = null;
        Map.Entry<Long, ObTableParam> entry = partIdIdMapObTable.entrySet().iterator().next();
        ans = entry.getValue();
        return ans;
    }

    /**
     * get TableParams by start-end range in this table
     * @param tableName table want to get
     * @param query query
     * @param start start key
     * @param startInclusive whether include start key
     * @param end end key
     * @param endInclusive whether include end key
     * @return list of table obTableParams
     * @throws Exception exception
     */
    public List<ObTableParam> getTableParams(String tableName, ObTableQuery query, Object[] start,
                                             boolean startInclusive, Object[] end,
                                             boolean endInclusive) throws Exception {
        return getTablesInternal(tableName, query.getScanRangeColumns(), start, startInclusive,
            end, endInclusive, tableClient.getRoute(false));
    }

    private List<ObTableParam> getTablesInternal(String tableName, List<String> scanRangeColumns,
                                                      Object[] start, boolean startInclusive,
                                                      Object[] end, boolean endInclusive,
                                                      ObServerRoute route) throws Exception {
        if (start.length != end.length) {
            throw new IllegalArgumentException("length of start key and end key is not equal");
        }
        // 1. get TableEntry information
        TableEntry tableEntry = getTableEntry(tableName);
        if (scanRangeColumns == null || scanRangeColumns.isEmpty()) {
            Map<String, Integer> rowkeyElement = tableClient.getRowKeyElement(tableName);
            if (rowkeyElement != null) {
                scanRangeColumns = new ArrayList<String>(rowkeyElement.keySet());
            }
        }
        // 2. get replica location
        // partIdWithReplicaList -> List<pair<logicId, replica>>
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
        // <partId, replica location>
        List<ObPair<Long, ReplicaLocation>> partIdWithReplicaList = getPartitionReplica(tableEntry, tableName,
                startRow, startInclusive, endRow, endInclusive, route);

        List<ObTableParam> params = new ArrayList<>();
        for (ObPair<Long, ReplicaLocation> partIdWithReplica : partIdWithReplicaList) {
            long partId = partIdWithReplica.getLeft();
            ObTableParam param = getTableInternal(tableName, tableEntry, partId, route);
            params.add(param);
        }
        return params;
    }

    /**
     * 根据 start-end 获取 partition id 和 addr
     * @param tableEntry
     * @param startRow
     * @param startIncluded
     * @param endRow
     * @param endIncluded
     * @param route
     * @return Pair of tabletId and ReplicaLocation of this tablet
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
        List<Long> partIds = getPartIds(tableEntry, startRow, startIncluded, endRow, endIncluded);

        for (Long partId : partIds) {
            long tabletId = getTabletIdByPartId(tableEntry, partId);
            ObPartitionLocationInfo locationInfo = getOrRefreshPartitionInfo(tableEntry, tableName, tabletId);
            if (locationInfo.getPartitionLocation() == null) {
                throw new ObTableNotExistException("partition location is null after refresh, table: { " + tableName + " } may not exist");
            }
            replicas.add(new ObPair<>(partId, getPartitionLocation(locationInfo, route)));
        }

        return replicas;
    }

    // get partIds for table
    private List<Long> getPartIds(TableEntry tableEntry, Row startRow,
                                                boolean startIncluded, Row endRow,
                                                boolean endIncluded)
                                                                                                 throws Exception {
        if (!tableEntry.isPartitionTable()
                || tableEntry.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_ZERO) {
            List<Long> ans = new ArrayList<>();
            ans.add(0L);
            return ans;
        }
        ObPartitionLevel level = tableEntry.getPartitionInfo().getLevel();
        if (level == ObPartitionLevel.LEVEL_ONE) {
            return tableEntry.getPartitionInfo().getFirstPartDesc()
                .getPartIds(startRow, startIncluded, endRow, endIncluded);
        } else if (level == ObPartitionLevel.LEVEL_TWO) {
            return getPartIdsForLevelTwo(tableEntry, startRow, startIncluded, endRow, endIncluded);
        } else {
            RUNTIME.error("not allowed bigger than level two");
            throw new ObTableGetException("not allowed bigger than level two");
        }
    }

    /*
     * Get logicId from giving range
     */
    private List<Long> getPartIdsForLevelTwo(TableEntry tableEntry, Row startRow,
                                             boolean startIncluded, Row endRow, boolean endIncluded)
                                                                                                    throws Exception {
        if (tableEntry.getPartitionInfo().getLevel() != ObPartitionLevel.LEVEL_TWO) {
            RUNTIME.error("getPartIdsForLevelTwo need ObPartitionLevel LEVEL_TWO");
            throw new Exception("getPartIdsForLevelTwo need ObPartitionLevel LEVEL_TWO");
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

    /*------------------------------------------------------------------------ODP routing------------------------------------------------------------------------*/

    /**
     * get ODP tableEntry by tableName,
     * this methods will guarantee the tableEntry is not null
     * only support by ODP version after 4.3.2
     * */
    public TableEntry getOdpTableEntry(String tableName) throws Exception {
        TableEntry tableEntry;
        tableEntry = odpTableLocations.getTableEntry(tableName);
        if (tableEntry == null) {
            tableEntry = refreshOdpMeta(tableName, false);
        }
        return tableEntry;
    }

    /**
     * get or refresh table meta information in odp mode
     * only support by ODP version after 4.3.2
     * */
    public TableEntry refreshOdpMeta(String tableName, boolean forceRefresh) throws Exception {
        return odpTableLocations.refreshOdpMeta(tableName, forceRefresh, odpInfo.getObTable());
    }

    /**
     * get odp TableParam by tableName and rowkey
     * only support by ODP version after 4.3.2
     * @param tableName tableName
     * @param rowkey row key or partition key names and values
     * @return ObTableParam tableParam
     * */
    public ObTableParam getOdpTableParam(String tableName, Row rowkey) throws Exception {
        TableEntry odpTableEntry = getOdpTableEntry(tableName);
        if (odpTableEntry == null) {
            logger.error("tableEntry is null, tableName: {}", tableName);
            throw new ObTableEntryRefreshException("tableEntry is null, tableName: " + tableName);
        }
        long partId = getPartId(odpTableEntry, rowkey);
        return getOdpTableInternal(odpTableEntry, partId);
    }

    /**
     * get odp table addr by partId
     * only support by ODP version after 4.3.2
     * @param tableName table want to get
     * @param partId tabletId of table (real tablet id in 4.x)
     * @return ObTableParam table information for execution
     * @throws Exception exception
     */
    public ObTableParam getOdpTableWithPartId(String tableName, long partId)
            throws Exception {
        TableEntry tableEntry = getOdpTableEntry(tableName);
        return getOdpTableInternal(tableEntry, partId);
    }

    /**
     * get odp table entry by partId, just get meta information
     * only support by ODP version after 4.3.2
     * @param odpTableEntry odp tableEntry
     * @param partId logicId of tablet
     * @return ObTableParam table information for execution
     */
    private ObTableParam getOdpTableInternal(TableEntry odpTableEntry, long partId) {
        ObTable obTable = odpInfo.getObTable();
        ObTableParam param = new ObTableParam(obTable);
        param.setPartId(partId);
        long tabletId = getTabletIdByPartId(odpTableEntry, partId);
        param.setLsId(odpTableEntry.getPartitionEntry().getLsId(tabletId));
        param.setTableId(odpTableEntry.getTableId());
        // real partition(tablet) id
        param.setPartitionId(tabletId);
        return param;
    }

    /**
     * get odp TableParams by start-end range in this table
     * only support by ODP version after 4.3.2
     * @param tableName table want to get
     * @param query query
     * @param start start key
     * @param startInclusive whether include start key
     * @param end end key
     * @param endInclusive whether include end key
     * @return list of table obTableParams
     * @throws Exception exception
     */
    public List<ObTableParam> getOdpTableParams(String tableName, ObTableQuery query, Object[] start,
                                                boolean startInclusive, Object[] end,
                                                boolean endInclusive) throws Exception {

        return getOdpTablesInternal(tableName, query.getScanRangeColumns(), start,
                startInclusive, end, endInclusive);
    }

    private List<ObTableParam> getOdpTablesInternal(String tableName, List<String> scanRangeColumns,
                                                    Object[] start, boolean startInclusive,
                                                    Object[] end, boolean endInclusive)
            throws Exception {
        if (start.length != end.length) {
            throw new IllegalArgumentException("length of start key and end key is not equal");
        }
        List<ObTableParam> obTableParams = new ArrayList<ObTableParam>();
        TableEntry odpTableEntry = getOdpTableEntry(tableName);

        if (scanRangeColumns == null || scanRangeColumns.isEmpty()) {
            Map<String, Integer> tableEntryRowKeyElement = tableClient.getRowKeyElement(tableName);
            if (tableEntryRowKeyElement != null) {
                scanRangeColumns = new ArrayList<String>(tableEntryRowKeyElement.keySet());
            }
        }
        // 2. get replica location
        // partIdWithReplicaList -> List<pair<logicId, replica>>
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

        List<Long> partIds = getPartIds(odpTableEntry, startRow, startInclusive, endRow,
                endInclusive);
        for (Long partId : partIds) {
            ObTable obTable = odpInfo.getObTable();
            ObTableParam param = new ObTableParam(obTable);
            param.setPartId(partId);
            long tabletId = getTabletIdByPartId(odpTableEntry, partId);
            param.setLsId(odpTableEntry.getPartitionEntry().getLsId(tabletId));
            param.setTableId(odpTableEntry.getTableId());
            // real partition(tablet) id
            param.setPartitionId(tabletId);
            obTableParams.add(param);
        }

        return obTableParams;
    }

    /*------------------------------------------------------------------------Table Group------------------------------------------------------------------------*/

    /**
     * get table name with table group
     * @param tableGroupName table group name
     * @param refresh if refresh or not
     * @return actual table name
     * @throws Exception exception
     */
    public String tryGetTableNameFromTableGroupCache(final String tableGroupName,
                                                     final boolean refresh) throws Exception {
        if (tableGroupCache == null && tableClient.isOdpMode()) {
            throw new FeatureNotSupportedException("not supported yet in odp mode");
        }
        return tableGroupCache.tryGetTableNameFromTableGroupCache(tableGroupName, refresh,
            serverRoster, sysUA);
    }

    /**
     * get table route fail than clear table group message
     * @param tableGroupName table group name that need to delete
     */
    public void eraseTableGroupFromCache(String tableGroupName) {
        tableGroupCache.eraseTableGroupFromCache(tableGroupName);
    }

    public ConcurrentHashMap<String, String> getTableGroupInverted() {
        return tableGroupCache.getTableGroupInverted();
    }

    public ConcurrentHashMap<String, String> getTableGroupCache() {
        return tableGroupCache.getTableGroupCache();
    }
}
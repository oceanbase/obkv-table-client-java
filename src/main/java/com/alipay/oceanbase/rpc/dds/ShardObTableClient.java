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

import com.alipay.common.tracer.util.LoadTestUtil;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.exception.ObShardTableException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.mutation.Append;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Delete;
import com.alipay.oceanbase.rpc.mutation.Increment;
import com.alipay.oceanbase.rpc.mutation.Insert;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.Put;
import com.alipay.oceanbase.rpc.mutation.Replace;
import com.alipay.oceanbase.rpc.mutation.Update;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.groupupdate.ObTableGroupComparator;
import com.alipay.oceanbase.rpc.table.AbstractTable;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.*;
import com.alipay.tracer.biz.util.TracerBizUtil;
import com.alipay.oceanbase.rpc.shard.ShardRule;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
* Ref:
*    http://gitlab.alibaba-inc.com/middleware/tddl5-wiki/wikis/TDDLM
*    http://gitlab.alibaba-inc.com/middleware/tddl5-wiki/wikis/Tddlm-KvClient
*    http://gitlab.alibaba-inc.com/dbtech/X-KV-Wiki/wikis/QuickStart
*
* @author hongwei.yhw
* @since 2018-May-07
*/
public class ShardObTableClient extends AbstractTable {

    private final static Logger         LOGGER                       = TableClientLoggerFactory
                                                                         .getRUNTIMELogger();
    private String                      dbNamePattern;
    private Map<String, ShardRule>      shardRules;

    /** logic table name -> table name pattern */
    private Map<String, NamePattern>    tableNamePatterns            = new ConcurrentHashMap<String, NamePattern>();
    /** db id -> table client */
    private Map<Integer, ObTableClient> clients                      = new ConcurrentHashMap<Integer, ObTableClient>();

    /** warmup table locations */
    private boolean                     warmupTableLocations         = true;
    /** warmup test load table locations */
    private boolean                     warmupTestLoadTableLocations = true;
    /** warmup by ldc uid range */
    private boolean                     warmupByLdcUidRange          = true;
    /** warmup async */
    private boolean                     warmupAsync                  = true;

    /**
    * Init.
    */
    @Override
    public void init() throws Exception {
        final String[] dataSourceList = StringUtil.parseNamePattern(dbNamePattern);

        final DataSourceUtil dataSourceUtil = new DataSourceUtil();
        AsyncExecutor executor = new AsyncExecutor();
        for (int i = 0; i < dataSourceList.length; i++) {
            final int idx = i;
            executor.addTask(new Runnable() {
                /**
                * Run.
                */
                @Override
                public void run() {
                    try {
                        final String dataSource = dataSourceList[idx];

                        Map<String, String> conf = dataSourceUtil.fetchDataSourceInfo(dataSource);
                        ObTableClient obTableClient = new ObTableClient();
                        obTableClient.setDataSourceName(dataSource);
                        obTableClient.setFullUserName(conf.get("username"));
                        String encPassword = conf.get("password");
                        obTableClient.setPassword(String.valueOf(Security.decode(encPassword)));
                        obTableClient.setParamURL(conf.get("jdbcUrl"));
                        obTableClient.setProperties(getProperties());
                        obTableClient.init();

                        clients.put(idx, obTableClient);
                    } catch (Throwable t) {
                        throw new RuntimeException(t);
                    }
                }
            });
        }
        executor.waitComplete();

        for (ShardRule entry : shardRules.values()) {
            tableNamePatterns.put(entry.getLogicTableName(),
                new NamePattern(entry.getTableNamePattern()));
        }

        if (warmupTableLocations) {
            warmup();
        }
    }

    /**
    * Warmup.
    */
    public void warmup() throws Exception {
        // not in zonemode && db size not equals 100
        // should not warmup by idc uid range
        if (warmupByLdcUidRange || clients.size() != 100 || !ZoneUtil.isZoneMode()) {
            warmupByLdcUidRange = false;
            LOGGER.warn("warmupByUidRange set false, clients.size(): " + clients.size()
                        + ", zoneMode: " + ZoneUtil.isZoneMode());
        }

        AsyncExecutor executor = new AsyncExecutor();
        for (final Map.Entry<String, ShardRule> ruleEntry : shardRules.entrySet()) {
            executor.addTask(new Runnable() {
                /**
                * Run.
                */
                @Override
                public void run() {
                    String logicTable = ruleEntry.getKey();
                    NamePattern tbNamePattern = tableNamePatterns.get(logicTable);
                    int dbSize = clients.size();
                    int tbSize = tbNamePattern.getSize();

                    int range = tbSize / dbSize;
                    for (int i = 0; i < dbSize; i++) {
                        if (warmupByLdcUidRange) {
                            // only warmup in current uid range
                            if (!ZoneUtil.isInCurrentUidRange(i)) {
                                LOGGER
                                    .warn(
                                        "warmup dbId: {} ignore it because not in current uid range",
                                        i);
                                continue;
                            }
                        }

                        for (int j = 0; j < range; j++) {
                            int warmupDbId = i;
                            int warmupTbId = i * range + j + tbNamePattern.getMinValue();
                            LOGGER.warn("warmup table location, dbId: {}, tbId: {} start",
                                warmupDbId, warmupTbId);

                            String physicalTableName = tbNamePattern.wrapValue(warmupTbId);
                            Pair pair = calculate(logicTable, warmupDbId, warmupTbId);
                            try {
                                pair.client.getOrRefreshTableEntry(physicalTableName, false, false);
                            } catch (Exception e) {
                                LOGGER.warn("warmup failed, tableName: " + physicalTableName
                                            + ", dbId: " + warmupDbId, e);
                            }
                            if (warmupTestLoadTableLocations) {
                                LOGGER.warn("warmup test load table location, dbId: {}, tbId: {}",
                                    warmupDbId, warmupTbId);
                                try {
                                    pair.client.getOrRefreshTableEntry(physicalTableName + "_T",
                                        false, false);
                                } catch (Exception e) {
                                    LOGGER.warn("warmup failed, tableName: " + physicalTableName
                                                + ", dbId: " + warmupDbId, e);
                                }
                            }
                            LOGGER.warn("warmup table location, dbId: {}, tbId: {} finished",
                                warmupDbId, warmupTbId);
                        }
                    }
                }
            });
        }
        if (warmupAsync) {
            LOGGER.warn("warmup all ShardObTableClient table locations async");
        } else {
            executor.waitComplete();
            LOGGER.warn("warmup all ShardObTableClient table locations finished");
        }

    }

    /**
    * Batch.
    */
    public TableBatchOps batch(String tableName, int dbId, int tableId) throws Exception {
        Pair pair = calculate(tableName, dbId, tableId);
        return pair.client.batch(pair.tableName);
    }

    /**
    * Batch.
    */
    @Override
    public TableBatchOps batch(String tableName) throws Exception {
        throw new IllegalArgumentException(
            "not support batch without shard value in ShardObTableClient");
    }

    /**
    * Query.
    */
    public TableQuery query(String tableName, int dbId, int tableId) throws Exception {
        Pair pair = calculate(tableName, dbId, tableId);
        return pair.client.query(pair.tableName);
    }

    /**
    * Query.
    */
    @Override
    public TableQuery query(String tableName) throws Exception {
        throw new IllegalArgumentException(
            "not support query without shard value in ShardObTableClient");
    }

    // /**
    // *
    // * @param tableName
    // * @return
    // * @throws Exception
    // */
    // @Override
    // public TableQuery queryByBatchV2(String tableName) throws Exception {
    //     throw new IllegalArgumentException(
    //         "not support query without shard value in ShardObTableClient");
    // }

    public TableQuery queryByBatch(String tableName) throws Exception {
        return query(tableName);
    }

    private Pair calculate(String tableName, Object... rowkey) {
        ShardRule shardRule = shardRules.get(tableName);
        if (shardRule == null) {
            throw new ObShardTableException("table rule not exists: " + tableName);
        }

        int dbId = shardRule.applyDatabaseRule(rowkey);
        int tbId = shardRule.applyTableRule(rowkey);
        return calculate(tableName, dbId, tbId);
    }

    private Pair calculate(String tableName, int dbId, int tableId) {
        NamePattern namePattern = tableNamePatterns.get(tableName);
        if (namePattern == null) {
            throw new ObShardTableException("table name pattern not exists: " + tableName);
        }

        ObTableClient tableClient = clients.get(dbId);
        if (tableClient == null) {
            throw new ObShardTableException("database not exists: " + dbId + ", tableName: "
                                            + tableName);
        }

        String physicalTableName = namePattern.wrapValue(tableId);
        // support load test
        if (LoadTestUtil.isLoadTestMode() || TracerBizUtil.isShadowTest()) {
            physicalTableName += "_T";
        }

        return new Pair(physicalTableName, tableClient);
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
        Pair pair = calculate(tableName, rowkeys);
        return pair.client.get(pair.tableName, rowkeys, columns);
    }

    /**
    * Update.
    */
    @Override
    public long update(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                             throws Exception {
        Pair pair = calculate(tableName, rowkeys);
        return pair.client.update(pair.tableName, rowkeys, columns, values);
    }

    /**
    * Delete.
    */
    @Override
    public long delete(String tableName, Object[] rowkeys) throws Exception {
        Pair pair = calculate(tableName, rowkeys);
        return pair.client.delete(pair.tableName, rowkeys);
    }

    /**
    * Insert.
    */
    @Override
    public long insert(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                             throws Exception {
        Pair pair = calculate(tableName, rowkeys);
        return pair.client.insert(pair.tableName, rowkeys, columns, values);
    }

    /**
    * Replace.
    */
    @Override
    public long replace(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                              throws Exception {
        Pair pair = calculate(tableName, rowkeys);
        return pair.client.replace(pair.tableName, rowkeys, columns, values);
    }

    /**
    * Insert or update.
    */
    @Override
    public long insertOrUpdate(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                                     throws Exception {
        Pair pair = calculate(tableName, rowkeys);
        return pair.client.insertOrUpdate(pair.tableName, rowkeys, columns, values);
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
        Pair pair = calculate(tableName, rowkeys);
        return pair.client.increment(pair.tableName, rowkeys, columns, values, withResult);
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
        Pair pair = calculate(tableName, rowkeys);
        return pair.client.append(pair.tableName, rowkeys, columns, values, withResult);
    }

    // @Override
    // @Deprecated
    // public Map<String, Object> groupIncrement(String reqNo, String tableName, Object[] rowkeys,
    //                                           String[] columns, Object[] values,
    //                                           ObTableGroupComparator cmp, boolean withResult)
    //                                                                                         throws Exception {
    //     Pair pair = calculate(tableName, rowkeys);
    //     return pair.client.groupIncrement(reqNo, pair.tableName, rowkeys, columns, values, cmp,
    //         withResult);
    // }

    // @Override
    // @Deprecated
    // public Map<Long, Long> groupIncCheck(String reqNo, String tableName, Object[] rowkeys)
    //                                                                                       throws Exception {
    //     Pair pair = calculate(tableName, rowkeys);
    //     return pair.client.groupIncCheck(reqNo, pair.tableName, rowkeys);
    // }

    // @Override
    // public long queryAndUpdate(TableQuery query, String[] columns, Object[] values)
    //                                                                               throws Exception {
    //     throw new ObTableException("not supported yet");
    // }

    // @Override
    // public long queryAndAppend(TableQuery query, String[] columns, Object[] values)
    //                                                                               throws Exception {
    //     throw new ObTableException("not supported yet");
    // }

    // @Override
    // public long queryAndIncrement(TableQuery query, String[] columns, Object[] values)
    //                                                                                   throws Exception {
    //     throw new ObTableException("not supported yet");
    // }

    // @Override
    // public long queryAndDelete(TableQuery query) throws Exception {
    //     return 0;
    // }

    /**
    * Get db name pattern.
    */
    public String getDbNamePattern() {
        return dbNamePattern;
    }

    /**
    * Set db name pattern.
    */
    public void setDbNamePattern(String dbNamePattern) {
        this.dbNamePattern = dbNamePattern;
    }

    /**
    *
    * @return
    */
    public Map<String, ShardRule> getShardRules() {
        return shardRules;
    }

    /**
    * Set shard rules.
    */
    public void setShardRules(Map<String, ShardRule> shardRules) {
        this.shardRules = shardRules;
    }

    /**
    *
    * @return
    */
    public Map<Integer, ObTableClient> getClients() {
        return clients;
    }

    /**
    * Set clients.
    */
    public void setClients(Map<Integer, ObTableClient> clients) {
        this.clients = clients;
    }

    /**
    *
    * @return
    */
    public Map<String, NamePattern> getTableNamePatterns() {
        return tableNamePatterns;
    }

    /**
    * Set table name patterns.
    */
    public void setTableNamePatterns(Map<String, NamePattern> tableNamePatterns) {
        this.tableNamePatterns = tableNamePatterns;
    }

    /**
    * Is warmup table locations.
    */
    public boolean isWarmupTableLocations() {
        return warmupTableLocations;
    }

    /**
    * Set warmup table locations.
    */
    public void setWarmupTableLocations(boolean warmupTableLocations) {
        this.warmupTableLocations = warmupTableLocations;
    }

    /**
    * Is warmup test load table locations.
    */
    public boolean isWarmupTestLoadTableLocations() {
        return warmupTestLoadTableLocations;
    }

    /**
    * Set warmup test load table locations.
    */
    public void setWarmupTestLoadTableLocations(boolean warmupTestLoadTableLocations) {
        this.warmupTestLoadTableLocations = warmupTestLoadTableLocations;
    }

    /**
    * Is warmup by ldc uid range.
    */
    public boolean isWarmupByLdcUidRange() {
        return warmupByLdcUidRange;
    }

    /**
    * Set warmup by ldc uid range.
    */
    public void setWarmupByLdcUidRange(boolean warmupByLdcUidRange) {
        this.warmupByLdcUidRange = warmupByLdcUidRange;
    }

    /**
    * Is warmup async.
    */
    public boolean isWarmupAsync() {
        return warmupAsync;
    }

    /**
    * Set warmup async.
    */
    public void setWarmupAsync(boolean warmupAsync) {
        this.warmupAsync = warmupAsync;
    }

    class Pair {
        Pair(String tableName, ObTableClient client) {
            this.tableName = tableName;
            this.client = client;
        }

        String        tableName;
        ObTableClient client;
    }

    @Override
    public Update update(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'update'");
    }

    @Override
    public Delete delete(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'delete'");
    }

    @Override
    public Insert insert(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'insert'");
    }

    @Override
    public Replace replace(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'replace'");
    }

    @Override
    public InsertOrUpdate insertOrUpdate(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'insertOrUpdate'");
    }

    @Override
    public Put put(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'put'");
    }

    @Override
    public Increment increment(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'increment'");
    }

    @Override
    public Append append(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'append'");
    }

    @Override
    public BatchOperation batchOperation(String tableName) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'batchOperation'");
    }

    @Override
    public CheckAndInsUp checkAndInsUp(String tableName, ObTableFilter filter,
                                       InsertOrUpdate insUp, boolean checkExists) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndInsUp'");
    }

    @Override
    public CheckAndInsUp checkAndInsUp(String tableName, ObTableFilter filter,
                                       InsertOrUpdate insUp, boolean checkExists,
                                       boolean rollbackWhenCheckFailed) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'checkAndInsUp'");
    }

}

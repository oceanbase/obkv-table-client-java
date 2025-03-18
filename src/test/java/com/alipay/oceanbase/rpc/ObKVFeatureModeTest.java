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

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static org.junit.Assert.assertEquals;

public class ObKVFeatureModeTest {
    private String tableName            = "test$KVModeConfig";
    private String createTableStatement = "CREATE TABLE IF NOT EXISTS `test$KVModeConfig`(\n"
                                          + "    `K` varbinary(256),\n"
                                          + "    `Q` varbinary(256),\n" + "    `T` bigint,\n"
                                          + "    `V` varbinary(1024),\n"
                                          + "    PRIMARY KEY(`K`, `Q`, `T`)\n"
                                          + ") partition by key(`K`) partitions 3;";

    @Before
    public void setup() throws Exception {
        executeSQL(createTableStatement);
    }

    @After
    public void teardown() throws Exception {
        executeSQL("drop table " + tableName);
    }

    private void executeSQL(String sql) throws SQLException {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private void executeSysSQL(String sql) throws SQLException {
        Connection connection = ObTableClientTestUtil.getSysConnection();
        Statement statement = connection.createStatement();
        statement.execute(sql);
    }

    private ObTableClient createHbaseModeClient() throws Exception {
        ObTableClient client = ObTableClientTestUtil.newTestClient();
        client.setRunningMode(ObTableClient.RunningMode.HBASE);
        client.init();
        return client;
    }

    private ObTableClient createNormalClient() throws Exception {
        ObTableClient client = ObTableClientTestUtil.newTestClient();
        client.setRunningMode(ObTableClient.RunningMode.NORMAL);
        client.init();
        return client;
    }

    private void switchDistributeExecute(boolean enable) throws Exception {
        if (enable) {
            executeSysSQL("alter system set _obkv_feature_mode='distributed_execute=on';");
        } else {
            executeSysSQL("alter system set _obkv_feature_mode='distributed_execute=off'");
        }
    }

    private int getSqlAuditResCount(long timestamp, String keyWord, String stmtType)
                                                                                    throws SQLException {
        String tenantName = ObTableClientTestUtil.getTenantName();
        keyWord = String.format("%s_key_%d", keyWord, timestamp);
        String sqlAuditStr = "select count(*) from oceanbase.__all_virtual_sql_audit where query_sql like"
                             + "\"%"
                             + keyWord
                             + "%\""
                             + "and tenant_name="
                             + "\""
                             + tenantName
                             + "\"" + "and stmt_type = \"" + stmtType + "\";";
        Connection connection = ObTableClientTestUtil.getSysConnection();
        Statement statement = connection.createStatement();
        statement.execute(sqlAuditStr);
        int resCnt = 0;
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            resCnt = resultSet.getInt(1);
        }
        assertEquals(false, resultSet.next());
        return resCnt;
    }

    @Test
    public void testTableRequest() throws Exception {
        // 1. distributed_execute = on
        switchDistributeExecute(true);
        ObTableClient client1 = createNormalClient();
        Assert.assertFalse(client1.getServerCapacity().isSupportDistributedExecute());
        long timeStamp = System.currentTimeMillis();
        testBatchInsertUp(client1, timeStamp, false, "table_dist_on");
        Assert.assertEquals(3, getSqlAuditResCount(timeStamp, "table_dist_on", "KV_MULTI_PUT"));
        testQuery(client1, timeStamp, false, "table_dist_on");
        Assert.assertEquals(6, getSqlAuditResCount(timeStamp, "table_dist_on", "KV_QUERY"));

        // 2. distributed_execute = off
        switchDistributeExecute(false);
        Thread.sleep(5000);

        ObTableClient client2 = createNormalClient();
        Assert.assertFalse(client2.getServerCapacity().isSupportDistributedExecute());
        timeStamp = System.currentTimeMillis();
        testBatchInsertUp(client2, timeStamp, false, "table_dist_off");
        Assert.assertEquals(3, getSqlAuditResCount(timeStamp, "table_dist_off", "KV_MULTI_PUT"));
        testQuery(client2, timeStamp, false, "table_dist_on");
        Assert.assertEquals(6, getSqlAuditResCount(timeStamp, "table_dist_on", "KV_QUERY"));

    }

    @Test
    public void testHbaseRequest() throws Exception {
        // 1. distributed_execute = on
        switchDistributeExecute(true);
        ObTableClient client1 = createHbaseModeClient();
        Assert.assertTrue(client1.getServerCapacity().isSupportDistributedExecute());
        long timeStamp = System.currentTimeMillis();
        testBatchInsertUp(client1, timeStamp, true, "hbase_dist_on");
        Assert.assertEquals(1, getSqlAuditResCount(timeStamp, "hbase_dist_on", "KV_MULTI_PUT"));
        testQuery(client1, timeStamp, true, "hbase_dist_on");
        Assert.assertEquals(6, getSqlAuditResCount(timeStamp, "hbase_dist_on", "KV_QUERY"));

        // 2. distributed_execute = off
        switchDistributeExecute(false);
        Thread.sleep(6000);

        ObTableClient client2 = createHbaseModeClient();
        Assert.assertFalse(client2.getServerCapacity().isSupportDistributedExecute());
        long timeStamp2 = System.currentTimeMillis();
        testBatchInsertUp(client2, timeStamp2, true, "hbase_dist_off");
        Assert.assertEquals(3, getSqlAuditResCount(timeStamp2, "hbase_dist_off", "KV_MULTI_PUT"));
        testQuery(client2, timeStamp2, true, "hbase_dist_off");
        Assert.assertEquals(6, getSqlAuditResCount(timeStamp2, "hbase_dist_off", "KV_QUERY"));

    }

    private void testBatchInsertUp(ObTableClient client, long timeStamp, boolean isHkv,
                                   String prefix) throws Exception {
        BatchOperation batchOperation = client.batchOperation(tableName);
        int batchCount = 10;
        if (isHkv) {
            batchOperation.setEntityType(ObTableEntityType.HKV);
        }
        for (int i = 0; i < batchCount; i++) {
            String K = String.format("%s_key_%d_%d", prefix, timeStamp, i);
            String Q = String.format("%s_qualifier_%d", prefix, i);
            String V = String.format("%s_value_%d", prefix, i);
            ObObj T = ObObj.getInstance(timeStamp);
            InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
            insertOrUpdate.setRowKey(row(colVal("K", K.getBytes()), colVal("Q", Q.getBytes()),
                colVal("T", T)));
            insertOrUpdate.addMutateRow(row(colVal("V", V.getBytes())));
            batchOperation.addOperation(insertOrUpdate);
        }
        batchOperation.execute();
    }

    private void testQuery(ObTableClient client, long timestamp, boolean isHkv, String prefix)
                                                                                              throws Exception {
        // scan whole range
        TableQuery tableQuery = client.query(tableName);
        Object[] start = new Object[] { String.format("%s_key_%d_%d", prefix, timestamp, 0),
                ObObj.getMin(), ObObj.getMin() };
        Object[] end = new Object[] { String.format("%s_key_%d", prefix, timestamp, 9),
                ObObj.getMax(), ObObj.getMax() };
        tableQuery.addScanRange(start, end);
        if (isHkv) {
            tableQuery.setEntityType(ObTableEntityType.HKV);
        }
        tableQuery.select("K", "Q", "T", "V");
        // async query
        QueryResultSet result = tableQuery.asyncExecute();
        while (result.next()) {
            Map<String, Object> row = result.getRow();
            Assert.assertEquals(4, row.size());
            System.out.println("row:" + "K:" + row.get("K") + " Q:" + row.get("Q") + " T:"
                               + row.get("T") + " V:" + row.get("V"));
        }

        // sync query
        QueryResultSet result2 = tableQuery.execute();
        while (result2.next()) {
            Map<String, Object> row = result.getRow();
            Assert.assertEquals(4, row.size());
            System.out.println("row:" + "K:" + row.get("K") + " Q:" + row.get("Q") + " T:"
                               + row.get("T") + " V:" + row.get("V"));
        }
    }
}

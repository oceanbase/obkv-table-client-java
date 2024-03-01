/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2022 OceanBase
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

import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.assertEquals;

public class ObTableGlobalIndexTest {
    ObTableClient client;

    @Before
    public void setup() throws Exception {
        setEnableIndexDirectSelect();
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    public void setEnableIndexDirectSelect() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("set global ob_enable_index_direct_select = on");
    }

    public void cleanPartitionLocationTable(String tableName) throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("delete from " + tableName);
    }

    public void checkIndexTableRow(String tableName, int recordCount) throws Exception {
        String sql1 = "select table_name from oceanbase.__all_virtual_table where data_table_id = "
                      + "(select table_id from oceanbase.__all_virtual_table where table_name = '"
                      + tableName + "')";
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql1);
        resultSet.next();
        String indexTableName = resultSet.getString(1);
        Statement statement2 = connection.createStatement();
        ResultSet resultSet2 = statement2.executeQuery("select count(*) from " + indexTableName);
        resultSet2.next();
        Assert.assertEquals(recordCount, resultSet2.getLong(1));
    }

    public int gen_key(int i) {
        int key = i;
        if (i % 3 == 0) {
            key = i;
        } else if (i % 3 == 1) {
            key = i + 100 + 1;
        } else if (i % 3 == 2) {
            key = i + 200 + 2;
        }
        return key;
    }

    @Test
    public void test_insert() throws Exception {
        test_insert("test_global_hash_hash", 20);
        test_insert("test_global_key_key", 20);
        test_insert("test_global_range_range", 20);
    }

    public void test_insert(String tableName, int recordCount) throws Exception {
        try {
            client.addRowKeyElement(tableName, new String[] { "C1" });
            String[] properties_name = { "C2", "C3" };
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.insert(tableName, new Object[] { key }, properties_name,
                    new Object[] { key + 1, ("hello " + key).getBytes() });
                Assert.assertEquals(1, affectRows);
            }
            // check index table row counts
            checkIndexTableRow(tableName, recordCount);

            // get data
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(key + 1, result.get("C2"));
                Assert.assertEquals("hello " + key, result.get("C3"));
            }

        } finally {
            // delete data
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.delete(tableName, new Object[] { key });
                Assert.assertEquals(1, affectRows);
            }
            checkIndexTableRow(tableName, 0);
        }
    }

    @Test
    public void test_update() throws Exception {
        test_update("test_global_hash_hash", 20);
        test_update("test_global_key_key", 20);
        test_update("test_global_range_range", 20);
    }

    public void test_update(String tableName, int recordCount) throws Exception {
        try {
            client.addRowKeyElement(tableName, new String[] { "C1" });
            String[] properties_name = { "C2", "C3" };
            // prepare data
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.insert(tableName, new Object[] { key }, properties_name,
                    new Object[] { key + 1, ("hello " + key).getBytes() });
                Assert.assertEquals(1, affectRows);
            }
            // update global index key
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.update(tableName, new Object[] { key },
                    new String[] { "C2" }, new Object[] { key + 2 });
                Assert.assertEquals(1, affectRows);
                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(key + 2, result.get("C2"));
                Assert.assertEquals("hello " + key, result.get("C3"));
            }
            // update other key
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.update(tableName, new Object[] { key },
                    new String[] { "C3" }, new Object[] { "hi " + key });
                Assert.assertEquals(1, affectRows);
                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(key + 2, result.get("C2"));
                Assert.assertEquals("hi " + key, result.get("C3"));
            }
            checkIndexTableRow(tableName, recordCount);
        } finally {
            // delete data
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.delete(tableName, new Object[] { key });
                Assert.assertEquals(1, affectRows);
            }
            checkIndexTableRow(tableName, 0);
        }
    }

    @Test
    public void test_insert_or_update() throws Exception {
        test_insert_or_update("test_global_hash_hash", 20);
        test_insert_or_update("test_global_key_key", 20);
        test_insert_or_update("test_global_range_range", 20);
    }

    public void test_insert_or_update(String tableName, int recordCount) throws Exception {
        try {
            client.addRowKeyElement(tableName, new String[] { "C1" });
            String[] properties_name = { "C2", "C3" };
            // prepare data: insert
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.insertOrUpdate(tableName, new Object[] { key },
                    properties_name, new Object[] { key + 1, ("hello " + key).getBytes() });
                Assert.assertEquals(1, affectRows);
                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(key + 1, result.get("C2"));
                Assert.assertEquals("hello " + key, result.get("C3"));
            }

            // insert again: update
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.insertOrUpdate(tableName, new Object[] { key },
                    properties_name, new Object[] { key + 2, ("hi " + key).getBytes() });
                Assert.assertEquals(1, affectRows);
                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(key + 2, result.get("C2"));
                Assert.assertEquals("hi " + key, result.get("C3"));
            }

            checkIndexTableRow(tableName, recordCount);
        } finally {
            // delete data
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.delete(tableName, new Object[] { key });
                Assert.assertEquals(1, affectRows);
            }
            checkIndexTableRow(tableName, 0);
        }
    }

    @Test
    public void test_replace() throws Exception {
        test_replace("test_global_hash_hash", 20);
        test_replace("test_global_key_key", 20);
        test_replace("test_global_range_range", 20);
    }

    public void test_replace(String tableName, int recordCount) throws Exception {
        try {
            client.addRowKeyElement(tableName, new String[] { "C1" });
            String[] properties_name = { "C2", "C3" };
            // prepare data: insert
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.replace(tableName, new Object[] { key }, properties_name,
                    new Object[] { key + 1, ("hello " + key).getBytes() });
                Assert.assertEquals(1, affectRows);
                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(key + 1, result.get("C2"));
                Assert.assertEquals("hello " + key, result.get("C3"));
            }

            // insert again: update
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.replace(tableName, new Object[] { key }, properties_name,
                    new Object[] { key + 2, ("hi " + key).getBytes() });
                Assert.assertEquals(2, affectRows);
                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(key + 2, result.get("C2"));
                Assert.assertEquals("hi " + key, result.get("C3"));
            }

            checkIndexTableRow(tableName, recordCount);
        } finally {
            // delete data
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.delete(tableName, new Object[] { key });
                Assert.assertEquals(1, affectRows);
            }
            checkIndexTableRow(tableName, 0);
        }
    }

    @Test
    public void test_increment_append() throws Exception {
        test_increment_append("test_global_hash_hash", 20);
        test_increment_append("test_global_key_key", 20);
        test_increment_append("test_global_range_range", 20);

    }

    public void test_increment_append(String tableName, int recordCount) throws Exception {
        try {
            client.addRowKeyElement(tableName, new String[] { "c1" });
            String[] properties_name = { "c2" };
            // increment without record
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                Map<String, Object> affect_res = client.increment(tableName, new Object[] { key },
                    properties_name, new Object[] { key + 1 }, true);
                Assert.assertEquals(key + 1, affect_res.get("c2"));
                Map<String, Object> get_result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(key + 1, get_result.get("c2"));
            }

            // increment without column value
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                Map<String, Object> affect_res = client.increment(tableName, new Object[] { key },
                    properties_name, new Object[] { key + 1 }, true);
                Assert.assertEquals(2 * key + 2, affect_res.get("c2"));
                Map<String, Object> get_result = client.get(tableName, new Object[] { key },
                    properties_name);
                Assert.assertEquals(2 * key + 2, get_result.get("c2"));
            }

            // append with empty column value
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                Map<String, Object> affect_res = client.append(tableName, new Object[] { key },
                    new String[] { "c3" }, new Object[] { "hi~".getBytes() }, true);
                Assert.assertEquals("hi~", affect_res.get("c3"));
                Map<String, Object> get_result = client.get(tableName, new Object[] { key },
                    new String[] { "c3" });
                Assert.assertEquals("hi~", get_result.get("c3"));
            }
            checkIndexTableRow(tableName, recordCount);

            // append with not empty column value
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                Map<String, Object> affect_res = client.append(tableName, new Object[] { key },
                    new String[] { "c3" }, new Object[] { " hi~".getBytes() }, true);
                Assert.assertEquals("hi~ hi~", affect_res.get("c3"));
                Map<String, Object> get_result = client.get(tableName, new Object[] { key },
                    new String[] { "c3" });
                Assert.assertEquals("hi~ hi~", get_result.get("c3"));
            }
            checkIndexTableRow(tableName, recordCount);
        } finally {
            // delete data
            for (int i = 0; i < recordCount; i++) {
                int key = gen_key(i);
                long affectRows = client.delete(tableName, new Object[] { key });
                Assert.assertEquals(1, affectRows);
            }
            checkIndexTableRow(tableName, 0);
        }
    }

    @Test
    public void test_query_hash_range() throws Exception {
        String tableName = "test_global_hash_range";
        test_query_in_global_index_table(tableName);
    }

    @Test
    public void test_non_partition_index_table() throws Exception {
        test_query_in_global_index_table("test_global_index_no_part");
        test_query_in_global_index_table("test_global_all_no_part");
        test_query_in_global_index_table("test_global_primary_no_part");
    }

    public void test_query_in_global_index_table(String tableName) throws Exception {
        try {
            client.addRowKeyElement(tableName, new String[] { "C1" });
            // prepare data
            int recordCount = 20;
            String[] properties_name = { "C2", "C3" };
            Object[] properties_value = null;
            for (int i = 0; i < recordCount; i++) {
                if (i % 3 == 0) {
                    properties_value = new Object[] { i + 1, i + 2 };
                } else if (i % 3 == 1) {
                    properties_value = new Object[] { i + 100 + 1, i + 100 + 2 };
                } else if (i % 3 == 2) {
                    properties_value = new Object[] { i + 200 + 1, i + 200 + 2 };
                }
                long affectRows = client.insert(tableName, new Object[] { i }, properties_name,
                    properties_value);
                Assert.assertEquals(1, affectRows);
            }

            // query by primary index
            TableQuery query = client.query(tableName);
            query.addScanRange(new Object[] { 0 }, new Object[] { recordCount });

            QueryResultSet resultSet = query.execute();
            int count = 0;
            while (resultSet.next()) {
                Map<String, Object> row = resultSet.getRow();
                int c1 = (int) row.get("C1");
                int c2 = (int) row.get("C2");
                int c3 = (int) row.get("C3");
                if (c1 % 3 == 0) {
                    Assert.assertEquals(c1 + 1, c2);
                    Assert.assertEquals(c1 + 2, c3);
                } else if (c1 % 3 == 1) {
                    Assert.assertEquals(c1 + 100 + 1, c2);
                    Assert.assertEquals(c1 + 100 + 2, c3);
                } else if (c1 % 3 == 2) {
                    Assert.assertEquals(c1 + 200 + 1, c2);
                    Assert.assertEquals(c1 + 200 + 2, c3);
                }
                count++;
            }
            Assert.assertEquals(count, recordCount);

            // query by global index, will lookup primary table
            TableQuery query2 = client.query(tableName).indexName("idx");
            query2.setScanRangeColumns("C2");
            query2.addScanRange(new Object[] { 0 }, new Object[] { recordCount + 200 + 1 });
            QueryResultSet resultSet2 = query2.execute();
            count = 0;
            while (resultSet2.next()) {
                Map<String, Object> row = resultSet2.getRow();
                int c1 = (int) row.get("C1");
                int c2 = (int) row.get("C2");
                int c3 = (int) row.get("C3");
                if (c1 % 3 == 0) {
                    Assert.assertEquals(c1 + 1, c2);
                    Assert.assertEquals(c1 + 2, c3);
                } else if (c1 % 3 == 1) {
                    Assert.assertEquals(c1 + 100 + 1, c2);
                    Assert.assertEquals(c1 + 100 + 2, c3);
                } else if (c1 % 3 == 2) {
                    Assert.assertEquals(c1 + 200 + 1, c2);
                    Assert.assertEquals(c1 + 200 + 2, c3);
                }
                count++;
            }

            // query by global index, without lookup primary table
            TableQuery query3 = client.query(tableName).indexName("idx");
            query3.setScanRangeColumns("C2");
            query3.addScanRange(new Object[] { 0 }, new Object[] { recordCount + 200 + 1 });
            query3.select("C2");
            QueryResultSet resultSet3 = query3.execute();
            Assert.assertEquals(resultSet3.cacheSize(), recordCount);

            // query by local index, will lookup primary table
            TableQuery query4 = client.query(tableName).indexName("idx2");
            query4.setScanRangeColumns("C3");
            query4.addScanRange(new Object[] { 0 }, new Object[] { recordCount + 200 + 2 });
            query4.select("C1", "C2", "C3");
            QueryResultSet resultSet4 = query4.execute();
            Assert.assertEquals(resultSet4.cacheSize(), recordCount);
            count = 0;
            while (resultSet4.next()) {
                Map<String, Object> row = resultSet4.getRow();
                int c1 = (int) row.get("C1");
                int c2 = (int) row.get("C2");
                int c3 = (int) row.get("C3");
                if (c1 % 3 == 0) {
                    Assert.assertEquals(c1 + 1, c2);
                    Assert.assertEquals(c1 + 2, c3);
                } else if (c1 % 3 == 1) {
                    Assert.assertEquals(c1 + 100 + 1, c2);
                    Assert.assertEquals(c1 + 100 + 2, c3);
                } else if (c1 % 3 == 2) {
                    Assert.assertEquals(c1 + 200 + 1, c2);
                    Assert.assertEquals(c1 + 200 + 2, c3);
                }
                count++;
            }

        } finally {
            cleanPartitionLocationTable(tableName);
        }
    }

    @Test
    public void test_query_sync() throws Exception {
        String tableName = "test_global_hash_range";
        test_query_sync(tableName);
    }

    public void test_query_sync(String tableName) throws Exception {
        try {
            client.addRowKeyElement(tableName, new String[] { "C1" });
            // prepare data
            int recordCount = 20;
            String[] properties_name = { "C2", "C3" };
            Object[] properties_value = null;
            for (int i = 0; i < recordCount; i++) {
                if (i % 3 == 0) {
                    properties_value = new Object[] { i + 1, i + 2 };
                } else if (i % 3 == 1) {
                    properties_value = new Object[] { i + 100 + 1, i + 100 + 2 };
                } else if (i % 3 == 2) {
                    properties_value = new Object[] { i + 200 + 1, i + 200 + 2 };
                }
                long affectRows = client.insert(tableName, new Object[] { i }, properties_name,
                    properties_value);
                Assert.assertEquals(1, affectRows);
            }

            // query sync by global index
            TableQuery query = client.queryByBatchV2(tableName).indexName("idx");
            query.setScanRangeColumns("C2");
            query.addScanRange(new Object[] { 0 }, new Object[] { recordCount + 200 + 1 });
            query.select("C1", "C2", "C3");
            query.limit(5);
            query.setBatchSize(2);
            query.setMaxResultSize(10000);
            // 异步query start, 获取第一个batch的结果集
            QueryResultSet result = query.execute();
            for (int i = 0; i < 5; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                int c1 = (int) row.get("C1");
                int c2 = (int) row.get("C2");
                int c3 = (int) row.get("C3");
                if (c1 % 3 == 0) {
                    Assert.assertEquals(c1 + 1, c2);
                    Assert.assertEquals(c1 + 2, c3);
                } else if (c1 % 3 == 1) {
                    Assert.assertEquals(c1 + 100 + 1, c2);
                    Assert.assertEquals(c1 + 100 + 2, c3);
                } else if (c1 % 3 == 2) {
                    Assert.assertEquals(c1 + 200 + 1, c2);
                    Assert.assertEquals(c1 + 200 + 2, c3);
                }
            }
        } finally {
            cleanPartitionLocationTable(tableName);
        }
    }

    /**
     CREATE TABLE `test_ttl_timestamp_with_index` (
     `c1` varchar(20) NOT NULL,
     `c2` bigint NOT NULL,
     `c3` bigint DEFAULT NULL,
     `c4` bigint DEFAULT NULL,
     `expired_ts` timestamp,
     PRIMARY KEY (`c1`, `c2`),
     KEY `idx`(`c1`, `c4`) local,
     KEY `idx2`(`c3`) global partition by hash(`c3`) partitions 4) TTL(expired_ts + INTERVAL 0 SECOND) partition by key(`c1`) partitions 4;
    **/
    @Test
    public void test_ttl_query_with_global_index() throws Exception {
        String tableName = "test_ttl_timestamp_with_index";
        String rowKey1 = "c1";
        String rowKey2 = "c2";
        String intCol = "c3";
        String intCol2 = "c4";
        String expireCol = "expired_ts";
        String prefixKey = "test";
        long[] keyIds = { 1L, 2L };
        try {
            // 1. insert records with null expired_ts
            for (long id : keyIds) {
                client.insert(tableName).setRowKey(colVal(rowKey1, prefixKey), colVal(rowKey2, id))
                    .addMutateColVal(colVal(intCol, id + 100))
                    .addMutateColVal(colVal(intCol2, id + 200))
                    .addMutateColVal(colVal(expireCol, null)).execute();
            }
            // 2. query all inserted records
            QueryResultSet resultSet = client.query(tableName).indexName("idx2")
                .setScanRangeColumns(intCol)
                .addScanRange(new Object[] { 101L }, new Object[] { 102L }).execute();
            Assert.assertEquals(resultSet.cacheSize(), keyIds.length);

            // 3. update the expired_ts
            Timestamp curTs = new Timestamp(System.currentTimeMillis());
            client.update(tableName)
                .setRowKey(colVal(rowKey1, prefixKey), colVal(rowKey2, keyIds[1]))
                .addMutateColVal(colVal(expireCol, curTs)).execute();

            // 3. re-query all inserted records, the expired record won't be returned
            resultSet = client.query(tableName).indexName("idx2").setScanRangeColumns(intCol2)
                .addScanRange(new Object[] { 101L }, new Object[] { 102L }).execute();
            Assert.assertEquals(resultSet.cacheSize(), 1);
            Assert.assertTrue(resultSet.next());
            Row row = resultSet.getResultRow();
            Assert.assertEquals(row.get(rowKey2), keyIds[0]);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long id : keyIds) {
                client.delete(tableName).setRowKey(colVal(rowKey1, prefixKey), colVal(rowKey2, id))
                    .execute();
            }
        }
    }

}

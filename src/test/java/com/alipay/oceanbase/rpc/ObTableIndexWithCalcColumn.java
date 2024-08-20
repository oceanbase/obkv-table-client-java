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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static org.junit.Assert.*;

public class ObTableIndexWithCalcColumn {

    String        CreateTableStatement = "CREATE TABLE `index_has_current_timestamp` (\n"
                                         + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
                                         + "  `adiu` varchar(512) NOT NULL DEFAULT '',\n"
                                         + "  `mode` varchar(512) NOT NULL DEFAULT '',\n"
                                         + "  `time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
                                         + "  `tag` varchar(512) DEFAULT '',\n"
                                         + "  `content` varchar(412) DEFAULT '',\n"
                                         + "  PRIMARY KEY (`id`, `adiu`),\n"
                                         + "  KEY `idx_adiu_time_mode_tag` (`id`, `adiu`, `time`, `mode`) BLOCK_SIZE 16384 LOCAL,\n"
                                         + "  KEY `g_idx_time_tag_mode` (`time`, `tag`, `mode`) BLOCK_SIZE 16384 GLOBAL\n"
                                         + " ) TTL(time + INTERVAL 300 second) partition by key(adiu) partitions 8;";

    String        TableName            = "index_has_current_timestamp";
    Long          TableId;
    String        LocalIndexTableName;
    String        GlobalIndexTableName;
    String        StringFormat         = "%s_%d";
    String[]      AllColumns           = { "id", "adiu", "mode", "time", "tag", "content" };
    int           recordCount          = 10;
    ObTableClient client;

    @Before
    public void setup() throws Exception {
        setEnableIndexDirectSelect();
        createTable();
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        this.client.addRowKeyElement(TableName, new String[] { "id", "adiu" });
    }

    @After
    public void teardown() throws Exception {
        dropTable();
    }

    public void setEnableIndexDirectSelect() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("set global ob_enable_index_direct_select = on");
    }

    public void createTable() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(CreateTableStatement);
        ResultSet rs = statement
            .executeQuery("select table_id from oceanbase.__all_table where table_name = '"
                          + TableName + "'");
        if (rs.next()) {
            TableId = rs.getLong(1);
            LocalIndexTableName = "__idx_" + TableId + "_idx_adiu_time_mode_tag";
            GlobalIndexTableName = "__idx_" + TableId + "_g_idx_time_tag_mode";
        }
        statement.close();
    }

    public void dropTable() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("drop table " + TableName);
    }

    public void deleteTable() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("delete from " + TableName);
    }

    public void removeTTLAttribute() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter table " + TableName + " remove TTL");
    }

    public void addTTLAttribute(int expire_secord) throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter table " + TableName + " TTL (time + INTERVAL " + expire_secord
                          + " SECOND)");
    }

    private void checkIndexData(long count) throws Exception {
        String sql = "select count(1) as cnt from " + LocalIndexTableName;
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            long total = resultSet.getLong(1);
            Assert.assertEquals(count, total);
        } else {
            Assert.fail("there is no data for " + LocalIndexTableName);
        }
        sql = "select count(1) as cnt from " + GlobalIndexTableName;
        resultSet = statement.executeQuery(sql);
        if (resultSet.next()) {
            long total = resultSet.getLong(1);
            Assert.assertEquals(count, total);
        } else {
            Assert.fail("there is no data for " + GlobalIndexTableName);
        }
        statement.close();
    }

    @Test
    public void test_without_ttl_attributes() throws Exception {
        recordCount = 10;
        removeTTLAttribute();
        test_insert();
        test_update();
        test_insert_up();
        test_replace();
        test_delete();
        test_query();
    }

    @Test
    public void test_with_ttl_attribute() throws Exception {
        recordCount = 10;
        // test rows has been not expired
        addTTLAttribute(300);
        test_insert();
        test_update();
        test_insert_up();
        test_replace();
        test_delete();
        test_query();
        // test rows has been expired
        addTTLAttribute(5);
        test_insert_with_expired_row();
        test_query_with_expired_row();
    }

    public void test_insert_with_expired_row() throws Exception {
        try {
            insert("insert", recordCount, true);
            Thread.sleep(5000);
            update("insertOrupdate", recordCount, true);
            checkIndexData(recordCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable();
        }
    }

    public void insert(String op_type, int count, boolean fill_autoinc) throws Exception {
        for (int i = 1; i <= count; i++) {
            String adiu = String.format(StringFormat, "adiu", i);
            String mode = String.format(StringFormat, "mode", i);
            String tag = String.format(StringFormat, "tag", i);
            String content = String.format(StringFormat, "content", i);
            Object autoincObj = fill_autoinc ? Long.valueOf(i) : null;
            Row rowKey = row(colVal("id", autoincObj), colVal("adiu", adiu));
            Row row = row();
            row.add(colVal("mode", mode));
            row.add(colVal("tag", tag));
            row.add(colVal("content", content));
            if ("insert".equalsIgnoreCase(op_type)) {
                client.insert(TableName).setRowKey(rowKey).addMutateRow(row).execute();
            }
            if ("insertOrUpdate".equalsIgnoreCase(op_type)) {
                client.insertOrUpdate(TableName).setRowKey(rowKey).addMutateRow(row).execute();
            }
            if ("replace".equalsIgnoreCase(op_type)) {
                client.replace(TableName).setRowKey(rowKey).addMutateRow(row).execute();
            }
        }
    }

    public void update(String op_type, int count, boolean is_expired) throws Exception {
        for (int i = 1; i <= count; i++) {
            Long id = Long.valueOf(i);
            String adiu = String.format(StringFormat, "adiu", i);
            Map<String, Object> valueMap = client.get(TableName, new Object[] { id, adiu },
                AllColumns);
            Timestamp time1 = new Timestamp(System.currentTimeMillis() + 100000);
            if (is_expired) {
                Assert.assertTrue(valueMap.isEmpty());
            } else {
                Assert.assertEquals(id, valueMap.get("id"));
                Assert.assertEquals(String.format(StringFormat, "adiu", id), valueMap.get("adiu"));
                time1 = (Timestamp) valueMap.get("time");
            }

            // do update
            Row rowKey = row(colVal("id", id), colVal("adiu", adiu));
            Row row = row();
            String update_mode = String.format(StringFormat, "mode_update", i);
            String update_tag = String.format(StringFormat, "mode_tag", i);
            String update_content = String.format(StringFormat, "mode_content", i);
            row.add(colVal("mode", update_mode));
            row.add(colVal("tag", update_tag));
            row.add(colVal("content", update_content));
            if ("update".equalsIgnoreCase(op_type)) {
                client.update(TableName).setRowKey(rowKey).addMutateRow(row).execute();
            }
            if ("insertOrUpdate".equalsIgnoreCase(op_type)) {
                client.insertOrUpdate(TableName).setRowKey(rowKey).addMutateRow(row).execute();
            }
            if ("replace".equalsIgnoreCase(op_type)) {
                client.replace(TableName).setRowKey(rowKey).addMutateRow(row).execute();
            }
            // get again
            Map<String, Object> valueMap_2 = client.get(TableName, new Object[] { id, adiu },
                AllColumns);
            Assert.assertEquals(id, valueMap_2.get("id"));
            Assert.assertEquals(String.format(StringFormat, "adiu", id), valueMap_2.get("adiu"));
            Assert.assertEquals(update_mode, valueMap_2.get("mode"));
            Assert.assertEquals(update_tag, valueMap_2.get("tag"));
            Assert.assertEquals(update_content, valueMap_2.get("content"));
            if (is_expired) {
                Assert.assertTrue(time1.after((Timestamp) valueMap_2.get("time")));
            } else {
                Assert.assertTrue(time1.before((Timestamp) valueMap_2.get("time")));
            }
        }
    }

    public void test_insert() throws Exception {
        try {
            insert("insert", recordCount, false);
            for (int i = 1; i <= recordCount; i++) {
                Long id = Long.valueOf(i);
                String adiu = String.format(StringFormat, "adiu", i);
                Map<String, Object> valueMap = client.get(TableName, new Object[] { id, adiu },
                    AllColumns);
                Assert.assertEquals(id, valueMap.get("id"));
                Assert.assertEquals(String.format(StringFormat, "adiu", id), valueMap.get("adiu"));
            }
            checkIndexData(recordCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable();
        }
    }

    public void test_update() throws Exception {
        try {
            insert("insert", recordCount, true);
            Thread.sleep(1000);
            update("update", recordCount, false);
            checkIndexData(recordCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable();
        }

    }

    public void test_insert_up() throws Exception {
        try {
            insert("insertOrUpdate", recordCount, true);
            Thread.sleep(1000);
            update("insertOrUpdate", recordCount, false);
            checkIndexData(recordCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable();
        }
    }

    public void test_replace() throws Exception {
        try {
            insert("replace", recordCount, true);
            Thread.sleep(1000);
            update("replace", recordCount, false);
            checkIndexData(recordCount);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable();
        }
    }

    public void test_delete() throws Exception {
        try {
            insert("insert", recordCount, true);
            checkIndexData(recordCount);
            for (int i = 1; i <= recordCount; i++) {
                Long id = Long.valueOf(i);
                String adiu = String.format(StringFormat, "adiu", i);
                Map<String, Object> valueMap = client.get(TableName, new Object[] { id, adiu },
                    AllColumns);
                Assert.assertEquals(id, valueMap.get("id"));
                Assert.assertEquals(String.format(StringFormat, "adiu", id), valueMap.get("adiu"));
                Row rowKey = row(colVal("id", id), colVal("adiu", adiu));
                client.delete(TableName).setRowKey(rowKey).execute();
            }
            checkIndexData(0);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable();
        }
    }

    public void test_query() throws Exception {
        try {
            insert("insert", recordCount, true);
            test_query(false);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable();
        }
    }

    public void test_query_with_expired_row() throws Exception {
        try {
            insert("insert", recordCount, true);
            Thread.sleep(5000);
            test_query(true);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable();
        }
    }

    public void test_query(boolean is_expire) throws Exception {
        // query with primary index
        String start_adiu = String.format(StringFormat, "adiu", 1);
        Object[] start = { 0L, start_adiu };
        String end_adiu = String.format(StringFormat, "adiu", recordCount);
        Object[] end = { Long.valueOf(recordCount), end_adiu };
        QueryResultSet resultSet = client.query(TableName).addScanRange(start, end).execute();
        if (is_expire) {
            Assert.assertEquals(resultSet.cacheSize(), 0);
        } else {
            Assert.assertEquals(resultSet.cacheSize(), recordCount);
        }

        // query with local index
        Timestamp start_time1 = new Timestamp(System.currentTimeMillis() - 100000);
        start_time1.setNanos(0);
        String start_adiu1 = String.format(StringFormat, "adiu", 1);
        String start_mode1 = String.format(StringFormat, "mode", 1);
        Object[] start1 = { 0L, start_adiu1, start_time1, start_mode1 };
        Timestamp end_time1 = new Timestamp(System.currentTimeMillis() + 100000);
        end_time1.setNanos(0);
        String end_adiu1 = String.format(StringFormat, "adiu", recordCount);
        String end_mode1 = String.format(StringFormat, "mode", recordCount);
        Object[] end1 = { Long.valueOf(recordCount), end_adiu1, end_time1, end_mode1 };
        QueryResultSet resultSet1 = client.query(TableName).indexName("idx_adiu_time_mode_tag")
            .setScanRangeColumns("id", "time", "tag", "mode").addScanRange(start1, end1).execute();
        if (is_expire) {
            Assert.assertEquals(resultSet1.cacheSize(), 0);
        } else {
            Assert.assertEquals(resultSet1.cacheSize(), recordCount);
        }
        // query with global index
        Timestamp startTime2 = new Timestamp(System.currentTimeMillis() - 100000);
        startTime2.setNanos(0);
        String start_tag = String.format(StringFormat, "tag", 1);
        String start_mode = String.format(StringFormat, "mode", 1);
        Object[] start2 = { startTime2, start_tag, start_mode };
        Timestamp endTime2 = new Timestamp(System.currentTimeMillis() + 100000);
        endTime2.setNanos(0);
        String end_tag = String.format(StringFormat, "tag", recordCount);
        String end_mode = String.format(StringFormat, "mode", recordCount);
        Object[] end2 = { endTime2, end_tag, end_mode };
        QueryResultSet resultSet2 = client.query(TableName).indexName("g_idx_time_tag_mode")
            .setScanRangeColumns("time", "tag", "mode").addScanRange(start2, end2).execute();
        if (is_expire) {
            Assert.assertEquals(resultSet2.cacheSize(), 0);
        } else {
            Assert.assertEquals(resultSet2.cacheSize(), recordCount);
        }
    }

    @Test
    public void testLocalIndexHasGenerateColumn() throws Exception {
        /*
          CREATE TABLE `test_local_index_with_vgen_col` (
              `name` varchar(512) NOT NULL DEFAULT '',
              `pk` varchar(512) NOT NULL,
              `adiu` varchar(512) NOT NULL DEFAULT '',
              `id` bigint(20) NOT NULL DEFAULT 0,
              `name_v` varchar(20) GENERATED ALWAYS AS (substr(`name`,1,5)) VIRTUAL,
              `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`adiu`, `pk`, `gmt_create`),
              KEY `idx_adiu_v_name` (`adiu`, `name_v`) BLOCK_SIZE 16384 LOCAL
          ) TTL (gmt_create + INTERVAL 300 SECOND) partition by key(adiu) partitions 8;
        */
        String TABLE_NAME = "test_local_index_with_vgen_col";
        testIndexHasGenerateColumn(TABLE_NAME);
    }

    @Test
    public void testGlobalIndexHasGenerateColumn() throws Exception {
        /*
            CREATE TABLE `test_global_index_with_vgen_col` (
              `name` varchar(512) NOT NULL DEFAULT '',
              `pk` varchar(512) NOT NULL,
              `adiu` varchar(512) NOT NULL DEFAULT '',
              `id` bigint(20) NOT NULL DEFAULT 0,
              `name_v` varchar(20) GENERATED ALWAYS AS (substr(`name`,1,5)) VIRTUAL,
              `gmt_create` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
              PRIMARY KEY (`adiu`, `pk`, `gmt_create`),
              KEY `idx_adiu_v_name` (`adiu`) global
            ) TTL (gmt_create + INTERVAL 300 SECOND) partition by key(adiu) partitions 8;
        */
        String tableName = "test_global_index_with_vgen_col";
        testIndexHasGenerateColumn(tableName);
    }

    public void testIndexHasGenerateColumn(String tableName) throws SQLException {
        long cur_ts = System.currentTimeMillis();
        Timestamp cur_time = new Timestamp(cur_ts);
        cur_time.setNanos(0);
        Timestamp expire_time = new Timestamp(cur_ts - 301000);
        expire_time.setNanos(0);
        try {
            Row rowKey1 = row(colVal("adiu", "adiu111"), colVal("pk", "pk1"),
                colVal("gmt_create", cur_time));
            Row rowKey2 = row(colVal("adiu", "adiu222"), colVal("pk", "pk2"),
                colVal("gmt_create", expire_time));
            Row mutateRow1 = row(colVal("id", 1L), colVal("name", "name1111111"));
            Row mutateRow2 = row(colVal("id", 2L), colVal("name", "name2222222"));

            // prepare data
            MutationResult res = client.insert(tableName).setRowKey(rowKey1)
                .addMutateRow(mutateRow1).execute();
            assertEquals(1, res.getAffectedRows());
            res = client.insert(tableName).setRowKey(rowKey2).addMutateRow(mutateRow2).execute();
            assertEquals(1, res.getAffectedRows());
            // get
            // not expired row
            Map<String, Object> getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals("adiu111", getRes.get("adiu"));
            assertEquals("pk1", getRes.get("pk"));
            assertEquals(cur_time, getRes.get("gmt_create"));
            assertEquals(1L, getRes.get("id"));
            assertEquals("name1111111", getRes.get("name"));
            assertEquals("name1", getRes.get("name_v"));
            // expired row
            getRes = client.get(tableName, rowKey2.getValues(), null);
            assertTrue(getRes.isEmpty());

            // insertup
            // not expired: update
            res = client.insertOrUpdate(tableName).setRowKey(rowKey1).addMutateRow(mutateRow2)
                .execute();
            assertEquals(1, res.getAffectedRows());
            // expired: delete + insert
            res = client.insertOrUpdate(tableName).setRowKey(rowKey2).addMutateRow(mutateRow1)
                .execute();
            assertEquals(1, res.getAffectedRows());
            // get
            getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals("adiu111", getRes.get("adiu"));
            assertEquals("pk1", getRes.get("pk"));
            assertEquals(cur_time, getRes.get("gmt_create"));
            assertEquals(2L, getRes.get("id"));
            assertEquals("name2222222", getRes.get("name"));
            assertEquals("name2", getRes.get("name_v"));
            // expired row
            getRes = client.get(tableName, rowKey2.getValues(), null);
            assertTrue(getRes.isEmpty());

            // update
            res = client.update(tableName).setRowKey(rowKey1).addMutateRow(mutateRow1).execute();
            assertEquals(1, res.getAffectedRows());
            getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals("adiu111", getRes.get("adiu"));
            assertEquals("pk1", getRes.get("pk"));
            assertEquals(cur_time, getRes.get("gmt_create"));
            assertEquals(1L, getRes.get("id"));
            assertEquals("name1111111", getRes.get("name"));
            assertEquals("name1", getRes.get("name_v"));

            // replace
            res = client.replace(tableName).setRowKey(rowKey1).addMutateRow(mutateRow2).execute();
            assertEquals(2, res.getAffectedRows());
            getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals("adiu111", getRes.get("adiu"));
            assertEquals("pk1", getRes.get("pk"));
            assertEquals(cur_time, getRes.get("gmt_create"));
            assertEquals(2L, getRes.get("id"));
            assertEquals("name2222222", getRes.get("name"));
            assertEquals("name2", getRes.get("name_v"));

            // increment
            Row incrMutateRow = row(colVal("id", 100L));
            res = client.increment(tableName).setRowKey(rowKey1).addMutateRow(incrMutateRow)
                .execute();
            assertEquals(1, res.getAffectedRows());
            getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals("adiu111", getRes.get("adiu"));
            assertEquals("pk1", getRes.get("pk"));
            assertEquals(cur_time, getRes.get("gmt_create"));
            assertEquals(102L, getRes.get("id"));
            assertEquals("name2222222", getRes.get("name"));
            assertEquals("name2", getRes.get("name_v"));

            // append
            Row appendMutateRow = row(colVal("name", "ssssss"));
            res = client.append(tableName).setRowKey(rowKey1).addMutateRow(appendMutateRow)
                .execute();
            assertEquals(1, res.getAffectedRows());
            getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals("adiu111", getRes.get("adiu"));
            assertEquals("pk1", getRes.get("pk"));
            assertEquals(cur_time, getRes.get("gmt_create"));
            assertEquals(102L, getRes.get("id"));
            assertEquals("name2222222ssssss", getRes.get("name"));
            assertEquals("name2", getRes.get("name_v"));

            // delete
            res = client.delete(tableName).setRowKey(rowKey1).execute();
            assertEquals(1, res.getAffectedRows());
            client.delete(tableName).setRowKey(rowKey2).execute();
            assertEquals(1, res.getAffectedRows());
        } catch (Exception ex) {
            ex.printStackTrace();
            assertFalse(true);
        } finally {
            ObTableClientTestUtil.getConnection().createStatement()
                .execute("truncate table " + tableName);
        }
    }

    @Test
    public void test_current_ts() throws Exception {
        String tableName = "test_current_timestamp";
        try {
            long cur_ts = System.currentTimeMillis();
            Timestamp ts_1 = new Timestamp(cur_ts + 1000000);
            ts_1.setNanos(0);
            Timestamp ts_2 = new Timestamp(cur_ts - 1000000);
            ts_2.setNanos(0);
            Row rowKey1 = row(colVal("c1", 1));
            Row mutateRow1 = row(colVal("c2", "c2_val"), colVal("c3", ts_1));
            Row mutateRow2 = row(colVal("c2", "c2_val_1"), colVal("c3", ts_2));

            // insert
            MutationResult res = client.insert(tableName).setRowKey(rowKey1)
                .addMutateRow(mutateRow1).execute();
            assertEquals(1, res.getAffectedRows());
            // get
            Map<String, Object> getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals(1, getRes.get("c1"));
            assertEquals("c2_val", getRes.get("c2"));
            assertEquals(ts_1, getRes.get("c3"));

            // insertup: update
            res = client.insertOrUpdate(tableName).setRowKey(rowKey1).addMutateRow(mutateRow2)
                .execute();
            assertEquals(1, res.getAffectedRows());
            // get
            getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals(1, getRes.get("c1"));
            assertEquals("c2_val_1", getRes.get("c2"));
            assertEquals(ts_2, getRes.get("c3"));

            // replace: update
            res = client.replace(tableName).setRowKey(rowKey1).addMutateRow(mutateRow1).execute();
            assertEquals(2, res.getAffectedRows());
            // get
            getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals(1, getRes.get("c1"));
            assertEquals("c2_val", getRes.get("c2"));
            assertEquals(ts_1, getRes.get("c3"));

            // update
            res = client.update(tableName).setRowKey(rowKey1).addMutateRow(mutateRow2).execute();
            assertEquals(1, res.getAffectedRows());
            // get
            getRes = client.get(tableName, rowKey1.getValues(), null);
            assertTrue(!getRes.isEmpty());
            assertEquals(1, getRes.get("c1"));
            assertEquals("c2_val_1", getRes.get("c2"));
            assertEquals(ts_2, getRes.get("c3"));
        } catch (Exception ex) {
            ex.printStackTrace();
            assertTrue(false);
        } finally {
            ObTableClientTestUtil.getConnection().createStatement()
                .execute("truncate table " + tableName);
        }
    }
}

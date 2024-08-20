/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableFilterList;
import com.alipay.oceanbase.rpc.filter.ObTableValueFilter;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.andList;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;

public class ObTableAuditTest {
    ObTableClient        client;
    public static String tableName = "audit_test";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { "c1" });
    }

    private static void setMinimalImage() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("SET GLOBAL binlog_row_image=MINIMAL");
    }

    private static void setFullImage() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("SET GLOBAL binlog_row_image=Full");
    }

    public static String generateRandomString(int length) {
        String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        StringBuilder sb = new StringBuilder();

        Random random = new Random();
        for (int i = 0; i < length; i++) {
            int index = random.nextInt(characters.length());
            char randomChar = characters.charAt(index);
            sb.append(randomChar);
        }

        return sb.toString();
    }

    public static int generateRandomNumber() {
        Random random = new Random();
        return random.nextInt(64536) + 1000;
    }

    private List<String> get_sql_id(String keyWord) throws SQLException {
        List<String> sqlIds = new ArrayList<>();
        String tenantName = ObTableClientTestUtil.getTenantName();
        Connection conn = ObTableClientTestUtil.getSysConnection();
        PreparedStatement ps = conn.prepareStatement("select sql_id from oceanbase.__all_virtual_sql_audit where query_sql like "
                + "\"%" + keyWord + "%\"" + "and tenant_name=" + "\"" + tenantName + "\"" + "order by request_time asc" + ";");
        ResultSet rs = ps.executeQuery();

        while (rs.next()) {
            String sqlId = rs.getString(1);
            sqlIds.add(sqlId);
        }

        for (String sqlId : sqlIds) {
            System.out.println(sqlId);
        }

        return sqlIds;
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testSingleOperation() throws Exception {
        try {
            String prefix = generateRandomString(10);
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", prefix)).execute();
            List<String> sqlIds = get_sql_id(prefix);
            Assert.assertEquals(1, sqlIds.size());

            // same operation and columns generate same sql_id
            sqlIds.clear();
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", prefix)).execute();
            sqlIds = get_sql_id(prefix);
            Assert.assertEquals(2, sqlIds.size());
            for (String sqlId : sqlIds) {
                Assert.assertEquals(sqlIds.get(0), sqlId);
            }

            // different operation generate different sql_id
            sqlIds.clear();
            client.update(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", prefix)).execute();
            sqlIds = get_sql_id(prefix);
            Assert.assertEquals(3, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));
            Assert.assertNotEquals(sqlIds.get(1), sqlIds.get(2));

            // different columns generate different sql_id
            // write c3
            sqlIds.clear();
            client.update(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c3", prefix)).execute();
            sqlIds = get_sql_id(prefix);
            Assert.assertEquals(4, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));
            Assert.assertNotEquals(sqlIds.get(1), sqlIds.get(2));
            Assert.assertNotEquals(sqlIds.get(2), sqlIds.get(3));
        } finally {
            client.delete(tableName, 1L);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testMultiOperation() throws Exception {
        try {
            String prefix = generateRandomString(10);
            BatchOperation batchOps = client.batchOperation(tableName);
            Insert ins1 = client.insert(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", prefix));
            Insert ins2 = client.insert(tableName).setRowKey(colVal("c1", 2L))
                .addMutateColVal(colVal("c2", prefix));
            batchOps.addOperation(ins1, ins2).execute();
            List<String> sqlIds = get_sql_id(prefix);
            Assert.assertEquals(1, sqlIds.size());

            // same sql_id even id rowkey value is different
            sqlIds.clear();
            batchOps = client.batchOperation(tableName);
            ins1 = client.insert(tableName).setRowKey(colVal("c1", 3L))
                .addMutateColVal(colVal("c2", prefix));
            ins2 = client.insert(tableName).setRowKey(colVal("c1", 4L))
                .addMutateColVal(colVal("c2", prefix));
            batchOps.addOperation(ins1, ins2).execute();
            sqlIds = get_sql_id(prefix);
            Assert.assertEquals(2, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));
        } finally {
            client.delete(tableName, 1L);
            client.delete(tableName, 2L);
            client.delete(tableName, 3L);
            client.delete(tableName, 4L);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testMixedBatchOperation1() throws Exception {
        try {
            String prefix = generateRandomString(10);
            // mixed op has multi sql_id
            BatchOperation batchOps = client.batchOperation(tableName);
            Insert ins = client.insert(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", prefix));
            InsertOrUpdate insUp = client.insertOrUpdate(tableName).setRowKey(colVal("c1", 2L))
                .addMutateColVal(colVal("c2", prefix));
            batchOps.addOperation(ins, insUp).execute();
            List<String> sqlIds = get_sql_id(prefix);
            Assert.assertEquals(2, sqlIds.size());
        } finally {
            client.delete(tableName, 1L);
            client.delete(tableName, 2L);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testMixedBatchOperation2() throws Exception {
        try {
            String prefix = generateRandomString(10);
            // mixed op has multi sql_id
            BatchOperation batchOps = client.batchOperation(tableName);
            Insert ins = client.insert(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", prefix));
            InsertOrUpdate insUp = client.insertOrUpdate(tableName).setRowKey(colVal("c1", 2L))
                .addMutateColVal(colVal("c2", prefix));
            Append appn = client.append(tableName).setRowKey(colVal("c1", 2L))
                .addMutateColVal(colVal("c2", prefix));
            batchOps.addOperation(ins, insUp, appn).execute();
            List<String> sqlIds = get_sql_id(prefix);
            Assert.assertEquals(3, sqlIds.size());
        } finally {
            client.delete(tableName, 1L);
            client.delete(tableName, 2L);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testSyncQuery() throws Exception {
        // query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name $filter
        try {
            // insert
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", "c2_val")).execute();

            // query
            int limit = generateRandomNumber();
            TableQuery tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            tableQuery.execute();
            List<String> sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(1, sqlIds.size());

            // different Range generate same sql_id
            // 0 - 100
            sqlIds.clear();
            tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 100L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            tableQuery.execute();
            sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(2, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));

            // different select columns generate different sql_id
            // select c1, c2
            sqlIds.clear();
            tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1", "c2");
            tableQuery.limit(limit);
            tableQuery.execute();
            sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(3, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));
            Assert.assertNotEquals(sqlIds.get(1), sqlIds.get(2));

            // different index generate different sql_id
            // index idx_c2
            sqlIds.clear();
            tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { "c2" }, new Object[] { "c2_val" });
            tableQuery.setScanRangeColumns("c2");
            tableQuery.select("c1", "c2");
            tableQuery.limit(limit);
            tableQuery.indexName("idx_c2");
            tableQuery.execute();
            sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(4, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));
            Assert.assertNotEquals(sqlIds.get(1), sqlIds.get(2));
            Assert.assertNotEquals(sqlIds.get(2), sqlIds.get(3));
        } finally {
            client.delete(tableName, 1L);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testSyncQueryIndex() throws Exception {
        // query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name $filter
        try {
            // different index generate different sql_id
            int limit = generateRandomNumber();
            client.query(tableName).select("c1").indexName("idx_c2").limit(limit).execute();
            client.query(tableName).select("c1").indexName("idx_c3").limit(limit).execute();
            List<String> sqlIds = get_sql_id("limit:" + limit);
            Assert.assertEquals(2, sqlIds.size());
            Assert.assertNotEquals(sqlIds.get(0), sqlIds.get(1));
        } finally {
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testSyncQueryFilter1() throws Exception {
        // query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name $filter
        try {
            // different filter column generate different sql_id
            int limit = generateRandomNumber();
            ObTableValueFilter filter1 = new ObTableValueFilter(ObCompareOp.EQ, "c2", "hello");
            ObTableValueFilter filter2 = new ObTableValueFilter(ObCompareOp.EQ, "c3", "hello");
            client.query(tableName).select("c1").setFilter(filter1).limit(limit).execute();
            client.query(tableName).select("c1").setFilter(filter2).limit(limit).execute();
            List<String> sqlIds = get_sql_id("limit:" + limit);
            Assert.assertEquals(2, sqlIds.size());
            Assert.assertNotEquals(sqlIds.get(0), sqlIds.get(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testSyncQueryFilter2() throws Exception {
        // query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name $filter
        try {
            int limit = generateRandomNumber();
            ObTableValueFilter filter1 = new ObTableValueFilter(ObCompareOp.EQ, "c2", "hello");
            ObTableValueFilter filter2 = new ObTableValueFilter(ObCompareOp.EQ, "c3", "hello");
            ObTableFilterList list1 = andList(filter1, filter2);
            ObTableFilterList list2 = andList(filter2, filter1);
            client.query(tableName).select("c1").setFilter(list1).limit(limit).execute();
            client.query(tableName).select("c1").setFilter(list2).limit(limit).execute();
            List<String> sqlIds = get_sql_id("limit:" + limit);
            Assert.assertEquals(2, sqlIds.size());
            Assert.assertNotEquals(sqlIds.get(0), sqlIds.get(1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testSyncQueryFilter3() throws Exception {
        // query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name $filter
        try {
            // insert
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", "c2_val")).execute();

            // query
            int limit = generateRandomNumber();
            TableQuery tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            tableQuery.execute();
            List<String> sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(1, sqlIds.size());

            // has filter generate different sql_id
            ObTableValueFilter filter = new ObTableValueFilter(ObCompareOp.GT, "c2", "c2_val");
            sqlIds.clear();
            tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 100L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            tableQuery.setFilter(filter);
            tableQuery.execute();
            sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(2, sqlIds.size());
            Assert.assertNotEquals(sqlIds.get(0), sqlIds.get(1));
        } finally {
            client.delete(tableName, 1L);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testAsyncQuery() throws Exception {
        // query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name $filter
        try {
            // insert
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", "c2_val")).execute();

            // query
            int limit = generateRandomNumber();
            TableQuery tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            tableQuery.asyncExecute();
            List<String> sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(1, sqlIds.size());

            // different Range generate same sql_id
            // 0 - 100
            sqlIds.clear();
            tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 100L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            tableQuery.asyncExecute();
            sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(2, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));

            // different select columns generate different sql_id
            // select c1, c2
            sqlIds.clear();
            tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1", "c2");
            tableQuery.limit(limit);
            tableQuery.asyncExecute();
            sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(3, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));
            Assert.assertNotEquals(sqlIds.get(1), sqlIds.get(2));

            // different index generate different sql_id
            // index idx_c2
            sqlIds.clear();
            tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { "c2" }, new Object[] { "c2_val" });
            tableQuery.setScanRangeColumns("c2");
            tableQuery.select("c1", "c2");
            tableQuery.limit(limit);
            tableQuery.indexName("idx_c2");
            tableQuery.asyncExecute();
            sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(4, sqlIds.size());
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(1));
            Assert.assertNotEquals(sqlIds.get(1), sqlIds.get(2));
            Assert.assertNotEquals(sqlIds.get(2), sqlIds.get(3));
        } finally {
            client.delete(tableName, 1L);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testAsyncQueryFilter() throws Exception {
        // query $table_name $column_0, $column_1, ..., $column_n range:$column_0, $column_1, ..., $column_n index:$index_name $filter
        try {
            // insert
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", "c2_val")).execute();

            // query
            int limit = generateRandomNumber();
            TableQuery tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            tableQuery.execute();
            List<String> sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(1, sqlIds.size());

            // has filter generate different sql_id
            ObTableValueFilter filter = new ObTableValueFilter(ObCompareOp.GT, "c2", "c2_val");
            sqlIds.clear();
            tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 100L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            tableQuery.setFilter(filter);
            tableQuery.execute();
            sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(2, sqlIds.size());
            Assert.assertNotEquals(sqlIds.get(0), sqlIds.get(1));
        } finally {
            client.delete(tableName, 1L);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS  `audit_test` (
     `c1` bigint(20) not null,
     `c2` varchar(50) default 'hello',
     `c3` varchar(50) default 'world',
     KEY `idx_c2` (`c2`) LOCAL,
     KEY `idx_c3` (`c3`) LOCAL,
     primary key (c1));
     **/
    @Test
    public void testQueryAndMutate() throws Exception {
        try {
            // insert
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", "c2_val")).execute();

            int limit = generateRandomNumber();
            TableQuery tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.setScanRangeColumns("c1");
            tableQuery.select("c1");
            tableQuery.limit(limit);
            ObTableQueryAndMutateRequest req = client.obTableQueryAndAppend(tableQuery,
                new String[] { "c2" }, new Object[] { "_append0" }, false);
            client.execute(req);
            List<String> sqlIds = get_sql_id("limit:" + String.valueOf(limit));
            Assert.assertEquals(3, sqlIds.size()); // query twice
            Assert.assertNotEquals(sqlIds.get(0), sqlIds.get(1));
            Assert.assertEquals(sqlIds.get(0), sqlIds.get(2));
        } finally {
            client.delete(tableName, 1L);
        }
    }
}

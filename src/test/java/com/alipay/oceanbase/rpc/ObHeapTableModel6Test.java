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
import com.alipay.oceanbase.rpc.mutation.Update;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.mutation.Delete;
import com.alipay.oceanbase.rpc.mutation.Insert;
import com.alipay.oceanbase.rpc.mutation.Replace;
import com.alipay.oceanbase.rpc.mutation.Append;
import com.alipay.oceanbase.rpc.mutation.Increment;
import com.alipay.oceanbase.rpc.mutation.Put;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.get.Get;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static org.junit.Assert.*;
import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.compareVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

/*
 * 表模型6：普通分区堆表 + 唯一二级索引(c1) + 自增列 + 默认更新时间 + 唯一二级索引(c1, c2) + 生成列 + TTL
 * CREATE TABLE test_heap_model6 (
 * c1 bigint, 
 * c2 varchar(20), 
 * c3 bigint auto_increment, 
 * c4 bigint, 
 * c5 timestamp default current_timestamp on update current_timestamp, 
 * c6 varchar(20),
 * c7 varchar(20) generated always as (SUBSTRING(c2, 2)),
 * UNIQUE KEY idx_unique_c1 (c1),
 * UNIQUE KEY idx_unique_c1_c2 (c1, c2)
 * ) ORGANIZATION = HEAP TTL(c5 + INTERVAL 100 SECOND) PARTITION BY KEY(c1) PARTITIONS 3;
 */
public class ObHeapTableModel6Test {
    ObTableClient        client;
    public static String tableName = "test_heap_model6";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        createTable();
    }

    @After
    public void tearDown() throws Exception {
        dropTable();
    }

    private void createTable() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement
            .execute("CREATE TABLE "
                     + tableName
                     + " ( "
                     + "c1 bigint, "
                     + "c2 varchar(20), "
                     + "c3 bigint auto_increment, "
                     + "c4 bigint, "
                     + "c5 timestamp default current_timestamp on update current_timestamp, "
                     + "c6 varchar(20), "
                     + "c7 varchar(20) generated always as (SUBSTRING(c2, 1, 2)), "
                     + "UNIQUE KEY idx_unique_c1 (c1), "
                     + "UNIQUE KEY idx_unique_c1_c2 (c1, c2) "
                     + ") ORGANIZATION = HEAP TTL(c5 + INTERVAL 100 SECOND) PARTITION BY KEY(c1) PARTITIONS 3");
    }

    private void dropTable() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("DROP TABLE " + tableName);
    }

    private void truncateTable() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("TRUNCATE TABLE " + tableName);
    }

    // ==================== 单行操作测试 ====================

    /**
     * 单行写入测试，插入不同数据
     */
    @Test
    public void testSingleInsert1() throws Exception {
        try {
            // 插入数据
            MutationResult result1 = client
                .insert(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c4", 4001L),
                    colVal("c6", "extra1")).execute();

            assertEquals("Insert should affect 1 row", 1, result1.getAffectedRows());
            System.out.println("Single insert successful: " + result1 + " row affected");

            // 插入数据
            MutationResult result2 = client
                .insert(tableName)
                .setPartitionKey(row(colVal("c1", 1002L)))
                .addMutateColVal(colVal("c1", 1002L), colVal("c2", "test2"), colVal("c4", 4002L),
                    colVal("c6", "extra2")).execute();

            assertEquals("Insert should affect 1 row", 1, result2.getAffectedRows());
            System.out.println("Single insert successful: " + result2 + " row affected");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax())
                .select("c1", "c2", "c3", "c4", "c5", "c6", "c7");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                Map<String, Object> valueMap = res.getRow();
                long c1 = (Long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                Long c3 = (Long) valueMap.get("c3");
                Long c4 = (Long) valueMap.get("c4");
                Object c5 = valueMap.get("c5");
                String c6 = (String) valueMap.get("c6");
                String c7 = (String) valueMap.get("c7");

                // 校验c1在预期范围内
                assertTrue("c1 should be in range [1001, 1002]", c1 >= 1001L && c1 <= 1002L);
                // 校验c2以"test"开头
                assertTrue("c2 should start with 'test'", c2.startsWith("test"));
                // 校验c3是自增列，应该不为null
                assertNotNull("c3 should be auto-generated", c3);
                // 校验c4在预期范围内
                assertTrue("c4 should be in range [4001, 4002]", c4 >= 4001L && c4 <= 4002L);
                // 校验c5有默认时间戳，应该不为null
                assertNotNull("c5 should have default timestamp", c5);
                // 校验c6以"extra"开头
                assertTrue("c6 should start with 'extra'", c6.startsWith("extra"));
                // 校验c7应该匹配
                assertEquals("c7 should match", "te", c7);
                count++;
            }
            assertEquals("Should have 2 records", 2, count);
        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 单行写入测试，插入相同数据
     */
    @Test
    public void testSingleInsert2() throws Exception {
        try {
            // 插入数据
            MutationResult result1 = client.insert(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c3", 2001L))
                .execute();
            
            assertEquals("Insert should affect 1 row", 1, result1.getAffectedRows());   
            System.out.println("Single insert successful: " + result1 + " row affected");
            
            ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    // 插入相同数据
                    client.insert(tableName)
                    .setPartitionKey(row(colVal("c1", 1001L)))
                    .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c3", 2001L))
                    .execute();
                }
            );
    
            System.out.println(thrown.getMessage());
            assertTrue(thrown.getMessage().contains("[-5024][OB_ERR_PRIMARY_KEY_DUPLICATE]"));
        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 单行写入测试，插入不同数据
     */
    @Test
    public void testSingleInsertOrUpdate1() throws Exception {
        try {
            // 插入数据
            MutationResult result1 = client
                .insertOrUpdate(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c4", 4001L),
                    colVal("c6", "extra1")).execute();

            assertEquals("Insert should affect 1 row", 1, result1.getAffectedRows());
            System.out.println("Single insertOrUpdate successful: " + result1 + " row affected");

            // 插入数据
            MutationResult result2 = client
                .insertOrUpdate(tableName)
                .setPartitionKey(row(colVal("c1", 1002L)))
                .addMutateColVal(colVal("c1", 1002L), colVal("c2", "test2"), colVal("c4", 4002L),
                    colVal("c6", "extra2")).execute();

            assertEquals("Insert should affect 1 row", 1, result2.getAffectedRows());
            System.out.println("Single insertOrUpdate successful: " + result2 + " row affected");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax())
                .select("c1", "c2", "c3", "c4", "c5", "c6", "c7");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                Map<String, Object> valueMap = res.getRow();
                long c1 = (Long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                Long c3 = (Long) valueMap.get("c3");
                Long c4 = (Long) valueMap.get("c4");
                Object c5 = valueMap.get("c5");
                String c6 = (String) valueMap.get("c6");
                String c7 = (String) valueMap.get("c7");
                // 校验c1在预期范围内
                assertTrue("c1 should be in range [1001, 1002]", c1 >= 1001L && c1 <= 1002L);
                // 校验c2以"test"开头
                assertTrue("c2 should start with 'test'", c2.startsWith("test"));
                // 校验c3是自增列，应该不为null
                assertNotNull("c3 should be auto-generated", c3);
                // 校验c4在预期范围内
                assertTrue("c4 should be in range [4001, 4002]", c4 >= 4001L && c4 <= 4002L);
                // 校验c5有默认时间戳，应该不为null
                assertNotNull("c5 should have default timestamp", c5);
                // 校验c6以"extra"开头
                assertTrue("c6 should start with 'extra'", c6.startsWith("extra"));
                // 校验c7应该匹配
                assertEquals("c7 should match", "te", c7);
                count++;
            }
            assertEquals("Should have 2 records", 2, count);
        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 单行写入测试，插入数据冲突，转为更新
     */
    @Test
    public void testSingleInsertOrUpdate2() throws Exception {
        try {
            // 插入数据
            MutationResult result1 = client.insertOrUpdate(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c3", 2001L))
                .execute();

            assertEquals("Insert should affect 1 row", 1, result1.getAffectedRows());
            System.out.println("Single insertOrUpdate successful: " + result1 + " row affected");

            // c1 冲突，转为更新，c2 c3 的值被更新
            MutationResult result2 = client.insertOrUpdate(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test2"), colVal("c3", 2002L))
                .execute();

            assertEquals("Insert should affect 1 row", 1, result2.getAffectedRows());
            System.out.println("Single insertOrUpdate successful: " + result2 + " row affected");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax()).select("c1", "c2", "c3");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                Map<String, Object> valueMap = res.getRow();
                assertEquals("c1 should match", 1001L, valueMap.get("c1"));
                assertEquals("c2 should match", "test2", valueMap.get("c2"));
                assertEquals("c3 should match", 2002L, valueMap.get("c3"));
                count++;
            }
            assertEquals("Should have 2 records", 1, count);

            // sql 验证
            String sql = "select /*+ opt_param('hidden_column_visible','true') */ __pk_increment, c1, c2, c3 from "
                         + tableName + " where c1 = 1001";
            Connection connection = ObTableClientTestUtil.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            while (resultSet.next()) {
                assertEquals("__pk_increment should match", 1, resultSet.getLong("__pk_increment"));
                assertEquals("c1 should match", 1001L, resultSet.getLong("c1"));
                assertEquals("c2 should match", "test2", resultSet.getString("c2"));
                assertEquals("c3 should match", 2002L, resultSet.getLong("c3"));
            }
        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 单行写入测试，插入不同数据
     */
    @Test
    public void testSingleReplace1() throws Exception {
        try {
            // 插入数据
            MutationResult result1 = client
                .replace(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c4", 4001L),
                    colVal("c6", "extra1")).execute();

            assertEquals("Insert should affect 1 row", 1, result1.getAffectedRows());
            System.out.println("Single replace successful: " + result1 + " row affected");

            // 插入数据
            MutationResult result2 = client
                .replace(tableName)
                .setPartitionKey(row(colVal("c1", 1002L)))
                .addMutateColVal(colVal("c1", 1002L), colVal("c2", "test2"), colVal("c4", 4002L),
                    colVal("c6", "extra2")).execute();

            assertEquals("Insert should affect 1 row", 1, result2.getAffectedRows());
            System.out.println("Single replace successful: " + result2 + " row affected");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax())
                .select("c1", "c2", "c3", "c4", "c5", "c6", "c7");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                Map<String, Object> valueMap = res.getRow();
                long c1 = (Long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                Long c3 = (Long) valueMap.get("c3");
                Long c4 = (Long) valueMap.get("c4");
                Object c5 = valueMap.get("c5");
                String c6 = (String) valueMap.get("c6");
                String c7 = (String) valueMap.get("c7");
                // 校验c1在预期范围内
                assertTrue("c1 should be in range [1001, 1002]", c1 >= 1001L && c1 <= 1002L);
                // 校验c2以"test"开头
                assertTrue("c2 should start with 'test'", c2.startsWith("test"));
                // 校验c3是自增列，应该不为null
                assertNotNull("c3 should be auto-generated", c3);
                // 校验c4在预期范围内
                assertTrue("c4 should be in range [4001, 4002]", c4 >= 4001L && c4 <= 4002L);
                // 校验c5有默认时间戳，应该不为null
                assertNotNull("c5 should have default timestamp", c5);
                // 校验c6以"extra"开头
                assertTrue("c6 should start with 'extra'", c6.startsWith("extra"));
                // 校验c7应该匹配
                assertEquals("c7 should match", "te", c7);
                count++;
            }
            assertEquals("Should have 2 records", 2, count);
        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 单行写入测试，插入相同数据
     */
    @Test
    public void testSingleReplace2() throws Exception {
        try {
            // 插入数据
            MutationResult result1 = client
                .replace(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c4", 2001L),
                    colVal("c6", "test1")).execute();

            assertEquals("Insert should affect 1 row", 1, result1.getAffectedRows());
            System.out.println("Single replace successful: " + result1 + " row affected");

            // 插入数据
            MutationResult result2 = client
                .replace(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c4", 2001L),
                    colVal("c6", "test1")).execute();

            assertEquals("Insert should affect 2 row", 2, result2.getAffectedRows());
            System.out.println("Single replace successful: " + result2 + " row affected");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax())
                .select("c1", "c2", "c3", "c4", "c5", "c6", "c7");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                Map<String, Object> valueMap = res.getRow();
                long c1 = (Long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                Long c3 = (Long) valueMap.get("c3");
                Long c4 = (Long) valueMap.get("c4");
                Object c5 = valueMap.get("c5");
                String c6 = (String) valueMap.get("c6");
                String c7 = (String) valueMap.get("c7");
                // 校验c1应该匹配
                assertEquals("c1 should match", 1001L, c1);
                // 校验c2应该匹配
                assertEquals("c2 should match", "test1", c2);
                // 校验c3是自增列，应该不为null
                assertNotNull("c3 should be auto-generated", c3);
                // 校验c4应该匹配
                assertEquals("c4 should match", Long.valueOf(2001L), c4);
                // 校验c5有默认时间戳，应该不为null
                assertNotNull("c5 should have default timestamp", c5);
                // 校验c6应该匹配
                assertEquals("c6 should match", "test1", c6);
                // 校验c7应该匹配
                assertEquals("c7 should match", "te", c7);
                count++;
            }
            assertEquals("Should have 1 records", 1, count);
        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 单行读取测试
     */
    @Test
    public void testSingleGet() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.get(tableName)
                    .setRowKey(row(colVal("c1", 1001L)))
                    .select("c1", "c2")
                    .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use get not supported]"));
    }

    /**
     * 单行覆盖写入测试
     */
    @Test
    public void testSinglePut() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.put(tableName)
                    .setPartitionKey(row(colVal("c1", 1001L)))
                    .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c3", 2001L))
                    .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use put not supported]"));
    }

    /**
     * 单行Increment测试
     */
    @Test
    public void testSingleIncrement() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.increment(tableName)
                    .setPartitionKey(row(colVal("c1", 1001L)))
                    .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c3", 2001L))
                    .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use increment not supported]"));
    }

    /**
     * 单行Append测试
     */
    @Test
    public void testSingleAppend() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.append(tableName)
                    .setPartitionKey(row(colVal("c1", 1001L)))
                    .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c3", 2001L))
                    .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use append not supported]"));
    }

    /**
     * 单行更新测试
     */
    @Test
    public void testSingleUpdate() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.update(tableName)
                    .setPartitionKey(row(colVal("c1", 1001L)))
                    .addMutateColVal(colVal("c1", 1002L), colVal("c2", "test2"), colVal("c3", 2002L))
                    .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use update not supported]"));
    }

    /**
     * 单行删除测试
     */
    @Test
    public void testSingleDelete() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.delete(tableName)
                    .setPartitionKey(row(colVal("c1", 1001L)))
                    .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use delete not supported]"));
    }

    /**
     * 单行CheckAndInsUp测试
     */
    @Test
    public void testSingleCheckAndInsUp() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    InsertOrUpdate insUp = client.insertOrUpdate(tableName)
                    .setPartitionKey(row(colVal("c1", 1001L)))
                    .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c3", 2001L));
                    ObTableFilter filter = compareVal(ObCompareOp.GE, "c1", 1001L);
                    client.checkAndInsUp(tableName, filter, insUp, true, true).execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use checkAndInsUp not supported]"));
    }

    /**
     * 扫描测试,同步查询
     */
    @Test
    public void testScan1() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Insert insert = client.insert(tableName);
                insert.setPartitionKey(row(colVal("c1", (long) (2000 + i)))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test"),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(insert);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 指定select column
            TableQuery query1 = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax()).select("c1", "c2", "c3");
            QueryResultSet res1 = query1.execute();
            int count1 = 0;
            while (res1.next()) {
                count1++;
                Map<String, Object> valueMap = res1.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count1);

            // 不指定select column
            TableQuery query2 = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax());
            QueryResultSet res2 = query2.execute();
            int count2 = 0;
            while (res2.next()) {
                count2++;
                Map<String, Object> valueMap = res2.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count2);

        } finally {
            truncateTable();
        }
    }

    /**
     * 扫描测试,异步查询
     */
    @Test
    public void testScan2() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Insert insert = client.insert(tableName);
                insert.setPartitionKey(row(colVal("c1", (long) (2000 + i)))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test"),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(insert);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 指定select column
            TableQuery query1 = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax()).select("c1", "c2", "c3");
            QueryResultSet res1 = query1.asyncExecute();
            int count1 = 0;
            while (res1.next()) {
                count1++;
                Map<String, Object> valueMap = res1.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count1);

            // 不指定select column
            TableQuery query2 = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax());
            QueryResultSet res2 = query2.asyncExecute();
            int count2 = 0;
            while (res2.next()) {
                count2++;
                Map<String, Object> valueMap = res2.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count2);

        } finally {
            truncateTable();
        }
    }

    /**
     * 扫描测试,同步查询,指定索引 idx_unique_c1
     */
    @Test
    public void testScan3() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Insert insert = client.insert(tableName);
                insert.setPartitionKey(row(colVal("c1", (long) (2000 + i)))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test"),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(insert);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 指定select column
            TableQuery query1 = client.query(tableName).addScanRange(2000L, 2003L)
                .setScanRangeColumns("c1").indexName("idx_unique_c1").select("c1", "c2", "c3");
            QueryResultSet res1 = query1.execute();
            int count1 = 0;
            while (res1.next()) {
                count1++;
                Map<String, Object> valueMap = res1.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count1);

            // 不指定select column
            TableQuery query2 = client.query(tableName).addScanRange(2000L, 2003L)
                .setScanRangeColumns("c1").indexName("idx_unique_c1");
            QueryResultSet res2 = query2.execute();
            int count2 = 0;
            while (res2.next()) {
                count2++;
                Map<String, Object> valueMap = res2.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count2);

        } finally {
            truncateTable();
        }
    }

    /**
     * 扫描测试,异步查询,指定索引 idx_unique_c1
     */
    @Test
    public void testScan4() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Insert insert = client.insert(tableName);
                insert.setPartitionKey(row(colVal("c1", (long) (2000 + i)))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test"),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(insert);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 指定select column
            TableQuery query1 = client.query(tableName).addScanRange(2000L, 2003L)
                .indexName("idx_unique_c1").setScanRangeColumns("c1").select("c1", "c2", "c3");
            QueryResultSet res1 = query1.asyncExecute();
            int count1 = 0;
            while (res1.next()) {
                count1++;
                Map<String, Object> valueMap = res1.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count1);

            // 不指定select column
            TableQuery query2 = client.query(tableName).addScanRange(2000L, 2003L)
                .indexName("idx_unique_c1").setScanRangeColumns("c1");
            QueryResultSet res2 = query2.asyncExecute();
            int count2 = 0;
            while (res2.next()) {
                count2++;
                Map<String, Object> valueMap = res2.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count2);

        } finally {
            truncateTable();
        }
    }

    /**
     * 扫描测试,同步查询,指定索引 idx_unique_c1_c2
     */
    @Test
    public void testScan5() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Insert insert = client.insert(tableName);
                insert.setPartitionKey(row(colVal("c1", (long) (2000 + i)))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test"),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(insert);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 指定select column
            TableQuery query1 = client
                .query(tableName)
                .addScanRange(new Object[] { 2000L, "batch_test" },
                    new Object[] { 2003L, "batch_test" }).setScanRangeColumns("c1", "c2")
                .indexName("idx_unique_c1_c2").select("c1", "c2", "c3");
            QueryResultSet res1 = query1.execute();
            int count1 = 0;
            while (res1.next()) {
                count1++;
                Map<String, Object> valueMap = res1.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count1);

            // 不指定select column
            TableQuery query2 = client
                .query(tableName)
                .addScanRange(new Object[] { 2000L, "batch_test" },
                    new Object[] { 2003L, "batch_test" }).setScanRangeColumns("c1", "c2")
                .indexName("idx_unique_c1_c2");
            QueryResultSet res2 = query2.execute();
            int count2 = 0;
            while (res2.next()) {
                count2++;
                Map<String, Object> valueMap = res2.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count2);

        } finally {
            truncateTable();
        }
    }

    /**
     * 扫描测试,异步查询,指定索引 idx_unique_c1_c2
     */
    @Test
    public void testScan6() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Insert insert = client.insert(tableName);
                insert.setPartitionKey(row(colVal("c1", (long) (2000 + i)))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test"),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(insert);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 指定select column
            TableQuery query1 = client
                .query(tableName)
                .addScanRange(new Object[] { 2000L, "batch_test" },
                    new Object[] { 2003L, "batch_test" }).indexName("idx_unique_c1_c2")
                .setScanRangeColumns("c1", "c2").select("c1", "c2", "c3");
            QueryResultSet res1 = query1.asyncExecute();
            int count1 = 0;
            while (res1.next()) {
                count1++;
                Map<String, Object> valueMap = res1.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count1);

            // 不指定select column
            TableQuery query2 = client
                .query(tableName)
                .addScanRange(new Object[] { 2000L, "batch_test" },
                    new Object[] { 2003L, "batch_test" }).indexName("idx_unique_c1_c2")
                .setScanRangeColumns("c1", "c2");
            QueryResultSet res2 = query2.asyncExecute();
            int count2 = 0;
            while (res2.next()) {
                count2++;
                Map<String, Object> valueMap = res2.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test"), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count2);

        } finally {
            truncateTable();
        }
    }

    // ==================== 批量操作测试 ====================

    /**
     * 批量写入测试，插入不同数据
     */
    @Test
    public void testBatchInsert1() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Insert insert = client.insert(tableName);
                insert.setPartitionKey(row(colVal("c1", 1001L))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test" + i),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(insert);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax()).select("c1", "c2", "c3");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                count++;
                Map<String, Object> valueMap = res.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test" + count), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count);

        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 批量写入测试，插入相同数据
     */
    @Test
    public void testBatchInsert2() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batch = client.batchOperation(tableName);
                    // 添加多个插入操作
                    for (int i = 1; i <= 3; i++) {
                        Insert insert = client.insert(tableName);
                        insert.setPartitionKey(row(colVal("c1", 1001L)))
                            .addMutateColVal(colVal("c1", 1001L), colVal("c2", "batch_test1"), colVal("c3", 2001L));
                        batch.addOperation(insert);
                    }
                    batch.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-5024][OB_ERR_PRIMARY_KEY_DUPLICATE]"));
    }

    /**
     * 批量写入测试，插入不同数据
     */
    @Test
    public void testBatchInsertOrUpdate1() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                InsertOrUpdate insertOrUpdate = client.insertOrUpdate(tableName);
                insertOrUpdate.setPartitionKey(row(colVal("c1", 1001L))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test" + i),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(insertOrUpdate);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax()).select("c1", "c2", "c3");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                count++;
                Map<String, Object> valueMap = res.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test" + count), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count);

        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 批量写入测试，插入相同数据
     */
    @Test
    public void testBatchInsertOrUpdate2() throws Exception {
        BatchOperation batch = client.batchOperation(tableName);
        // 添加多个插入操作
        for (int i = 1; i <= 3; i++) {
            InsertOrUpdate insUp = client.insertOrUpdate(tableName);
            insUp.setPartitionKey(row(colVal("c1", 1001L))).addMutateColVal(colVal("c1", 1001L),
                colVal("c2", "batch_test1"), colVal("c3", 2001L));
            batch.addOperation(insUp);
        }
        batch.execute();

        System.out.println("Batch insert successful: 3 operations executed");

        // 验证插入结果
        TableQuery tableQuery = client.query(tableName)
            .addScanRange(ObObj.getMin(), ObObj.getMax()).select("c1", "c2", "c3");
        QueryResultSet res = tableQuery.execute();
        int count = 0;
        while (res.next()) {
            count++;
            Map<String, Object> valueMap = res.getRow();
            long c1 = (long) valueMap.get("c1");
            String c2 = (String) valueMap.get("c2");
            long c3 = (long) valueMap.get("c3");
            assertEquals("c1 should match", c1 == 1001L, true);
            assertEquals("c2 should match", c2.equals("batch_test1"), true);
            assertEquals("c3 should match", c3 == 2001L, true);
        }
        assertEquals("Should have 1 records", 1, count);

        // sql 验证
        String sql = "select /*+ opt_param('hidden_column_visible','true') */ __pk_increment, c1, c2, c3 from "
                     + tableName + " where c1 = 1001";
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(sql);
        while (resultSet.next()) {
            assertEquals("__pk_increment should match", 1, resultSet.getLong("__pk_increment"));
            assertEquals("c1 should match", 1001L, resultSet.getLong("c1"));
            assertEquals("c2 should match", "batch_test1", resultSet.getString("c2"));
            assertEquals("c3 should match", 2001L, resultSet.getLong("c3"));
        }
    }

    /**
     * 批量写入测试，插入不同数据
     */
    @Test
    public void testBatchReplace1() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Replace replace = client.replace(tableName);
                replace.setPartitionKey(row(colVal("c1", 1001L))).addMutateColVal(
                    colVal("c1", (long) (2000 + i)), colVal("c2", "batch_test" + i),
                    colVal("c3", (long) (3000 + i)));
                batch.addOperation(replace);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax()).select("c1", "c2", "c3");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                count++;
                Map<String, Object> valueMap = res.getRow();
                long c1 = (long) valueMap.get("c1");
                String c2 = (String) valueMap.get("c2");
                long c3 = (long) valueMap.get("c3");
                assertEquals("c1 should match", c1 >= 2000 && c1 <= 2003, true);
                assertEquals("c2 should match", c2.equals("batch_test" + count), true);
                assertEquals("c3 should match", c3 >= 3000 && c3 <= 3003, true);
            }
            assertEquals("Should have 3 records", 3, count);

        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 批量写入测试，插入相同数据
     */
    @Test
    public void testBatchReplace2() throws Exception {
        try {
            // 使用BatchOperation进行批量插入
            BatchOperation batch = client.batchOperation(tableName);

            // 添加多个插入操作
            for (int i = 1; i <= 3; i++) {
                Replace replace = client.replace(tableName);
                replace.setPartitionKey(row(colVal("c1", 1001L))).addMutateColVal(colVal("c1", 1001L),
                    colVal("c2", "batch_test1"), colVal("c3", 2001L));
                batch.addOperation(replace);
            }

            batch.execute();
            System.out.println("Batch insert successful: 3 operations executed");

            // 验证插入结果
            TableQuery tableQuery = client.query(tableName)
                .addScanRange(ObObj.getMin(), ObObj.getMax()).select("c1", "c2", "c3");
            QueryResultSet res = tableQuery.execute();
            int count = 0;
            while (res.next()) {
                count++;
                Map<String, Object> valueMap = res.getRow();
                assertEquals("c1 should match", 1001L, valueMap.get("c1"));
                assertEquals("c2 should match", "batch_test1", valueMap.get("c2"));
                assertEquals("c3 should match", 2001L, valueMap.get("c3"));
            }
            assertEquals("Should have 1 records", 1, count);

        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 批量读取测试
     */
    @Test
    public void testBatchGet() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batch = client.batchOperation(tableName);
                    // 添加多个Get操作
                    for (int i = 1; i <= 3; i++) {
                        Get get = client.get(tableName);
                        get.setRowKey(row(colVal("c1", 1001L)))
                            .select("c1", "c2", "c3");
                        batch.addOperation(get);
                    }
                    batch.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use get not supported]"));
    }

    /**
     * 批量更新测试
     */
    @Test
    public void testBatchUpdate() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batch = client.batchOperation(tableName);
                    // 添加多个Update操作
                    for (int i = 1; i <= 3; i++) {
                        Update update = client.update(tableName);
                        update.setPartitionKey(row(colVal("c1", 1001L)))
                            .addMutateColVal(colVal("c1", 1001L), colVal("c2", "batch_test1"), colVal("c3", 2001L));
                        batch.addOperation(update);
                    }
                    batch.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use update not supported]"));
    }

    /**
     * 批量删除测试
     */
    @Test
    public void testBatchDelete() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batch = client.batchOperation(tableName);
                    // 添加多个Delete操作
                    for (int i = 1; i <= 3; i++) {
                        Delete delete = client.delete(tableName);
                        delete.setPartitionKey(row(colVal("c1", 1001L)));
                        batch.addOperation(delete);
                    }
                    batch.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use delete not supported]"));
    }

    /**
     * 批量Increment测试
     */
    @Test
    public void testBatchIncrement() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batch = client.batchOperation(tableName);
                    // 添加多个Increment操作
                    for (int i = 1; i <= 3; i++) {
                        Increment increment = client.increment(tableName);
                        increment.setPartitionKey(row(colVal("c1", 1001L)))
                            .addMutateColVal(colVal("c1", 1001L), colVal("c2", "batch_test1"), colVal("c3", 2001L));
                        batch.addOperation(increment);
                    }
                    batch.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use increment not supported]"));
    }

    /**
     * 批量Append测试
     */
    @Test
    public void testBatchAppend() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batch = client.batchOperation(tableName);
                    // 添加多个append操作
                    for (int i = 1; i <= 3; i++) {
                        Append append = client.append(tableName);
                        append.setPartitionKey(row(colVal("c1", 1001L)))
                            .addMutateColVal(colVal("c1", 1001L), colVal("c2", "batch_test1"), colVal("c3", 2001L));
                        batch.addOperation(append);
                    }
                    batch.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use append not supported]"));
    }

    /**
     * 批量Put测试
     */
    @Test
    public void testBatchPut() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batch = client.batchOperation(tableName);
                    // 添加多个put操作
                    for (int i = 1; i <= 3; i++) {
                        Put put = client.put(tableName);
                        put.setPartitionKey(row(colVal("c1", 1001L)))
                            .addMutateColVal(colVal("c1", 1001L), colVal("c2", "batch_test1"), colVal("c3", 2001L));
                        batch.addOperation(put);
                    }
                    batch.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use put not supported]"));
    }

    /**
     * 批量CheckAndInsUp测试
     */
    @Test
    public void testBatchCheckAndInsUp() throws Exception {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batch = client.batchOperation(tableName);
                    // 添加多个checkAndInsUp操作
                    for (int i = 1; i <= 3; i++) {
                        InsertOrUpdate insUp = client.insertOrUpdate(tableName)
                            .setPartitionKey(row(colVal("c1", 1001L)))
                            .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c3", 2001L));
                        ObTableFilter filter = compareVal(ObCompareOp.GE, "c1", 1001L);
                        batch.addOperation(client.checkAndInsUp(tableName, filter, insUp, true, true));
                    }
                    batch.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][heap table use checkAndInsUp not supported]"));
    }
}
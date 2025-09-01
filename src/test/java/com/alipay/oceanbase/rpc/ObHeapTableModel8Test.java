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

 import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
 import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
 
 import org.junit.After;
 import org.junit.Before;
 import org.junit.Test;
 
 import java.sql.Connection;
 import java.sql.ResultSet;
 import java.sql.Statement;
 
 import static org.junit.Assert.*;
 import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
 import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

/*
 * 表模型8：普通分区堆表 + 自增列 + 默认更新时间 + TTL
 * CREATE TABLE test_heap_model8 (
 * c1 bigint, 
 * c2 varchar(20), 
 * c3 bigint auto_increment, 
 * c4 bigint, 
 * c5 timestamp default current_timestamp on update current_timestamp,
 * UNIQUE KEY idx_unique_c1 (c1)
 * ) ORGANIZATION = HEAP TTL(c5 + INTERVAL 5 SECOND) PARTITION BY KEY(c1) PARTITIONS 3;
 */
public class ObHeapTableModel8Test {
    ObTableClient        client;
    public static String tableName = "test_heap_model8";

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
                     + "UNIQUE KEY idx_unique_c1 (c1) "
                     + ") ORGANIZATION = HEAP TTL(c5 + INTERVAL 5 SECOND) PARTITION BY KEY(c1) PARTITIONS 3");
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

    private void enableTTL() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("ALTER SYSTEM SET enable_kv_ttl= true;");
    }

    private void triggerTTL() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("ALTER SYSTEM trigger TTL;");
    }

    // ==================== TTL 测试 ====================
    /**
     * 测试可见性
     */
    @Test
    public void testTTL1() throws Exception {
        try {
            // 插入数据
            MutationResult result1 = client
                .insertOrUpdate(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c4", 4001L))
                .execute();

            assertEquals("Insert should affect 1 row", 1, result1.getAffectedRows());
            System.out.println("Single insert successful: " + result1 + " row affected");

            // 等待6秒
            Thread.sleep(6000);

            // 插入数据, c1=1001 已经过期，对外不可见，内核会先删除旧行，再写入新行
            MutationResult result2 = client
                .insertOrUpdate(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test2"), colVal("c4", 4002L))
                .execute();
            assertEquals("Insert should affect 1 row", 1, result2.getAffectedRows());
            System.out.println("Single insert successful: " + result2 + " row affected");

            // sql 验证
            String sql = "select /*+ opt_param('hidden_column_visible','true') */ __pk_increment, c1, c2, c3 from "
                         + tableName + " where c1 = 1001";
            Connection connection = ObTableClientTestUtil.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            int count = 0;
            while (resultSet.next()) {
                count++;
                assertEquals("c1 should match", 1001L, resultSet.getLong("c1"));
                assertEquals("c2 should match", "test2", resultSet.getString("c2"));
            }
            assertEquals("Should have 1 record", 1, count);
        } finally {
            // 清理数据
            truncateTable();
        }
    }

    /**
     * 测试TTL后台任务删除数据
     */
    @Test
    public void testTTL2() throws Exception {
        try {
            // 插入数据
            MutationResult result1 = client
                .insertOrUpdate(tableName)
                .setPartitionKey(row(colVal("c1", 1001L)))
                .addMutateColVal(colVal("c1", 1001L), colVal("c2", "test1"), colVal("c4", 4001L))
                .execute();

            assertEquals("Insert should affect 1 row", 1, result1.getAffectedRows());
            System.out.println("Single insert successful: " + result1 + " row affected");

            // 等待5秒
            Thread.sleep(5000);

            // 触发TTL后台任务
            enableTTL();
            triggerTTL();

            // 等待5秒,等待后台任务执行完成
            Thread.sleep(5000);

            // sql 验证,数据应该被删除
            String sql = "select /*+ opt_param('hidden_column_visible','true') */ __pk_increment, c1, c2, c3 from "
                         + tableName + " where c1 = 1001";
            Connection connection = ObTableClientTestUtil.getConnection();
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            assertFalse("Data should be deleted", resultSet.next());
        } finally {
            // 清理数据
            truncateTable();
        }
    }

}

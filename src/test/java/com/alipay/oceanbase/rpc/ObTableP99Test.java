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

import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.*;

/**
 CREATE TABLE IF NOT EXISTS `test_p99` (
 `c1` bigint(20) NOT NULL,
 `c2` bigint(20) DEFAULT NULL,
 `c3` varchar(20) DEFAULT "hello",
 PRIMARY KEY (`c1`)
 );
 **/
public class ObTableP99Test {
    ObTableClient        client;
    private static Connection conn = null;
    public static String tableName = "test_p99";
    public static String insertSqlType = "TABLEAPI INSERT";
    public static String selectSqlType = "TABLEAPI SELECT";
    public static String deleteSqlType = "TABLEAPI DELETE";
    public static String updateSqlType = "TABLEAPI UPDATE";
    public static String replaceSqlType = "TABLEAPI REPLACE";
    public static String queryAndMutateSqlType = "TABLEAPI QUERY AND MUTATE";
    public static String otherSqlType = "TABLEAPI OTHER";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { "c1" });
        conn = ObTableClientTestUtil.getConnection();
    }

    private static long getResultCount(String sqlType) throws Exception {
        PreparedStatement ps = conn.prepareStatement("select * from oceanbase.gv$ob_query_response_time_histogram " +
                "where sql_type=" + "\"" +sqlType +"\"");
        ResultSet rs = ps.executeQuery();
        long totalCnt = 0L;
        while (rs.next()) {
            totalCnt += rs.getLong("count");
        }
        ps.close();
        return totalCnt;
    }

    private static void flushHistogram() throws Exception {
        Statement statement = conn.createStatement();
        statement.execute("alter system set query_response_time_flush = true;");
    }

    @Test
    public void testInsert() throws Exception {
        try {
            // single insert
            client.insert(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            assertEquals(1, getResultCount(insertSqlType));
            client.insert(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            assertEquals(2, getResultCount(insertSqlType));

            // single insertOrUpdate
            flushHistogram();
            Thread.sleep(100);
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            assertEquals(1, getResultCount(insertSqlType));
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            assertEquals(2, getResultCount(insertSqlType));

            // multi insert
            flushHistogram();
            Thread.sleep(100);
            BatchOperation batch1 = client.batchOperation(tableName);
            Insert ins_0 = client.insert(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L));
            batch1.addOperation(ins_0).execute();
            assertEquals(1, getResultCount(insertSqlType));
            batch1.addOperation(ins_0).execute();
            assertEquals(2, getResultCount(insertSqlType));

            // multi insertOrUpdate
            flushHistogram();
            Thread.sleep(100);
            BatchOperation batch2 = client.batchOperation(tableName);
            InsertOrUpdate insUp_0 = client.insertOrUpdate(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L));
            batch2.addOperation(insUp_0).execute();
            assertEquals(1, getResultCount(insertSqlType));
            batch2.addOperation(insUp_0).execute();
            assertEquals(2, getResultCount(insertSqlType));

        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
            client.delete(tableName).setRowKey(colVal("c1", 2L)).execute();
            flushHistogram();
        }
    }

    @Test
    public void testSelect() throws Exception {
        try {
            client.insert(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            assertEquals(1, getResultCount(insertSqlType));

            // single get
            flushHistogram();
            Thread.sleep(100);
            client.get(tableName, 1L, new String[]{"c1"});
            assertEquals(1, getResultCount(selectSqlType));
            client.get(tableName, 1L, new String[]{"c1"});
            assertEquals(2, getResultCount(selectSqlType));

            // multi get
            flushHistogram();
            Thread.sleep(100);
            TableBatchOps batchOps = client.batch(tableName);
            batchOps.get(1L, new String[]{"c1"});
            batchOps.execute();
            assertEquals(1, getResultCount(selectSqlType));
            batchOps.execute();
            assertEquals(2, getResultCount(selectSqlType));

            // sync query
            flushHistogram();
            Thread.sleep(100);
            TableQuery tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L,}, new Object[] { 1L,});
            tableQuery.select("c1");
            tableQuery.execute();
            assertEquals(1, getResultCount(selectSqlType));
            tableQuery.execute();
            assertEquals(2, getResultCount(selectSqlType));

            // async query
            flushHistogram();
            Thread.sleep(100);
            tableQuery.asyncExecute();
            assertEquals(1, getResultCount(selectSqlType));
            tableQuery.asyncExecute();
            assertEquals(2, getResultCount(selectSqlType));
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
            flushHistogram();
        }
    }

    @Test
    public void testDelete() throws Exception {
        try {
            client.insert(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            client.insert(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            assertEquals(2, getResultCount(insertSqlType));

            // single delete
            flushHistogram();
            Thread.sleep(100);
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
            assertEquals(1, getResultCount(deleteSqlType));
            client.delete(tableName).setRowKey(colVal("c1", 2L)).execute();
            assertEquals(2, getResultCount(deleteSqlType));

            // multi delete
            flushHistogram();
            Thread.sleep(100);
            BatchOperation batch = client.batchOperation(tableName);
            Delete del_0 = client.delete(tableName).setRowKey(colVal("c1", 1L));
            batch.addOperation(del_0).execute();
            assertEquals(1, getResultCount(deleteSqlType));
            batch.addOperation(del_0).execute();
            assertEquals(2, getResultCount(deleteSqlType));
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
            client.delete(tableName).setRowKey(colVal("c1", 2L)).execute();
            flushHistogram();
        }
    }

    @Test
    public void testUpdate() throws Exception {
        try {
            client.insert(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            client.insert(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            assertEquals(2, getResultCount(insertSqlType));

            // single update
            flushHistogram();
            Thread.sleep(100);
            client.update(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 2L))
                    .execute();
            assertEquals(1, getResultCount(updateSqlType));
            client.update(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 2L))
                    .execute();
            assertEquals(2, getResultCount(updateSqlType));

            // single increment
            flushHistogram();
            Thread.sleep(100);
            client.increment(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 2L))
                    .execute();
            assertEquals(1, getResultCount(updateSqlType));
            client.increment(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 2L))
                    .execute();
            assertEquals(2, getResultCount(updateSqlType));

            // single append
            flushHistogram();
            Thread.sleep(100);
            client.append(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c3", "world"))
                    .execute();
            assertEquals(1, getResultCount(updateSqlType));
            client.append(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c3", "world"))
                    .execute();
            assertEquals(2, getResultCount(updateSqlType));

            // multi update
            flushHistogram();
            Thread.sleep(100);
            BatchOperation batch1 = client.batchOperation(tableName);
            Update ins_0 = client.update(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L));
            batch1.addOperation(ins_0).execute();
            assertEquals(1, getResultCount(updateSqlType));
            batch1.addOperation(ins_0).execute();
            assertEquals(2, getResultCount(updateSqlType));

            // multi increment
            flushHistogram();
            Thread.sleep(100);
            BatchOperation batch2 = client.batchOperation(tableName);
            Increment inc_0 = client.increment(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L));
            batch2.addOperation(inc_0).execute();
            assertEquals(1, getResultCount(updateSqlType));
            batch2.addOperation(inc_0).execute();
            assertEquals(2, getResultCount(updateSqlType));

            // multi append
            flushHistogram();
            Thread.sleep(100);
            BatchOperation batch3 = client.batchOperation(tableName);
            Append app_0 = client.append(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c3", "llo"));
            batch3.addOperation(app_0).execute();
            assertEquals(1, getResultCount(updateSqlType));
            batch3.addOperation(app_0).execute();
            assertEquals(2, getResultCount(updateSqlType));
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
            client.delete(tableName).setRowKey(colVal("c1", 2L)).execute();
            flushHistogram();
        }
    }

    @Test
    public void testReplace() throws Exception {
        try {
            client.insert(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            client.insert(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            assertEquals(2, getResultCount(insertSqlType));
            flushHistogram();
            Thread.sleep(100);
            client.replace(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 2L))
                    .execute();
            assertEquals(1, getResultCount(replaceSqlType));
            client.replace(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 2L))
                    .execute();
            assertEquals(2, getResultCount(replaceSqlType));

            // multi replace
            flushHistogram();
            Thread.sleep(100);
            BatchOperation batch = client.batchOperation(tableName);
            Replace rep_0 = client.replace(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L));
            batch.addOperation(rep_0).execute();
            assertEquals(1, getResultCount(replaceSqlType));
            batch.addOperation(rep_0).execute();
            assertEquals(2, getResultCount(replaceSqlType));
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
            client.delete(tableName).setRowKey(colVal("c1", 2L)).execute();
            flushHistogram();
        }
    }

    @Test
    public void testQueryAndMutate() throws Exception {
        try {
            flushHistogram();
            Thread.sleep(100);
            TableQuery tableQuery = client.query(tableName);
            tableQuery.addScanRange(new Object[] { 0L,}, new Object[] { 1L,});
            tableQuery.select("c1");
            ObTableQueryAndMutateRequest request_0 = client.obTableQueryAndAppend(tableQuery,
                    new String[] { "c3" }, new Object[] {"_append0" }, true);
            client.execute(request_0);
            assertEquals(1, getResultCount(queryAndMutateSqlType));
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
            flushHistogram();
        }
    }

    @Test
    public void testOther() throws Exception {
        try {
            flushHistogram();
            Thread.sleep(100);
            BatchOperation batch = client.batchOperation(tableName);
            Insert ins_0 = client.insert(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L));
            Replace rep_0 = client.replace(tableName).setRowKey(colVal("c1", 2L))
                    .addMutateColVal(colVal("c2", 1L));
            batch.addOperation(ins_0, rep_0).execute();
            assertEquals(1, getResultCount(otherSqlType));
            batch.addOperation(ins_0, rep_0).execute();
            assertEquals(2, getResultCount(otherSqlType));
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
            client.delete(tableName).setRowKey(colVal("c1", 2L)).execute();
            flushHistogram();
        }
    }
}

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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.get.Get;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/*
CREATE TABLE IF NOT EXISTS `test_get` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) NOT NULL,
    `c3` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`, `c2`)
    ) PARTITION BY KEY(`c1`) PARTITIONS 3;
 */
public class ObTableGetTest {
    ObTableClient        client;
    public static String tableName = "test_get";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    /* test empty result */
    @Test
    public void testSingleGet1() throws Exception {
        // expect empty result
        Get get = client.get(tableName);
        Map<String, Object> res = get
            .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).select("c1", "c2")
            .execute();
        Assert.assertNotNull(res);
        Assert.assertEquals(null, res.get("c1"));
    }

    /* test select column */
    @Test
    public void testSingleGet2() throws Exception {
        try {
            // insert
            client.insertOrUpdate(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val")))
                .addMutateColVal(colVal("c3", "c3_val")).execute();

            // select c1,c2
            Map<String, Object> res = client.get(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).select("c1", "c2")
                .execute();
            Assert.assertNotNull(res);
            Assert.assertEquals("c1_val", res.get("c1"));
            Assert.assertEquals("c2_val", res.get("c2"));

            // get with empty select columns
            res = client.get(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).execute();
            Assert.assertNotNull(res);
            Assert.assertEquals("c1_val", res.get("c1"));
            Assert.assertEquals("c2_val", res.get("c2"));

            // select c1
            res = client.get(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).select("c1")
                .execute();
            Assert.assertNotNull(res);
            Assert.assertEquals("c1_val", res.get("c1"));
            Assert.assertEquals(null, res.get("c2"));

            // select c2
            res = client.get(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).select("c2")
                .execute();
            Assert.assertNotNull(res);
            Assert.assertEquals("c2_val", res.get("c2"));
            Assert.assertEquals(null, res.get("c1"));
        } finally {
            client.delete(tableName).setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val")))
                .execute();
        }
    }

    /* test rowKey */
    @Test
    public void testSingleGet3() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.get(tableName).select("c1", "c2").execute(); // not set rowKey
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("RowKey or scan range is null"));
    }

    /* test tableName */
    @Test
    public void testSingleGet4() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.get("my_not_exist_table")
                            .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val")))
                            .select("c1", "c2")
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("table not exist"));
    }

    /* test partition key */
    @Test
    public void testSingleGet5() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.get(tableName)
                            .setRowKey(row(colVal("c1", "c1_val"))) // only set one rowKey column
                            .select("c1", "c2")
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][operation is not supported to partially fill rowkey columns not supported]"));
    }

    /* test empty result */
    @Test
    public void testBatchGet1() throws Exception {
        // expect empty result
        BatchOperation batch = client.batchOperation(tableName);
        Get get = client.get(tableName)
            .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).select("c1", "c2");
        batch.addOperation(get);
        BatchOperationResult res = batch.execute();
        Assert.assertNotNull(res);
        Assert.assertEquals(null, res.get(0).getOperationRow().get("c1"));
        Assert.assertEquals(null, res.get(0).getOperationRow().get("c2"));
    }

    /* test select column */
    @Test
    public void testBatchGet2() throws Exception {
        try {
            // insert
            client.insertOrUpdate(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val")))
                .addMutateColVal(colVal("c3", "c3_val")).execute();

            // select c1,c2
            BatchOperation batch1 = client.batchOperation(tableName);
            Get get = client.get(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).select("c1", "c2");
            batch1.addOperation(get);
            BatchOperationResult res = batch1.execute();
            Assert.assertNotNull(res);
            Assert.assertEquals("c1_val", res.get(0).getOperationRow().get("c1"));
            Assert.assertEquals("c2_val", res.get(0).getOperationRow().get("c2"));

            // get with empty select columns（has bug in observer）
            //            BatchOperation batch2 = client.batchOperation(tableName);
            //            get = client.get(tableName).setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val")));
            //            batch2.addOperation(get);
            //            res = batch2.execute();
            //            Assert.assertNotNull(res);
            //            Assert.assertEquals("c1_val", res.get(0).getOperationRow().get("c1"));
            //            Assert.assertEquals("c2_val", res.get(0).getOperationRow().get("c2"));

            // select c1
            BatchOperation batch3 = client.batchOperation(tableName);
            get = client.get(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).select("c1");
            batch3.addOperation(get);
            res = batch3.execute();
            Assert.assertNotNull(res);
            Assert.assertEquals("c1_val", res.get(0).getOperationRow().get("c1"));
            Assert.assertEquals(null, res.get(0).getOperationRow().get("c2"));

            // select c2
            BatchOperation batch4 = client.batchOperation(tableName);
            get = client.get(tableName)
                .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val"))).select("c2");
            batch4.addOperation(get);
            res = batch4.execute();
            Assert.assertNotNull(res);
            Assert.assertEquals("c2_val", res.get(0).getOperationRow().get("c2"));
            Assert.assertEquals(null, res.get(0).getOperationRow().get("c1"));
        } finally {
            client.delete(tableName).setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val")))
                .execute();
        }
    }

    /* test rowKey */
    @Test
    public void testBatchGet3() throws Exception {
        // not set rowKey
        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> {
                    Get get = client.get(tableName).select("c1", "c2");
                    client.batchOperation(tableName).addOperation(get).execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("RowKey is null"));
    }

    /* test tableName */
    @Test
    public void testBatchGet4() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    String tableName = "my_not_exist_table";
                    Get get = client.get(tableName)
                            .setRowKey(row(colVal("c1", "c1_val"), colVal("c2", "c2_val")))
                            .select("c1", "c2");
                    client.batchOperation(tableName).addOperation(get).execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("table not exist"));
    }

    /* test partition key */
    @Test
    public void testBatchGet5() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    Get get = client.get(tableName)
                            .setRowKey(row(colVal("c1", "c1_val"))) // only set one rowKey
                            .select("c1", "c2");
                    client.batchOperation(tableName).addOperation(get).execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][operation is not supported to partially fill rowkey columns not supported]"));
    }
}

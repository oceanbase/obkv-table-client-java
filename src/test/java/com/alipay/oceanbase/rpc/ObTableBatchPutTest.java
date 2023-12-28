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

import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Put;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Before;
import org.junit.Test;


import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;
import static org.junit.Assert.assertTrue;

/*
CREATE TABLE  IF NOT EXISTS `batch_put` (
    `id` varchar(20) NOT NULL,
    `c1` bigint DEFAULT NULL,
    `c2` bigint DEFAULT NULL,
    `c3` varchar(32) DEFAULT NULL,
    `c4` bigint DEFAULT NULL,
    PRIMARY KEY(`id`))PARTITION BY HASH(`id`) PARTITIONS 32;
 */
public class ObTableBatchPutTest {
    ObTableClient        client;
    public static String tableName    = "batch_put";
    public static String idColumnName = "hello world";
    public static String c1ColumnName = "c1";
    public static String c2ColumnName = "c2";
    public static String c3ColumnName = "c3";
    public static String c4ColumnName = "c4";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
//        client.addRowKeyElement(tableName, new String[] { idColumnName });
    }

    /*
        CREATE TABLE  IF NOT EXISTS `batch_put` (
            `id` varchar(20) NOT NULL,
            `c1` bigint DEFAULT NULL,
            `c2` bigint DEFAULT NULL,
            `c3` varchar(32) DEFAULT NULL,
            `c4` bigint DEFAULT NULL,
            PRIMARY KEY(`id`))PARTITION BY HASH(`id`) PARTITIONS 32;
     */
    @Test
    public void test_batch_put() throws Exception {
        try {
            BatchOperation batchOperation = client.batchOperation(tableName).setIsAtomic(true).setSamePropertiesNames(true);
            for (long i = 0; i < 100; i++) {
                Put putOp = put().setRowKey(row(colVal("id", String.valueOf(i))))
                        .addMutateColVal(colVal("c1", i))
                        .addMutateColVal(colVal("c2", i))
                        .addMutateColVal(colVal("c3", String.valueOf(i)))
                        .addMutateColVal(colVal("c4", i));
                batchOperation.addOperation(putOp);
            }
            BatchOperationResult result = batchOperation.setIsAtomic(true).execute();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        } finally {
            client.addRowKeyElement(tableName, new String[] { idColumnName });
            for (long i = 0; i < 100; i++) {
                client.delete(tableName).addScanRange(String.valueOf(i), String.valueOf(i)).execute();
            }
        }
    }
}
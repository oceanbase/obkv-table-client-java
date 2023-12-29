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
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Before;
import org.junit.Test;


import java.sql.Date;
import java.sql.Timestamp;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;
import static org.junit.Assert.assertTrue;

public class ObTableBatchPutTest {
    ObTableClient        client;
    public static String tableName    = "batch_put";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    /*
        CREATE TABLE  IF NOT EXISTS `batch_put` (
            `id` varchar(20) NOT NULL,
            `c_1` varchar(32) NOT NULL,
            `t_1` datetime(3) DEFAULT NULL,
            `t_2` timestamp(3) DEFAULT NULL,
            `t_3` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
            `b_1` bigint(20) DEFAULT NULL,
            PRIMARY KEY(`id`, `c_1`)) partition by key(`id`) subpartition by key(`c_1`) subpartitions 4 partitions 97;
     */
    @Test
    public void test_batch_put() throws Exception {
        try {
            // 当设置setSamePropertiesNames(true)时，表中所有的列都必须填充值
            BatchOperation batchOperation = client.batchOperation(tableName).setIsAtomic(true).setSamePropertiesNames(true);
            for (long i = 0; i < 100; i++) {
                // 清除毫秒部分，仅保留到秒
                long timeInSeconds = (System.currentTimeMillis() / 1000) * 1000;
                Timestamp ts = new Timestamp(timeInSeconds);
                java.util.Date date = new Date(timeInSeconds);
                Row rowKey = new Row(colVal("id", String.valueOf(i)), // `id` varchar(20)
                        colVal("c_1", String.valueOf(i))); // `id` varchar(20)
                Put putOp = put().setRowKey(rowKey)
                        .addMutateColVal(colVal("t_1", date))          // `t_1` datetime(3)
                        .addMutateColVal(colVal("t_2", ts))            // `t_2` timestamp(3)
                        .addMutateColVal(colVal("t_3", ts))            // `t_3` timestamp(3)
                        .addMutateColVal(colVal("b_1", i));            // `b_1` bigint(20)
                batchOperation.addOperation(putOp);
            }
            BatchOperationResult result = batchOperation.setIsAtomic(true).execute();
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        } finally {
        }
    }
}
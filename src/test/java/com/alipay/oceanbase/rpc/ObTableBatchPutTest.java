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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ObTableBatchPutTest {
    public static String  PARAM_URL               = "configer-server url"; // http://ip:port/services?User_ID=alibaba&UID=test&Action=ObRootServiceInfo&ObCluster=xxx&database=xxx
    public static String  FULL_USER_NAME          = "userName@tenantName@clusterName";
    public static String  PASSWORD                = "password for userName";
    public static String  PROXY_SYS_USER_NAME     = "root";
    public static String  PROXY_SYS_USER_PASSWORD = "";

    ObTableClient        client = new ObTableClient();
    public static String tableName = "batch_put";

    @Before
    public void setup() throws Exception {
        client.setFullUserName(FULL_USER_NAME);
        client.setParamURL(PARAM_URL);
        client.setPassword(PASSWORD);
        client.setSysUserName(PROXY_SYS_USER_NAME);
        client.setSysPassword(PROXY_SYS_USER_PASSWORD);
        client.init();
    }

    /*
        CREATE TABLE  IF NOT EXISTS `batch_put` (
            `id` varchar(20) NOT NULL,
            `b_1` varchar(32) DEFAULT NULL,
            `t_1` datetime(3) NOT NULL,
            `t_2` timestamp(3) DEFAULT NULL,
            `t_3` timestamp(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
            `c_1` bigint(20) default NULL,
            PRIMARY KEY(`id`, `t_1`)) partition by range columns(`t_1`) subpartition by key(`id`) subpartition template (
                subpartition `p0`,
                subpartition `p1`,
                subpartition `p2`,
                subpartition `p3`,
                subpartition `p4`,
                subpartition `p5`,
                subpartition `p6`,
                subpartition `p7`,
                subpartition `p8`,
                subpartition `p9`,
                subpartition `p10`)
                (partition `p0` values less than ('2023-12-01 00:00:00'),
                 partition `p1` values less than ('2023-12-10 00:00:00'),
                 partition `p2` values less than ('2023-12-20 00:00:00'),
                 partition `p3` values less than ('2023-12-30 00:00:00'),
                 partition `p4` values less than ('2024-01-01 00:00:00'));
     */
    @Test
    public void test_batch_put() throws Exception {
        try {
            // 当设置setSamePropertiesNames(true)时，表中所有的列都必须填充值
            BatchOperation batchOperation = client.batchOperation(tableName).setIsAtomic(true)
                .setSamePropertiesNames(true);
            long batchSize = 50;
            for (long i = 0; i < batchSize; i++) {
                // 清除毫秒部分，仅保留到秒
                long timeInSeconds = (System.currentTimeMillis() / 1000) * 1000;
                Timestamp ts = new Timestamp(timeInSeconds);
                java.util.Date date = new Date(timeInSeconds);
                Row rowKey = new Row(colVal("id", String.valueOf(i)), // `id` varchar(20)
                    colVal("t_1", date)); // `t_1` varchar(20)
                Put putOp = put().setRowKey(rowKey)
                    .addMutateColVal(colVal("b_1", String.valueOf(i))) // `b_1` varchar(32)
                    .addMutateColVal(colVal("t_2", ts)) // `t_2` timestamp(3)
                    .addMutateColVal(colVal("t_3", ts)) // `t_3` timestamp(3)
                    .addMutateColVal(colVal("c_1", i)); // `c_1` bigint(20)
                batchOperation.addOperation(putOp);
            }
            BatchOperationResult result = batchOperation.setIsAtomic(true).execute();
            assertEquals(batchSize, result.size());
            assertEquals(0, result.getWrongCount());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        } finally {
        }
    }
}

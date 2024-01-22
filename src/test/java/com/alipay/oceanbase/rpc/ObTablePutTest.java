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

import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ObTablePutTest {
    ObTableClient        client;
    public static String tableName    = "test_put";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { "id" });
    }

    /**
     CREATE TABLE IF NOT EXISTS `test_put` (
         `id` varchar(20) NOT NULL,
         `c1` bigint DEFAULT NULL,
         `c2` bigint DEFAULT NULL,
         `c3` varchar(32) DEFAULT NULL,
         `c4` bigint DEFAULT NULL,
         PRIMARY KEY(`id`)) PARTITION BY KEY(`id`) PARTITIONS 32;
     **/
    @Test
    public void testPut() throws Exception {
        try {
            client.put(tableName).setRowKey(colVal("id", "id0")).execute();
            client.put(tableName).setRowKey(colVal("id", "id0"))
                    .addMutateColVal(colVal("c1", 1L))
                    .execute();
            client.put(tableName).setRowKey(colVal("id", "id0"))
                    .addMutateColVal(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 2L))
                    .execute();
            client.put(tableName).setRowKey(colVal("id", "id0"))
                    .addMutateColVal(colVal("c3", "c3"))
                    .execute();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            client.delete(tableName).setRowKey(colVal("id", "id0")).execute();
        }

    }

}

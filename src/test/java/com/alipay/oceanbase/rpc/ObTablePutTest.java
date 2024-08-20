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
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.sql.Timestamp;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 CREATE TABLE IF NOT EXISTS `test_put` (
 `id` varchar(20) NOT NULL,
 `c1` bigint DEFAULT NULL,
 `c2` bigint DEFAULT NULL,
 `c3` varchar(32) DEFAULT NULL,
 `c4` bigint DEFAULT NULL,
 `expired_ts` timestamp(6) NOT NULL,
 PRIMARY KEY(`id`)) TTL(expired_ts + INTERVAL 1 SECOND) PARTITION BY KEY(`id`) PARTITIONS 32;
 **/
public class ObTablePutTest {
    ObTableClient        client;
    public static String tableName = "test_put";

    @Before
    public void setup() throws Exception {
        setMinimalImage();
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { "id" });
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

    @Test
    public void testPut1() throws Exception {
        try {
            Timestamp curTs = new Timestamp(System.currentTimeMillis());
            curTs.setNanos(0);
            client.put(tableName).setRowKey(colVal("id", "id0"))
                .addMutateColVal(colVal("expired_ts", curTs)).execute();
            client.put(tableName).setRowKey(colVal("id", "id0")).addMutateColVal(colVal("c1", 1L))
                .addMutateColVal(colVal("expired_ts", curTs)).execute();
            client.put(tableName).setRowKey(colVal("id", "id0")).addMutateColVal(colVal("c1", 1L))
                .addMutateColVal(colVal("c2", 2L)).addMutateColVal(colVal("expired_ts", curTs))
                .execute();
            client.put(tableName).setRowKey(colVal("id", "id0"))
                .addMutateColVal(colVal("c3", "c3")).addMutateColVal(colVal("expired_ts", curTs))
                .execute();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            client.delete(tableName).setRowKey(colVal("id", "id0")).execute();
        }
    }

    @Test
    public void testPut2() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.put(tableName).setRowKey(colVal("id", "id0"))
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4227][OB_ERR_NO_DEFAULT_FOR_FIELD][Field 'expired_ts' doesn't have a default value]"));
    }

    @Test
    public void testPut3() throws Exception {
        setFullImage();
        ObTableClient newClient = ObTableClientTestUtil.newTestClient();
        newClient.init();
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    newClient.put(tableName).setRowKey(colVal("id", "id0"))
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][binlog_row_image is full use put not supported]"));
    }

    @Test
    public void testPut4() throws Exception {
        setMinimalImage();
        ObTableClient newClient = ObTableClientTestUtil.newTestClient();
        newClient.init();
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    newClient.put("test_put_with_local_index").setRowKey(colVal("id", "id0"))
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][table with index use put not supported]"));
    }

    @Test
    public void testPut5() throws Exception {
        setMinimalImage();
        ObTableClient newClient = ObTableClientTestUtil.newTestClient();
        newClient.init();
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    newClient.put("test_put_with_global_index").setRowKey(colVal("id", "id0"))
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][table with index use put not supported]"));
    }
}

/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

import com.alipay.oceanbase.rpc.bolt.ObTableClientTestBase;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableValueFilter;
import com.alipay.oceanbase.rpc.location.model.ObServerAddr;
import com.alipay.oceanbase.rpc.location.model.ServerRoster;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.compareVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

public class ObTableClientCheckAndInsertTest extends ObTableClientTestBase  {
    @Before
    public void setup() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty(Property.RPC_CONNECT_TIMEOUT.getKey(), "800");
        obTableClient.addProperty(Property.RPC_LOGIN_TIMEOUT.getKey(), "800");
        obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), "1");
        obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), "3000");
        obTableClient.addProperty(Property.RUNTIME_BATCH_MAX_WAIT.getKey(), "3000");
        obTableClient.addProperty(Property.RUNTIME_BATCH_EXECUTOR.getKey(), "32");
        obTableClient.addProperty(Property.RPC_OPERATION_TIMEOUT.getKey(), "3000");
        obTableClient.init();

        this.client = obTableClient;
        syncRefreshMetaHelper(obTableClient);
    }

    private long getMaxAccessTime(ObTableClient client) throws Exception {
        Class clientClass = client.getClass();
        Field field = clientClass.getDeclaredField("serverRoster");
        field.setAccessible(true);
        ServerRoster serverRoster = (ServerRoster) field.get(client);
        long resTime = 0;
        for (ObServerAddr addr : serverRoster.getMembers()) {
            resTime = Math.max(resTime, addr.getLastAccessTime());
        }
        return resTime;
    }

    @BeforeClass
    public static void testVersion() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        if (ObGlobal.OB_VERSION <= 0) {
            // ob version is invalid
            Assert.assertTrue(false);
        } else if (ObGlobal.OB_VERSION != 4) {
            // todo: only support in 4.x currently
            Assert.assertTrue(false);
        }
    }

    @Test
    // test check and insert
    public void testCheckAndInsert() throws Exception {
        TableQuery tableQuery = client.query("test_mutation");
        tableQuery.addScanRange(new Object[] { 0L, "\0" }, new Object[] { 200L, "\254" });
        tableQuery.select("c1", "c2", "c3", "c4");

        try {
            // prepare data with insert
            client.insert("test_mutation").setRowKey(row(colVal("c1", 0L), colVal("c2", "row_0")))
                    .addMutateColVal(colVal("c3", new byte[]{1}))
                    .addMutateColVal(colVal("c4", 100L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 1L), colVal("c2", "row_1"))
                    .addMutateColVal(colVal("c3", new byte[]{1}))
                    .addMutateColVal(colVal("c4", 101L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 2L), colVal("c2", "row_2"))
                    .addMutateColVal(colVal("c3", new byte[]{1}))
                    .addMutateColVal(colVal("c4", 102L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                    .addMutateColVal(colVal("c1", 3L)).addMutateColVal(colVal("c2", "row_3"))
                    .addMutateColVal(colVal("c3", new byte[]{1}))
                    .addMutateColVal(colVal("c4", 103L)).execute();

            // insert / match filter
            ObTableValueFilter c4_EQ_101 = compareVal(ObCompareOp.EQ, "c4", 101L);
            MutationResult insertResult = client.insert("test_mutation")
                    .setRowKey(colVal("c1", 100L), colVal("c2", "row_5")).setFilter(c4_EQ_101)
                    .addScanRange(new Object[] { 0L, "\0" }, new Object[] { 200L, "\254" })
                    .addMutateRow(row(colVal("c3", new byte[]{1}), colVal("c4", 999L))).execute();
            Assert.assertEquals(1, insertResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_0 = compareVal(ObCompareOp.EQ, "c4", 999L);
            tableQuery.setFilter(confirm_0);
            QueryResultSet result_0 = tableQuery.execute();
            Assert.assertEquals(1, result_0.cacheSize());

            insertResult = client.insert("test_mutation")
                    .setRowKey(colVal("c1", 120L), colVal("c2", "row_6")).setFilter(c4_EQ_101)
                    .addScanRange(new Object[] { 0L, "\0" }, new Object[] { 200L, "\254" })
                    .addMutateRow(row(colVal("c3", new byte[]{1}), colVal("c4", 999L))).execute();
            Assert.assertEquals(1, insertResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            confirm_0 = compareVal(ObCompareOp.EQ, "c4", 999L);
            tableQuery.setFilter(confirm_0);
            result_0 = tableQuery.execute();
            Assert.assertEquals(2, result_0.cacheSize());

            // insert / only insert one row when multiple match
            ObTableValueFilter c4_EQ_999 = compareVal(ObCompareOp.EQ, "c4", 999L);
            insertResult = client.insert("test_mutation")
                    .setRowKey(colVal("c1", 130L), colVal("c2", "row_7")).setFilter(c4_EQ_999)
                    .addScanRange(new Object[] { 0L, "\0" }, new Object[] { 200L, "\254" })
                    .addMutateRow(row(colVal("c3", new byte[]{1}), colVal("c4", 99L))).execute();
            Assert.assertEquals(1, insertResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            confirm_0 = compareVal(ObCompareOp.EQ, "c4", 99L);
            tableQuery.setFilter(confirm_0);
            result_0 = tableQuery.execute();
            Assert.assertEquals(1, result_0.cacheSize());

            // insert / do not match filter
            ObTableValueFilter c4_EQ_201 = compareVal(ObCompareOp.EQ, "c4", 201L);
            insertResult = client.insert("test_mutation")
                    .setRowKey(colVal("c1", 150L), colVal("c2", "row_8")).setFilter(c4_EQ_201)
                    .addScanRange(new Object[] { 0L, "\0" }, new Object[] { 200L, "\254" })
                    .addMutateRow(row(colVal("c3", new byte[]{1}), colVal("c4", 4000L))).execute();
            Assert.assertEquals(0, insertResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            confirm_0 = compareVal(ObCompareOp.EQ, "c4", 4000L);
            tableQuery.setFilter(confirm_0);
            result_0 = tableQuery.execute();
            Assert.assertEquals(0, result_0.cacheSize());

            // test defense for set scan range without filter
            try {
                insertResult = client.insert("test_mutation")
                        .setRowKey(colVal("c1", 120L), colVal("c2", "row_7"))
                        .addScanRange(new Object[] { 0L, "\0" }, new Object[] { 200L, "\254" })
                        .addMutateRow(row(colVal("c3", new byte[]{1}), colVal("c4", 999L))).execute();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTableUnexpectedException);
                Assert.assertEquals("should set filter and scan range both", e.getMessage());
            }
        } finally {
            client.delete("test_mutation").setRowKey(colVal("c1", 0L), colVal("c2", "row_0"))
                    .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 1L), colVal("c2", "row_1"))
                    .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 2L), colVal("c2", "row_2"))
                    .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                    .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 100L), colVal("c2", "row_5"))
                    .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 120L), colVal("c2", "row_6"))
                    .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 130L), colVal("c2", "row_7"))
                    .execute();
        }
    }
}

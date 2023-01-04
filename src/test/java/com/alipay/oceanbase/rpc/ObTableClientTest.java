/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
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
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.*;
import com.alipay.oceanbase.rpc.location.model.ObServerAddr;
import com.alipay.oceanbase.rpc.location.model.ServerRoster;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.*;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.stream.async.ObTableQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableClientQueryAsyncImpl;
import com.alipay.oceanbase.rpc.table.ObTableClientQueryImpl;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.management.Query;
import java.lang.reflect.Field;
import java.util.*;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.*;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ObTableClientTest extends ObTableClientTestBase {
    @Before
    public void setup() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty(Property.RPC_CONNECT_TIMEOUT.getKey(), "800");
        obTableClient.addProperty(Property.RPC_LOGIN_TIMEOUT.getKey(), "800");
        obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), "1");
        obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), "2000");
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

    @Test
    public void testMetadataRefresh() throws Exception {
        final ObTableClient client1 = ObTableClientTestUtil.newTestClient();
        if (!client1.isOdpMode()) {
            try {
                client1.setMetadataRefreshInterval(100);
                client1.setServerAddressCachingTimeout(8000);
                client1.init();
                long lastTime = getMaxAccessTime(client1);
                Thread.sleep(10000);
                client1.insertOrUpdate("test_varchar_table", "foo", new String[] { "c2" },
                    new String[] { "bar" });
                long nowTime = getMaxAccessTime(client1);
                Assert.assertTrue(nowTime - lastTime > 8000);
            } finally {
                client1.delete("test_varchar_table", "foo");
            }
        }
    }

    @Test
    public void testPropertiesNormal() throws Exception {
        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.addProperty("connectTimeout", "100000");
        obTableClient.addProperty("socketTimeout", "100000");
        obTableClient.setMetadataRefreshInterval(60000);
        obTableClient.setMetadataRefreshLockTimeout(8000);
        obTableClient.setRsListAcquireConnectTimeout(500);
        obTableClient.setRsListAcquireReadTimeout(3000);
        obTableClient.setRsListAcquireRetryInterval(100);
        obTableClient.setRsListAcquireTryTimes(3);
        obTableClient.setTableEntryAcquireConnectTimeout(500);
        obTableClient.setTableEntryAcquireSocketTimeout(3000);
        obTableClient.setTableEntryRefreshContinuousFailureCeiling(10);
        obTableClient.setTableEntryRefreshIntervalBase(100);
        obTableClient.setTableEntryRefreshIntervalCeiling(1000);
        obTableClient.setTableEntryRefreshLockTimeout(4000);
        obTableClient.addProperty(Property.RUNTIME_RETRY_INTERVAL.getKey(), "200");
        obTableClient.setServerAddressCachingTimeout(1000L);
        obTableClient.init();

        Assert.assertEquals(obTableClient.getMetadataRefreshInterval(), 60000);
        Assert.assertEquals(obTableClient.getMetadataRefreshLockTimeout(), 8000);
        Assert.assertEquals(obTableClient.getRsListAcquireConnectTimeout(), 500);
        Assert.assertEquals(obTableClient.getRsListAcquireReadTimeout(), 3000);
        Assert.assertEquals(obTableClient.getRsListAcquireRetryInterval(), 100);
        Assert.assertEquals(obTableClient.getRsListAcquireTryTimes(), 3);
        Assert.assertEquals(obTableClient.getTableEntryAcquireConnectTimeout(), 500);
        Assert.assertEquals(obTableClient.getTableEntryAcquireSocketTimeout(), 3000);
        Assert.assertEquals(obTableClient.getTableEntryRefreshContinuousFailureCeiling(), 10);
        Assert.assertEquals(obTableClient.getTableEntryRefreshIntervalBase(), 100);
        Assert.assertEquals(obTableClient.getTableEntryRefreshLockTimeout(), 4000);
        Assert.assertEquals(obTableClient.getRuntimeRetryInterval(), 200);
        Assert.assertEquals(obTableClient.getServerAddressCachingTimeout(), 1000);
        obTableClient.close();
    }

    @Test
    public void testPropertiesException() throws Exception {
        ObTableClient obTableClient1 = ObTableClientTestUtil.newTestClient();
        obTableClient1.addProperty("connectTimeout", "100000");
        obTableClient1.addProperty("socketTimeout", "100000");
        obTableClient1.setRuntimeRetryTimes(-1);
        obTableClient1.init();

        Assert.assertEquals(obTableClient1.getRuntimeRetryTimes(),
            Property.RUNTIME_RETRY_TIMES.getDefaultInt());
        obTableClient1.close();

        ObTableClient obTableClient2 = ObTableClientTestUtil.newTestClient();
        obTableClient2.addProperty("connectTimeout", "100000");
        obTableClient2.addProperty("socketTimeout", "100000");
        obTableClient2.addProperty(Property.RUNTIME_RETRY_TIMES.getKey(), "-1");
        obTableClient2.init();

        Assert.assertEquals(obTableClient2.getRuntimeRetryTimes(), 1);
        obTableClient2.close();
    }

    @Test
    public void testIncrement() throws Exception {

        /*create table test_increment(c1 varchar(255),c2 int ,c3 int,primary key(c1))  */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final Table obTableClient = client;
        try {

            obTableClient.insert("test_increment", "test_normal", new String[] { "c2", "c3" },
                new Object[] { 1, 2 });
            Map<String, Object> res = obTableClient.increment("test_increment", "test_normal",
                new String[] { "c2", "c3" }, new Object[] { 1, 2 }, true);
            Assert.assertEquals(4, res.get("c3"));
            Assert.assertEquals(2, res.get("c2"));

            obTableClient.insert("test_increment", "test_null", new String[] { "c2", "c3" },
                new Object[] { null, null });
            res = obTableClient.increment("test_increment", "test_null",
                new String[] { "c2", "c3" }, new Object[] { 1, 2 }, true);
            Assert.assertEquals(2, res.get("c3"));
            Assert.assertEquals(1, res.get("c2"));

            res = obTableClient.increment("test_increment", "test_empty",
                new String[] { "c2", "c3" }, new Object[] { 1, 2 }, true);
            Assert.assertEquals(2, res.get("c3"));
            Assert.assertEquals(1, res.get("c2"));

            res = obTableClient.increment("test_increment", "test_empty",
                new String[] { "c2", "c3" }, new Object[] { 1, 2 }, false);
            Assert.assertTrue(res.isEmpty());
        } finally {
            obTableClient.delete("test_increment", "test_normal");
            obTableClient.delete("test_increment", "test_null");
            obTableClient.delete("test_increment", "test_empty");
        }
    }

    @Test
    public void testAppend() throws Exception {

        /*create table test_append(c1 varchar(255),c2 varbinary(1024) ,c3 varchar(255),primary key(c1));  */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        try {

            client.insert("test_append", "test_normal", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 1 }, "P" });
            Map<String, Object> res = client.append("test_append", "test_normal", new String[] {
                    "c2", "c3" }, new Object[] { new byte[] { 2 }, "Y" }, true);

            Assert.assertTrue(Arrays.equals(new byte[] { 1, 2 }, (byte[]) res.get("c2")));
            Assert.assertEquals("PY", res.get("c3"));

            client.insert("test_append", "test_null", new String[] { "c2", "c3" }, new Object[] {
                    null, null });
            res = client.append("test_append", "test_null", new String[] { "c2", "c3" },
                new Object[] { new byte[] { 1 }, "P" }, true);
            Assert.assertTrue(Arrays.equals(new byte[] { 1 }, (byte[]) res.get("c2")));
            Assert.assertEquals("P", res.get("c3"));

            res = client.append("test_append", "test_empty", new String[] { "c2", "c3" },
                new Object[] { new byte[] { 1 }, "P" }, true);
            Assert.assertTrue(Arrays.equals(new byte[] { 1 }, (byte[]) res.get("c2")));
            Assert.assertEquals("P", res.get("c3"));

            res = client.append("test_append", "test_empty", new String[] { "c2", "c3" },
                new Object[] { new byte[] { 1 }, "P" }, false);
            Assert.assertTrue(res.isEmpty());

        } finally {
            client.delete("test_append", "test_normal");
            client.delete("test_append", "test_null");
            client.delete("test_append", "test_empty");
        }
    }

    @Test
    public void testIncrementAppendBatch() throws Exception {
        /*create table test_increment(c1 varchar(255),c2 int ,c3 int,primary key(c1))*/
        /*create table test_append(c1 varchar(255),c2 varbinary(1024) ,c3 varchar(255),primary key(c1));  */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final Table obTableClient = client;

        try {

            obTableClient.insert("test_increment", "test_normal", new String[] { "c2", "c3" },
                new Object[] { 1, 2 });
            TableBatchOps batchOps = obTableClient.batch("test_increment");
            batchOps.get("test_normal", new String[] { "c2", "c3" });
            batchOps.insert("test_insert", new String[] { "c2", "c3" }, new Object[] { 2, 4 });
            batchOps.increment("test_normal", new String[] { "c2", "c3" }, new Object[] { 1, 2 },
                true);
            batchOps.increment("test_insert", new String[] { "c2", "c3" }, new Object[] { 2, 4 },
                true);
            List<Object> res = batchOps.execute();

            Assert.assertTrue(res.get(0) instanceof Map);

            Assert.assertEquals(1, ((Map) res.get(0)).get("c2"));
            Assert.assertEquals(2, ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertEquals(2, ((Map) res.get(2)).get("c2"));
            Assert.assertEquals(4, ((Map) res.get(2)).get("c3"));
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertEquals(4, ((Map) res.get(3)).get("c2"));
            Assert.assertEquals(8, ((Map) res.get(3)).get("c3"));

            batchOps = obTableClient.batch("test_increment");
            batchOps.get("test_normal", new String[] { "c2", "c3" });
            batchOps.insert("test_insert_two", new String[] { "c2", "c3" }, new Object[] { 2, 4 });
            batchOps.increment("test_normal", new String[] { "c2", "c3" }, new Object[] { 1, 2 },
                false);
            batchOps.increment("test_insert", new String[] { "c2", "c3" }, new Object[] { 2, 4 },
                false);
            res = batchOps.execute();

            Assert.assertTrue(res.get(0) instanceof Map);

            Assert.assertEquals(2, ((Map) res.get(0)).get("c2"));
            Assert.assertEquals(4, ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertTrue(((Map) res.get(2)).isEmpty());
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertTrue(((Map) res.get(3)).isEmpty());

            obTableClient.insert("test_append", "test_normal", new String[] { "c2", "c3" },
                new Object[] { new byte[] { 1 }, "P" });

            batchOps = obTableClient.batch("test_append");
            batchOps.get("test_normal", new String[] { "c2", "c3" });
            batchOps.insert("test_append", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 2 }, "Q" });
            batchOps.append("test_normal", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 1 }, "P" }, true);
            batchOps.append("test_append", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 2 }, "Q" }, true);
            res = batchOps.execute();
            Assert.assertTrue(res.get(0) instanceof Map);
            Assert
                .assertTrue(Arrays.equals(new byte[] { 1 }, (byte[]) ((Map) res.get(0)).get("c2")));
            Assert.assertEquals("P", ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[] { 1, 1 },
                (byte[]) ((Map) res.get(2)).get("c2")));
            Assert.assertEquals("PP", ((Map) res.get(2)).get("c3"));
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[] { 2, 2 },
                (byte[]) ((Map) res.get(3)).get("c2")));
            Assert.assertEquals("QQ", ((Map) res.get(3)).get("c3"));

            batchOps = obTableClient.batch("test_append");
            batchOps.get("test_normal", new String[] { "c2", "c3" });
            batchOps.insert("test_append_two", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 2 }, "Q" });
            batchOps.append("test_normal", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 1 }, "P" }, false);
            batchOps.append("test_append", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 2 }, "Q" }, false);
            res = batchOps.execute();
            Assert.assertTrue(res.get(0) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[] { 1, 1 },
                (byte[]) ((Map) res.get(0)).get("c2")));
            Assert.assertEquals("PP", ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertTrue(((Map) res.get(2)).isEmpty());
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertTrue(((Map) res.get(3)).isEmpty());

        } finally {
            obTableClient.delete("test_increment", "test_normal");
            obTableClient.delete("test_increment", "test_insert");
            obTableClient.delete("test_increment", "test_insert_two");
            obTableClient.delete("test_append", "test_normal");
            obTableClient.delete("test_append", "test_append");
            obTableClient.delete("test_append", "test_append_two");
        }
    }

    @Test
    public void testIncrementAppendBatchWithPriority() throws Exception {
        /*create table test_increment(c1 varchar(255),c2 int ,c3 int,primary key(c1))*/
        /*create table test_append(c1 varchar(255),c2 varbinary(1024) ,c3 varchar(255),primary key(c1));  */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");
        ThreadLocalMap.setProcessHighPriority();
        final Table obTableClient = client;

        try {

            obTableClient.insert("test_increment", "test_normal", new String[] { "c2", "c3" },
                new Object[] { 1, 2 });
            TableBatchOps batchOps = obTableClient.batch("test_increment");
            batchOps.get("test_normal", new String[] { "c2", "c3" });
            batchOps.insert("test_insert", new String[] { "c2", "c3" }, new Object[] { 2, 4 });
            batchOps.increment("test_normal", new String[] { "c2", "c3" }, new Object[] { 1, 2 },
                true);
            batchOps.increment("test_insert", new String[] { "c2", "c3" }, new Object[] { 2, 4 },
                true);
            List<Object> res = batchOps.execute();

            Assert.assertTrue(res.get(0) instanceof Map);

            Assert.assertEquals(1, ((Map) res.get(0)).get("c2"));
            Assert.assertEquals(2, ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertEquals(2, ((Map) res.get(2)).get("c2"));
            Assert.assertEquals(4, ((Map) res.get(2)).get("c3"));
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertEquals(4, ((Map) res.get(3)).get("c2"));
            Assert.assertEquals(8, ((Map) res.get(3)).get("c3"));

            batchOps = obTableClient.batch("test_increment");
            batchOps.get("test_normal", new String[] { "c2", "c3" });
            batchOps.insert("test_insert_two", new String[] { "c2", "c3" }, new Object[] { 2, 4 });
            batchOps.increment("test_normal", new String[] { "c2", "c3" }, new Object[] { 1, 2 },
                false);
            batchOps.increment("test_insert", new String[] { "c2", "c3" }, new Object[] { 2, 4 },
                false);
            res = batchOps.execute();

            Assert.assertTrue(res.get(0) instanceof Map);

            Assert.assertEquals(2, ((Map) res.get(0)).get("c2"));
            Assert.assertEquals(4, ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertTrue(((Map) res.get(2)).isEmpty());
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertTrue(((Map) res.get(3)).isEmpty());

            obTableClient.insert("test_append", "test_normal", new String[] { "c2", "c3" },
                new Object[] { new byte[] { 1 }, "P" });

            batchOps = obTableClient.batch("test_append");
            batchOps.get("test_normal", new String[] { "c2", "c3" });
            batchOps.insert("test_append", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 2 }, "Q" });
            batchOps.append("test_normal", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 1 }, "P" }, true);
            batchOps.append("test_append", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 2 }, "Q" }, true);
            res = batchOps.execute();
            Assert.assertTrue(res.get(0) instanceof Map);
            Assert
                .assertTrue(Arrays.equals(new byte[] { 1 }, (byte[]) ((Map) res.get(0)).get("c2")));
            Assert.assertEquals("P", ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[] { 1, 1 },
                (byte[]) ((Map) res.get(2)).get("c2")));
            Assert.assertEquals("PP", ((Map) res.get(2)).get("c3"));
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[] { 2, 2 },
                (byte[]) ((Map) res.get(3)).get("c2")));
            Assert.assertEquals("QQ", ((Map) res.get(3)).get("c3"));

            batchOps = obTableClient.batch("test_append");
            batchOps.get("test_normal", new String[] { "c2", "c3" });
            batchOps.insert("test_append_two", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 2 }, "Q" });
            batchOps.append("test_normal", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 1 }, "P" }, false);
            batchOps.append("test_append", new String[] { "c2", "c3" }, new Object[] {
                    new byte[] { 2 }, "Q" }, false);
            res = batchOps.execute();
            Assert.assertTrue(res.get(0) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[] { 1, 1 },
                (byte[]) ((Map) res.get(0)).get("c2")));
            Assert.assertEquals("PP", ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertTrue(((Map) res.get(2)).isEmpty());
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertTrue(((Map) res.get(3)).isEmpty());

        } finally {
            obTableClient.delete("test_increment", "test_normal");
            obTableClient.delete("test_increment", "test_insert");
            obTableClient.delete("test_increment", "test_insert_two");
            obTableClient.delete("test_append", "test_normal");
            obTableClient.delete("test_append", "test_append");
            obTableClient.delete("test_append", "test_append_two");
        }
    }

    @Test
    public void testAddrExpired() throws Exception {
        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        try {
            obTableClient.setServerAddressCachingTimeout(1000);
            obTableClient.setMetadataRefreshInterval(100);
            obTableClient.init();
            assertEquals(1L, obTableClient.insert("test_varchar_table", "foo",
                new String[] { "c2" }, new String[] { "bar" }));

            // sleep 2000 ms to let the server addr expired.
            Thread.sleep(2000);
            Map<String, Object> values = obTableClient.get("test_varchar_table", "bar",
                new String[] { "c2" });
            assertNotNull(values);
            assertEquals(0, values.size());

            values = obTableClient.get("test_varchar_table", "foo", new String[] { "c2" });
            assertNotNull(values);
            assertEquals("bar", values.get("c2"));
        } finally {
            obTableClient.delete("test_varchar_table", "foo");
            obTableClient.close();
        }
    }

    @Test
    public void test_batch_query() throws Exception {
        /*
        * CREATE TABLE `test_batch_query` (
             `c1` bigint NOT NULL,
             `c2` varchar(20) DEFAULT NULL,
            PRIMARY KEY (`c1`))partition by range(`c1`)(partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));
            * alter table test_varchar_table add key `idx_test` (`c1`,`c3`);
            )*/
        Object[] c1 = new Object[] { 123L, 124L, 136L, 138L, 145L, 567L, 666L, 777L };
        Object[] c2 = new Object[] { "123c2", "124c2", "136c2", "138c2", "145c2", "567c2", "666c2",
                "777c2" };

        ObTableClient client1 = new ObTableClient();
        try {
            client1.setMetadataRefreshInterval(100);
            client1 = ObTableClientTestUtil.newTestClient();
            client1.setServerAddressCachingTimeout(10000000);
            client1.init();
            client1.addRowKeyElement("test_batch_query", new String[] { "c1" }); //同索引列的值一样
            for (int i = 0; i < 8; i++) {
                client1.insert("test_batch_query", new Object[] { c1[i] }, new String[] { "c2" },
                    new Object[] { c2[i] });
            }

            // 非阻塞query
            TableQuery tableQuery = client1.queryByBatchV2("test_batch_query");

            // 测试 filter string 生成函数
            ObTableValueFilter filter_0 = new ObTableValueFilter(ObCompareOp.EQ, "c3", "value");
            assertEquals("TableCompareFilter(=, 'c3:value')", filter_0.toString());
            ObTableFilterList filterList = new ObTableFilterList(ObTableFilterList.operator.AND);
            ObTableValueFilter filter_1 = new ObTableValueFilter(ObCompareOp.LE, "c2", 5);
            filterList.addFilter(filter_0);
            filterList.addFilter(filter_1);
            assertEquals("TableCompareFilter(=, 'c3:value') && TableCompareFilter(<=, 'c2:5')",
                filterList.toString());
            // test param null
            try {
                ObTableValueFilter filter = new ObTableValueFilter(ObCompareOp.NE, null, null);
            } catch (Exception e) {
                assertTrue(true);
            }
            // test null
            try {
                tableQuery.setFilter(null);
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                ObTableFilterList filterListNull = new ObTableFilterList(null);
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                ObTableFilterList filterListNull = andList(filter_0, null);
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                ObTableFilterList filterListNull = new ObTableFilterList(
                    ObTableFilterList.operator.AND, filter_0, null);
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                ObTableFilterList filterListNull = new ObTableFilterList();
                filterListNull.addFilter(filter_0, null);
            } catch (Exception e) {
                assertTrue(true);
            }
            // test nested
            ObTableFilterList filterListNested_0 = new ObTableFilterList(
                ObTableFilterList.operator.OR);
            ObTableFilterList filterListNested_1 = new ObTableFilterList(
                ObTableFilterList.operator.AND);
            filterListNested_0.addFilter(filter_0);
            filterListNested_0.addFilter(filter_1);
            filterListNested_1.addFilter(filterListNested_0);
            filterListNested_1.addFilter(filter_0);
            filterListNested_1.addFilter(filterListNested_0);
            assertEquals(
                "(TableCompareFilter(=, 'c3:value') || TableCompareFilter(<=, 'c2:5')) && TableCompareFilter(=, 'c3:value') && (TableCompareFilter(=, 'c3:value') || TableCompareFilter(<=, 'c2:5'))",
                filterListNested_1.toString());
            ObTableFilterList filterListNested_2 = new ObTableFilterList(
                ObTableFilterList.operator.OR, filter_0);
            filterListNested_2.addFilter(filterListNested_1);
            assertEquals(
                "TableCompareFilter(=, 'c3:value') || ((TableCompareFilter(=, 'c3:value') || TableCompareFilter(<=, 'c2:5')) && TableCompareFilter(=, 'c3:value') && (TableCompareFilter(=, 'c3:value') || TableCompareFilter(<=, 'c2:5')))",
                filterListNested_2.toString());

            // test query with filter = null
            try {
                TableQuery tableQuery_error = client1.query("test_batch_query");
                tableQuery_error.addScanRange(new Object[] { 0L }, new Object[] { 250L });
                tableQuery_error.select("c1", "c2", "c3");
                tableQuery_error.setFilter(null);
                tableQuery_error.setBatchSize(3);
            } catch (Exception e) {
                System.out.println(e);
                assertTrue(e instanceof IllegalArgumentException);
            }

            // test query with batchsize
            try {
                TableQuery tableQuery_error = client1.query("test_batch_query");
                tableQuery_error.addScanRange(new Object[] { 0L }, new Object[] { 250L });
                tableQuery_error.select("c1", "c2", "c3");
                tableQuery_error.setBatchSize(3);
            } catch (Exception e) {
                System.out.println(e);
                assertTrue(e instanceof ObTableException);
            }

            // 查询结果集
            QueryResultSet result = tableQuery.select("c2").primaryIndex().setBatchSize(2)
                .addScanRange(new Object[] { 123L }, new Object[] { 777L }).execute();

            for (int i = 0; i < 4; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get("c2"), c2[i]);
                System.out.println("c2:" + value.get("c2"));
            }
            for (int i = 4; i < 8; i++) {
                Assert.assertTrue(result.next());
                Row value = result.getResultRow();
                assertEquals(value.get("c2"), c2[i]);
                System.out.println("c2:" + value.get("c2"));
            }
            Assert.assertFalse(result.next());

        } catch (Exception e) {
            assertTrue(true);
        } finally {
            for (int i = 0; i < 8; i++) {
                client1.delete("test_batch_query", new Object[] { c1[i] });
            }
            client1.close();
        }
    }

    @Test
    public void test_batch_query_coverage() {
        ObTableClient client1 = new ObTableClient();

        TableQuery tableQuery = client1.query("test_batch_query");
        ObTable obTable = new ObTable();
        try {
            tableQuery.executeInit(new ObPair<Long, ObTable>(0L, obTable));
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.executeNext(new ObPair<Long, ObTable>(0L, obTable));
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
        try {
            tableQuery.setMaxResultSize(100000);
        } catch (Exception e) {
            assertTrue(true);
        }
        tableQuery.clear();

        ObTableClientQueryImpl obTableClientQuery = new ObTableClientQueryImpl("test_batch_query",
            client1);
        try {
            obTableClientQuery.executeInit(new ObPair<Long, ObTable>(0L, obTable));
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
        try {
            obTableClientQuery.executeNext(new ObPair<Long, ObTable>(0L, obTable));
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        ObTableClientQueryAsyncImpl obTableClientQueryAsync = new ObTableClientQueryAsyncImpl(
            "test_batch_query", tableQuery.getObTableQuery(), client1);
        obTableClientQueryAsync.getSessionId();
        try {
            obTableClientQueryAsync.setKeys("c1", "c3");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
        try {
            obTableClientQueryAsync.executeInternal(ObQueryOperationType.QUERY_START);
        } catch (Exception e) {
            assertTrue(true);
        }

        ObTableQueryAsyncStreamResult obTableQueryAsyncStreamResult = new ObTableQueryAsyncStreamResult();
        obTableQueryAsyncStreamResult.setSessionId(100000);
        obTableQueryAsyncStreamResult.getSessionId();
        obTableQueryAsyncStreamResult.setEnd(true);
    }

    @Test
    public void testQueryWithFilter() throws Exception {

        /*
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样

        try {
            client.insert("test_query_filter_mutate", new Object[] { 0L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row1" });
            client.insert("test_query_filter_mutate", new Object[] { 1L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row2" });
            client.insert("test_query_filter_mutate", new Object[] { 2L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row3" });

            TableQuery tableQuery = client.query("test_query_filter_mutate");
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 250L });
            tableQuery.select("c1", "c2", "c3");

            ObTableValueFilter filter_0 = new ObTableValueFilter(ObCompareOp.GT, "c1", 0);
            ObTableValueFilter filter_1 = new ObTableValueFilter(ObCompareOp.LE, "c1", 2);
            ObTableValueFilter filter_2 = new ObTableValueFilter(ObCompareOp.GT, "c1", 1);

            tableQuery.setFilter(filter_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            tableQuery.setFilter(filter_1);
            result = tableQuery.execute();
            Assert.assertEquals(3, result.cacheSize());

            tableQuery.setFilter(filter_2);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
        } finally {
            client.delete("test_query_filter_mutate", new Object[] { 0L });
            client.delete("test_query_filter_mutate", new Object[] { 1L });
            client.delete("test_query_filter_mutate", new Object[] { 2L });
        }
    }

    @Test
    public void testQueryAndAppend() throws Exception {

        /* 
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样

        try {
            client.insert("test_query_filter_mutate", new Object[] { 0L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row1" });
            client.insert("test_query_filter_mutate", new Object[] { 1L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row2" });
            client.insert("test_query_filter_mutate", new Object[] { 2L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row3" });
            TableQuery tableQuery = client.query("test_query_filter_mutate");
            /* Scan range must in one partition */
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.select("c1", "c2", "c3");

            /* Set Filter String */
            ObTableValueFilter filter_0 = compareVal(ObCompareOp.GT, "c1", 0);
            ObTableValueFilter filter_1 = compareVal(ObCompareOp.GE, "c3", "row3_append0");

            tableQuery.setFilter(filter_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client)
                .obTableQueryAndAppend(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "_append0" }, true);
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(2, res.getAffectedRows());
            /* check value before append */
            Assert.assertEquals("row2", res.getAffectedEntity().getPropertiesRows().get(0).get(2)
                .getValue());
            /* To confirm changing. re-query to get the latest data */;
            ObTableValueFilter confirm_0 = compareVal(ObCompareOp.EQ, "c3", "row2_append0");
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

            tableQuery.setFilter(filter_1);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client)
                .obTableQueryAndAppend(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "_append1" }, true);
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(1, res.getAffectedRows());
            ObTableValueFilter confirm_1 = compareVal(ObCompareOp.EQ, "c3", "row3_append0_append1");
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
        } finally {
            client.delete("test_query_filter_mutate", new Object[] { 0L });
            client.delete("test_query_filter_mutate", new Object[] { 1L });
            client.delete("test_query_filter_mutate", new Object[] { 2L });
        }
    }

    @Test
    public void testQueryAndIncrement() throws Exception {

        /* 
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样

        try {
            client.insert("test_query_filter_mutate", new Object[] { 0L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row1", 0L });
            client.insert("test_query_filter_mutate", new Object[] { 1L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row2", 10L });
            client.insert("test_query_filter_mutate", new Object[] { 2L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row3", 20L });

            TableQuery tableQuery = client.query("test_query_filter_mutate");
            /* Scan range must in one partition */
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.select("c1", "c2", "c3", "c4");

            /* Set Filter String */
            ObTableValueFilter filter_0 = compareVal(ObCompareOp.GT, "c1", 0);
            ObTableValueFilter filter_1 = compareVal(ObCompareOp.LT, "c3", "row3");

            try {
                ObTableQueryAndMutateRequest request = ((ObTableClient) client)
                    .obTableQueryAndIncrement(tableQuery, null, null, true);
                ObPayload res_exec = ((ObTableClient) client).execute(request);
            } catch (Exception e) {
                assertTrue(true);
            }
            tableQuery.setFilter(filter_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client)
                .obTableQueryAndIncrement(tableQuery, new String[] { "c4" }, new Object[] { 5L },
                    true);
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_0 = compareVal(ObCompareOp.GE, "c4", 15);
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            tableQuery.setFilter(filter_1);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client)
                .obTableQueryAndIncrement(tableQuery, new String[] { "c4" }, new Object[] { 7L },
                    true);
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_1 = compareVal(ObCompareOp.EQ, "c4", 22);
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
        } finally {
            client.delete("test_query_filter_mutate", new Object[] { 0L });
            client.delete("test_query_filter_mutate", new Object[] { 1L });
            client.delete("test_query_filter_mutate", new Object[] { 2L });
        }
    }

    @Test
    public void testQueryAndDelete() throws Exception {

        /* 
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样

        try {
            client.insert("test_query_filter_mutate", new Object[] { 0L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row1" });
            client.insert("test_query_filter_mutate", new Object[] { 1L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row2" });
            client.insert("test_query_filter_mutate", new Object[] { 2L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row3" });

            TableQuery tableQuery = client.query("test_query_filter_mutate");
            /* Scan range must in one partition */
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.select("c1", "c2", "c3");

            /* Set Filter String */
            ObTableValueFilter filter_0 = compareVal(ObCompareOp.EQ, "c1", 0);
            ObTableValueFilter filter_1 = new ObTableValueFilter(ObCompareOp.GT, "c3", "ro");
            ObTableValueFilter filter_2 = new ObTableValueFilter(ObCompareOp.LT, "c3", "row3");
            ObTableFilterList filterList = andList(filter_1, filter_2);
            ObTableValueFilter filter_3 = new ObTableValueFilter(ObCompareOp.LT, null, "row3");

            tableQuery.setFilter(filter_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client)
                .obTableQueryAndDelete(tableQuery);
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_0 = compareVal(ObCompareOp.GE, "c1", 0);
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            tableQuery.setFilter(filterList);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client)
                .obTableQueryAndDelete(tableQuery);
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_1 = compareVal(ObCompareOp.GE, "c1", 0);
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

            tableQuery.setFilter(filter_3);
            ObTableQueryAndMutateRequest request_2 = ((ObTableClient) client)
                .obTableQueryAndDelete(tableQuery);
            ObPayload res_exec_2 = ((ObTableClient) client).execute(request_2);
            res = (ObTableQueryAndMutateResult) res_exec_2;
            Assert.assertEquals(1, res.getAffectedRows());
        } finally {
            client.delete("test_query_filter_mutate", new Object[] { 0L });
            client.delete("test_query_filter_mutate", new Object[] { 1L });
            client.delete("test_query_filter_mutate", new Object[] { 2L });
        }
    }

    @Test
    public void testQueryAndUpdate() throws Exception {

        /* 
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样

        try {
            client.insert("test_query_filter_mutate", new Object[] { 0L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row1" });
            client.insert("test_query_filter_mutate", new Object[] { 1L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row2" });
            client.insert("test_query_filter_mutate", new Object[] { 2L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row3" });

            TableQuery tableQuery = client.query("test_query_filter_mutate");
            /* Scan range must in one partition */
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.select("c1", "c2", "c3");

            ObTableValueFilter filter_0 = compareVal(ObCompareOp.GT, "c1", 0);
            ObTableValueFilter filter_1 = compareVal(ObCompareOp.EQ, "c3", "update1");

            try {
                ObTableQueryAndMutateRequest request = ((ObTableClient) client)
                    .obTableQueryAndUpdate(tableQuery, null, null);
                ObPayload res_exec = ((ObTableClient) client).execute(request);
            } catch (Exception e) {
                assertTrue(true);
            }

            tableQuery.setFilter(filter_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client)
                .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "update1" });
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_0 = compareVal(ObCompareOp.EQ, "c3", "update1");
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            tableQuery.setFilter(filter_1);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client)
                .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "update2" });
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_1 = compareVal(ObCompareOp.EQ, "c3", "update2");
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            if (client instanceof ObTableClient && ((ObTableClient) client).isOdpMode()) {
                try {
                    tableQuery = client.query("test_query_filter_mutate");
                    tableQuery.select("c1", "c2", "c3");
                    tableQuery.setFilter(filter_0);
                    ObTableQueryAndMutateRequest request_2 = ((ObTableClient) client)
                        .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" },
                            new Object[] { new byte[] { 1 }, "update1" });
                    ObPayload res_exec_2 = ((ObTableClient) client).execute(request_2);
                    Assert.assertTrue(false);
                } catch (Exception e) {
                    Assert.assertTrue(e instanceof ObTableException);
                    Assert.assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode,
                        ((ObTableException) e).getErrorCode());
                }

            }
        } finally {
            client.delete("test_query_filter_mutate", new Object[] { 0L });
            client.delete("test_query_filter_mutate", new Object[] { 1L });
            client.delete("test_query_filter_mutate", new Object[] { 2L });
        }
    }

    @Test
    public void testQueryAndMutateComplex() throws Exception {

        /*
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样

        try {
            client.insert("test_query_filter_mutate", new Object[] { 0L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row1" });
            client.insert("test_query_filter_mutate", new Object[] { 1L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row2" });
            client.insert("test_query_filter_mutate", new Object[] { 2L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row3" });
            client.insert("test_query_filter_mutate", new Object[] { 3L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row4" });
            client.insert("test_query_filter_mutate", new Object[] { 4L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row5" });
            client.insert("test_query_filter_mutate", new Object[] { 5L }, new String[] { "c2",
                    "c3" }, new Object[] { new byte[] { 1 }, "row6" });
            client.insert("test_query_filter_mutate").setRowKey(new Row("c1", 10L))
                .addMutateColVal(new ColumnValue("c2", new byte[] { 1 }))
                .addMutateColVal(new ColumnValue("c3", "z_row"))
                .addMutateColVal(new ColumnValue("c4", 10L)).execute();

            TableQuery tableQuery = client.query("test_query_filter_mutate");
            /* Scan range must in one partition */
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.select("c1", "c2", "c3");

            ObTableValueFilter c1_GT_0 = compareVal(ObCompareOp.GT, "c1", 0);
            ObTableValueFilter c1_EQ_0 = compareVal(ObCompareOp.EQ, "c1", 0);
            ObTableValueFilter c1_LE_0 = compareVal(ObCompareOp.LE, "c1", 0);
            ObTableValueFilter c1_LT_5 = compareVal(ObCompareOp.LT, "c1", 5);
            ObTableValueFilter c1_LE_5 = compareVal(ObCompareOp.LE, "c1", 5);
            ObTableValueFilter c1_GT_3 = compareVal(ObCompareOp.GT, "c1", 3);
            ObTableValueFilter c1_LT_2 = compareVal(ObCompareOp.LT, "c1", 2);
            ObTableValueFilter c1_EQ_5 = compareVal(ObCompareOp.EQ, "c1", 5);
            ObTableValueFilter c3_GE = compareVal(ObCompareOp.GE, "c3", "update");
            ObTableValueFilter c3_LT = compareVal(ObCompareOp.LT, "c3", "update4");
            ObTableFilterList filters_0 = andList();
            ObTableFilterList filters_1 = andList();
            ObTableFilterList filters_2 = orList();

            // c1 = 0 && c1 = 0
            filters_0.addFilter(c1_EQ_0, c1_EQ_0);
            tableQuery.setFilter(filters_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client)
                .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "update1" });
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_0 = compareVal(ObCompareOp.EQ, "c3", "update1");
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

            // c1 = 0 && (c1 = 0 && c1 = 0)
            filters_1.addFilter(c1_EQ_0, filters_0);
            tableQuery.setFilter(filters_1);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client)
                .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "update2" });
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_1 = compareVal(ObCompareOp.EQ, "c3", "update2");
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

            // c1 = 5 || (c1 > 3 && c1 <= 5)
            filters_0 = andList(c1_GT_3, c1_LE_5);
            filters_1 = orList(c1_EQ_5, filters_0);
            tableQuery.setFilter(filters_1);
            ObTableQueryAndMutateRequest request_2 = ((ObTableClient) client)
                .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "update3" });
            ObPayload res_exec_2 = ((ObTableClient) client).execute(request_2);
            res = (ObTableQueryAndMutateResult) res_exec_2;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_2 = compareVal(ObCompareOp.EQ, "c3", "update3");
            tableQuery.setFilter(confirm_2);
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            // (c1 > 0 && c1 < 5) || (c1 <= 0 || c1 < 2)
            filters_0 = andList(c1_GT_0, c1_LT_5);
            filters_1 = orList(c1_LE_0, c1_LT_2);
            filters_2.addFilter(filters_0, filters_1);
            tableQuery.setFilter(filters_2);
            ObTableQueryAndMutateRequest request_3 = ((ObTableClient) client)
                .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "update4" });
            ObPayload res_exec_3 = ((ObTableClient) client).execute(request_3);
            res = (ObTableQueryAndMutateResult) res_exec_3;
            Assert.assertEquals(5, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_3 = compareVal(ObCompareOp.EQ, "c3", "update4");
            tableQuery.setFilter(confirm_3);
            result = tableQuery.execute();
            Assert.assertEquals(5, result.cacheSize());

            // (c3 >= update && c3 < update4 && c1 < 2) || (c3 < update4 && c1 > 3)
            filters_0 = andList(c3_GE, c3_LT, c1_LT_2);
            filters_1 = andList(c3_LT, c1_GT_3);
            filters_2 = orList(filters_0, filters_1);
            tableQuery.setFilter(filters_2);
            ObTableQueryAndMutateRequest request_4 = ((ObTableClient) client)
                .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "update5" });
            ObPayload res_exec_4 = ((ObTableClient) client).execute(request_4);
            res = (ObTableQueryAndMutateResult) res_exec_4;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_4 = compareVal(ObCompareOp.GE, "c3", "update5");
            tableQuery.setFilter(confirm_4);
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            // new test
            // match the filter
            ObTableValueFilter c3_EQ_rowX = compareVal(ObCompareOp.EQ, "c3", "z_row");
            MutationResult update_result = ((ObTableClient) client)
                .update("test_query_filter_mutate").setRowKey(colVal("c1", 10L))
                .setFilter(c3_EQ_rowX)
                .addMutateRow(row(colVal("c2", new byte[] { 1 }), colVal("c3", "update_ur")))
                .execute();
            Assert.assertEquals(1, update_result.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_ur = compareVal(ObCompareOp.EQ, "c3", "update_ur");
            tableQuery.setFilter(confirm_ur);
            QueryResultSet result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // do not match the filter
            update_result = ((ObTableClient) client).update("test_query_filter_mutate")
                .setRowKey(colVal("c1", 10L)).setFilter(c3_EQ_rowX)
                .addMutateRow(row(colVal("c2", new byte[] { 1 }), colVal("c3", "update_X")))
                .execute();
            Assert.assertEquals(0, update_result.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_x = compareVal(ObCompareOp.EQ, "c3", "update_X");
            tableQuery.setFilter(confirm_x);
            result_ = tableQuery.execute();
            Assert.assertEquals(0, result_.cacheSize());

        } finally {
            client.delete("test_query_filter_mutate", new Object[] { 0L });
            client.delete("test_query_filter_mutate", new Object[] { 1L });
            client.delete("test_query_filter_mutate", new Object[] { 2L });
            client.delete("test_query_filter_mutate", new Object[] { 3L });
            client.delete("test_query_filter_mutate", new Object[] { 4L });
            client.delete("test_query_filter_mutate", new Object[] { 5L });
            client.delete("test_query_filter_mutate", new Object[] { 10L });
        }
    }

    @Test
    public void testQueryFilter() throws Exception {

        /*
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样

        try {

            client.insert("test_query_filter_mutate", new Object[] { 0L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row1", 10L });
            client.insert("test_query_filter_mutate", new Object[] { 1L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row2", 11L });
            client.insert("test_query_filter_mutate", new Object[] { 2L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row3", 12L });
            client.insert("test_query_filter_mutate", new Object[] { 3L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row4", 13L });
            client.insert("test_query_filter_mutate", new Object[] { 4L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row5", 14L });
            client.insert("test_query_filter_mutate", new Object[] { 5L }, new String[] { "c2",
                    "c3", "c4" }, new Object[] { new byte[] { 1 }, "row6", 15L });

            TableQuery tableQuery = client.query("test_query_filter_mutate");
            /* Scan range must in one partition */
            tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
            tableQuery.select("c1", "c2", "c3", "c4");

            ObTableFilterList filterList;
            // in/notin null cases
            try {
                ObTableInFilter inFilter = in("", 5);
            } catch (Exception e) {
                assertTrue(true);
            }

            // in/notin null cases
            try {
                ObTableInFilter inFilter = in("xx", (Object) null);
            } catch (Exception e) {
                assertTrue(true);
            }

            // in/notin null cases
            try {
                ObTableNotInFilter notInFilter = notIn("", 5);
            } catch (Exception e) {
                assertTrue(true);
            }

            // in/notin null cases
            try {
                ObTableNotInFilter notInFilter = notIn("xx", (Object) null);
            } catch (Exception e) {
                assertTrue(true);
            }

            // c1 in {0(short), 1(int), 2(long)} and c4 not in { 11 }
            short num_16 = 0;
            int num_32 = 1;
            long num_64 = 2;
            filterList = andList(in("c1", num_16, num_32, num_64), notIn("c4", 11));
            tableQuery.setFilter(filterList);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client)
                .obTableQueryAndUpdate(tableQuery, new String[] { "c2", "c3" }, new Object[] {
                        new byte[] { 1 }, "update1" });
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            TableQuery confirmQuery = client.query("test_query_filter_mutate");
            confirmQuery.setFilter(compareVal(ObCompareOp.EQ, "c3", "update1"));
            // 查询结果集
            QueryResultSet result = confirmQuery.select("c1", "c2", "c3")
                .addScanRange(new Object[] { 0L }, new Object[] { 100L }).execute();
            long[] ans1 = { 0, 2 };
            for (int i = 0; i < 2; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get("c1"), ans1[i]);
                System.out.println("c1:" + value.get("c1"));
            }
            Assert.assertFalse(result.next());
        } finally {
            client.delete("test_query_filter_mutate", new Object[] { 0L });
            client.delete("test_query_filter_mutate", new Object[] { 1L });
            client.delete("test_query_filter_mutate", new Object[] { 2L });
            client.delete("test_query_filter_mutate", new Object[] { 3L });
            client.delete("test_query_filter_mutate", new Object[] { 4L });
            client.delete("test_query_filter_mutate", new Object[] { 5L });
            client.delete("test_query_filter_mutate", new Object[] { 6L });
        }
    }

    @Test
    // Test ObTableValueFilter with IS/IS_NOT compareOp
    public void testIsAndIsNot() throws Exception {

        /*
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样
        String[] allColumnNames = new String[] { "c1", "c2", "c3", "c4" };
        String[] columnNames = new String[] { "c2", "c3", "c4" };
        String tableName = "test_query_filter_mutate";
        Object[] c1 = new Object[] { 7L, 8L, 9L };
        Object[] c2 = new Object[] { null, null, new byte[] { 3 } };
        Object[] c3 = new Object[] { "row7", "row8", "row9" };
        Object[] c4 = new Object[] { 10L, null, null };
        try {
            for (int i = 0; i < c1.length; i++) {
                client.insert(tableName, c1[i], columnNames, new Object[] { c2[i], c3[i], c4[i] });
            }
            // IS compareOp with non-null value is not allowed
            try {
                ObTableFilter filter = compareVal(ObCompareOp.IS, "c1", "hello");
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }
            // IS_NOT compareOp with non-null value is not allowed
            try {
                ObTableFilter filter = compareVal(ObCompareOp.IS_NOT, "c1", "hello");
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }

            // query with c2 is null
            TableQuery tableQuery = client.query(tableName).select(allColumnNames)
                .addScanRange(new Object[] { 0L }, new Object[] { 200L })
                .setFilter(compareVal(ObCompareOp.IS, "c2", null));
            QueryResultSet result = tableQuery.execute();
            int expRowIdx[] = { 0, 1 };
            Assert.assertEquals(result.cacheSize(), expRowIdx.length);
            for (int i = 0; i < expRowIdx.length; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get(allColumnNames[0]), c1[expRowIdx[i]]);
                assertTrue(Arrays.equals((byte[]) value.get(allColumnNames[1]),
                    (byte[]) c2[expRowIdx[i]]));
                assertEquals(value.get(allColumnNames[2]), c3[expRowIdx[i]]);
                assertEquals(value.get(allColumnNames[3]), c4[expRowIdx[i]]);
            }
            Assert.assertFalse(result.next());

            // query with c2 is not null
            tableQuery = client.query(tableName).select(allColumnNames)
                .addScanRange(new Object[] { 0L }, new Object[] { 200L })
                .setFilter(compareVal(ObCompareOp.IS_NOT, "c2", null));
            result = tableQuery.execute();
            expRowIdx = new int[] { 2 };
            Assert.assertEquals(result.cacheSize(), expRowIdx.length);
            for (int i = 0; i < expRowIdx.length; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get(allColumnNames[0]), c1[expRowIdx[i]]);
                assertTrue(Arrays.equals((byte[]) value.get(allColumnNames[1]),
                    (byte[]) c2[expRowIdx[i]]));
                assertEquals(value.get(allColumnNames[2]), c3[expRowIdx[i]]);
                assertEquals(value.get(allColumnNames[3]), c4[expRowIdx[i]]);
            }
            Assert.assertFalse(result.next());

            // query with c2 is null and c4 equals to 10
            tableQuery = client
                .query(tableName)
                .select(allColumnNames)
                .addScanRange(new Object[] { 0L }, new Object[] { 200L })
                .setFilter(
                    andList(compareVal(ObCompareOp.IS, "c2", null),
                        compareVal(ObCompareOp.EQ, "c4", 10L)));
            result = tableQuery.execute();
            expRowIdx = new int[] { 0 };
            Assert.assertEquals(result.cacheSize(), expRowIdx.length);
            for (int i = 0; i < expRowIdx.length; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get(allColumnNames[0]), c1[expRowIdx[i]]);
                assertTrue(Arrays.equals((byte[]) value.get(allColumnNames[1]),
                    (byte[]) c2[expRowIdx[i]]));
                assertEquals(value.get(allColumnNames[2]), c3[expRowIdx[i]]);
                assertEquals(value.get(allColumnNames[3]), c4[expRowIdx[i]]);
            }
            Assert.assertFalse(result.next());

            // query with c2 is null or c4 is not null
            tableQuery = client
                .query(tableName)
                .select(allColumnNames)
                .addScanRange(new Object[] { 0L }, new Object[] { 200L })
                .setFilter(
                    orList(compareVal(ObCompareOp.IS, "c2", null),
                        compareVal(ObCompareOp.IS_NOT, "c4", null)));
            result = tableQuery.execute();
            expRowIdx = new int[] { 0, 1 };
            Assert.assertEquals(result.cacheSize(), expRowIdx.length);
            for (int i = 0; i < expRowIdx.length; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get(allColumnNames[0]), c1[expRowIdx[i]]);
                assertTrue(Arrays.equals((byte[]) value.get(allColumnNames[1]),
                    (byte[]) c2[expRowIdx[i]]));
                assertEquals(value.get(allColumnNames[2]), c3[expRowIdx[i]]);
                assertEquals(value.get(allColumnNames[3]), c4[expRowIdx[i]]);
            }
            Assert.assertFalse(result.next());

            // query with c2 is null and c4 is null
            tableQuery = client
                .query(tableName)
                .select(allColumnNames)
                .addScanRange(new Object[] { 0L }, new Object[] { 200L })
                .setFilter(
                    andList(compareVal(ObCompareOp.IS, "c2", null),
                        compareVal(ObCompareOp.IS, "c4", null)));
            result = tableQuery.execute();
            expRowIdx = new int[] { 1 };
            Assert.assertEquals(result.cacheSize(), expRowIdx.length);
            for (int i = 0; i < expRowIdx.length; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get(allColumnNames[0]), c1[expRowIdx[i]]);
                assertTrue(Arrays.equals((byte[]) value.get(allColumnNames[1]),
                    (byte[]) c2[expRowIdx[i]]));
                assertEquals(value.get(allColumnNames[2]), c3[expRowIdx[i]]);
                assertEquals(value.get(allColumnNames[3]), c4[expRowIdx[i]]);
            }
            Assert.assertFalse(result.next());
        } finally {
            for (int i = 0; i < c1.length; i++) {
                client.delete("test_query_filter_mutate", new Object[] { c1[i] });
            }
        }
    }

    @Test
    public void testCompareWithNull() throws Exception {
        /*
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */

        ObCompareOp ops[] = { ObCompareOp.LT, ObCompareOp.GT, ObCompareOp.LE, ObCompareOp.GE,
                ObCompareOp.NE, ObCompareOp.EQ };
        // valueFilter with null value is not allowed except IS/IS_NOT comapreOp
        for (ObCompareOp op : ops) {
            try {
                ObTableFilter filter = compareVal(op, "c1", null);
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }
        }

        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样
        String[] allColumnNames = new String[] { "c1", "c2", "c3", "c4" };
        String[] columnNames = new String[] { "c2", "c3", "c4" };
        String tableName = "test_query_filter_mutate";
        Object[] c1 = new Object[] { 10L };
        Object[] c2 = new Object[] { null };
        Object[] c3 = new Object[] { null };
        Object[] c4 = new Object[] { null };
        Object vals[] = { new byte[] { 3 }, "row100", 10L };

        try {
            for (int i = 0; i < c1.length; i++) {
                client.insert(tableName, c1[i], columnNames, new Object[] { c2[i], c3[i], c4[i] });
            }

            TableQuery tableQuery;
            QueryResultSet result;
            for (ObCompareOp op : ops) {
                tableQuery = client
                    .query(tableName)
                    .select(allColumnNames)
                    .addScanRange(new Object[] { 0L }, new Object[] { 200L })
                    .setFilter(
                        orList(compareVal(op, "c2", vals[0]), compareVal(op, "c3", vals[1]),
                            compareVal(op, "c4", vals[2])));
                result = tableQuery.execute();
                Assert.assertEquals(result.cacheSize(), 0);
            }

        } finally {
            for (int i = 0; i < c1.length; i++) {
                client.delete("test_query_filter_mutate", new Object[] { c1[i] });
            }
        }
    }

    @Test
    // Test Query with filter and limit
    public void testQueryFilterLimit() throws Exception {

        /*
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) DEFAULT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        ((ObTableClient) client)
            .addRowKeyElement("test_query_filter_mutate", new String[] { "c1" }); //同索引列的值一样
        String[] allColumnNames = new String[] { "c1", "c2", "c3", "c4" };
        String[] columnNames = new String[] { "c2", "c3", "c4" };
        String tableName = "test_query_filter_mutate";
        Object[] c1 = new Object[] { 11L, 12L, 13L, 14L, 15L };
        Object[] c2 = new Object[] { null, null, new byte[] { 3 }, new byte[] { 4 },
                new byte[] { 5 } };
        Object[] c3 = new Object[] { "row11", "row12", "row13", "row14", "row15" };
        Object[] c4 = new Object[] { 10L, null, null, null, null };
        try {
            for (int i = 0; i < c1.length; i++) {
                client.insert(tableName, c1[i], columnNames, new Object[] { c2[i], c3[i], c4[i] });
            }

            // only limit > 0 and offset >= 0 is allowed
            try {
                TableQuery tableQuery = client.query(tableName).select(allColumnNames)
                    .addScanRange(new Object[] { 0L }, new Object[] { 200L }).limit(0, 0)
                    .setFilter(compareVal(ObCompareOp.IS, "c4", null));
                QueryResultSet result = tableQuery.execute();
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                TableQuery tableQuery = client.query(tableName).select(allColumnNames)
                    .addScanRange(new Object[] { 0L }, new Object[] { 200L }).limit(-1, 1)
                    .setFilter(compareVal(ObCompareOp.IS, "c4", null));
                QueryResultSet result = tableQuery.execute();
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }
            // IS_NOT compareOp with non-null value is not allowed
            try {
                ObTableFilter filter = compareVal(ObCompareOp.IS_NOT, "c1", "hello");
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }

            // query with c4 is null ,limit is 2 and offset 1
            TableQuery tableQuery = client.query(tableName).select(allColumnNames)
                .addScanRange(new Object[] { 0L }, new Object[] { 200L }).limit(1, 2)
                .setFilter(compareVal(ObCompareOp.IS, "c4", null));
            QueryResultSet result = tableQuery.execute();
            int expRowIdx[] = { 2, 3 };
            Assert.assertEquals(result.cacheSize(), expRowIdx.length);
            for (int i = 0; i < expRowIdx.length; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get(allColumnNames[0]), c1[expRowIdx[i]]);
                assertTrue(Arrays.equals((byte[]) value.get(allColumnNames[1]),
                    (byte[]) c2[expRowIdx[i]]));
                assertEquals(value.get(allColumnNames[2]), c3[expRowIdx[i]]);
                assertEquals(value.get(allColumnNames[3]), c4[expRowIdx[i]]);
            }
            Assert.assertFalse(result.next());

            // query with c3 > "row12" ,limit 1 and offset 1
            tableQuery = client.query(tableName).select(allColumnNames)
                .addScanRange(new Object[] { 0L }, new Object[] { 200L }).limit(1, 1)
                .setFilter(compareVal(ObCompareOp.GT, "c3", "row12"));
            result = tableQuery.execute();
            expRowIdx = new int[] { 3 };
            Assert.assertEquals(result.cacheSize(), expRowIdx.length);
            for (int i = 0; i < expRowIdx.length; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get(allColumnNames[0]), c1[expRowIdx[i]]);
                assertTrue(Arrays.equals((byte[]) value.get(allColumnNames[1]),
                    (byte[]) c2[expRowIdx[i]]));
                assertEquals(value.get(allColumnNames[2]), c3[expRowIdx[i]]);
                assertEquals(value.get(allColumnNames[3]), c4[expRowIdx[i]]);
            }
            Assert.assertFalse(result.next());

            // query with c3 != 'row13' and c4 is null, limit 1000
            tableQuery = client
                .query(tableName)
                .select(allColumnNames)
                .addScanRange(new Object[] { 0L }, new Object[] { 200L })
                .limit(1000)
                .setFilter(
                    andList(compareVal(ObCompareOp.NE, "c3", "row13"),
                        compareVal(ObCompareOp.IS, "c4", null)));

            result = tableQuery.execute();
            expRowIdx = new int[] { 1, 3, 4 };
            Assert.assertEquals(result.cacheSize(), expRowIdx.length);
            for (int i = 0; i < expRowIdx.length; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get(allColumnNames[0]), c1[expRowIdx[i]]);
                assertTrue(Arrays.equals((byte[]) value.get(allColumnNames[1]),
                    (byte[]) c2[expRowIdx[i]]));
                assertEquals(value.get(allColumnNames[2]), c3[expRowIdx[i]]);
                assertEquals(value.get(allColumnNames[3]), c4[expRowIdx[i]]);
            }
            Assert.assertFalse(result.next());
        } finally {
            for (int i = 0; i < c1.length; i++) {
                client.delete("test_query_filter_mutate", new Object[] { c1[i] });
            }
        }
    }

    @Test
    public void testMutation() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        TableQuery tableQuery = client.query("test_mutation");
        tableQuery.addScanRange(new Object[] { 0L, "\0" }, new Object[] { 200L, "\254" });
        tableQuery.select("c1", "c2", "c3", "c4");

        try {
            // prepare data with insert
            client.insert("test_mutation").setRowKey(row(colVal("c1", 0L), colVal("c2", "row_0")))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 100L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 1L), colVal("c2", "row_1"))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 101L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 2L), colVal("c2", "row_2"))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 102L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 103L)).execute();

            // update / match filter
            ObTableValueFilter c4_EQ_100 = compareVal(ObCompareOp.EQ, "c4", 100L);
            MutationResult updateResult = client.update("test_mutation")
                .setRowKey(colVal("c1", 0L), colVal("c2", "row_0")).setFilter(c4_EQ_100)
                .addMutateRow(row(colVal("c3", new byte[] { 1 }), colVal("c4", 200L))).execute();
            Assert.assertEquals(1, updateResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_1 = compareVal(ObCompareOp.EQ, "c4", 200L);
            tableQuery.setFilter(confirm_1);
            QueryResultSet result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // update / do not match filter
            updateResult = client.update("test_mutation")
                .setRowKey(colVal("c1", 1L), colVal("c2", "row_1")).setFilter(c4_EQ_100)
                .addMutateRow(row(colVal("c3", new byte[] { 1 }), colVal("c4", 201L))).execute();
            Assert.assertEquals(0, updateResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_2 = compareVal(ObCompareOp.EQ, "c4", 201L);
            tableQuery.setFilter(confirm_2);
            result_ = tableQuery.execute();
            Assert.assertEquals(0, result_.cacheSize());

            // delete / do not match filter
            ObTableValueFilter c4_EQ_103 = compareVal(ObCompareOp.EQ, "c4", 103L);
            MutationResult deleteResult = client.delete("test_mutation")
                .setRowKey(colVal("c1", 2L), colVal("c2", "row_2")).setFilter(c4_EQ_103).execute();
            Assert.assertEquals(0, deleteResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            tableQuery.setFilter(c4_EQ_103);
            result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // replace
            MutationResult replaceResult = client.replace("test_mutation")
                .setRowKey(colVal("c1", 2L), colVal("c2", "row_2"))
                .addMutateRow(row(colVal("c3", new byte[] { 2 }), colVal("c4", 202L))).execute();
            Assert.assertEquals(2, replaceResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_3 = compareVal(ObCompareOp.EQ, "c4", 202L);
            tableQuery.setFilter(confirm_3);
            result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // InsertOrUpdate / Insert
            MutationResult insertOrUpdateResult = client.insertOrUpdate("test_mutation")
                .setRowKey(colVal("c1", 4L), colVal("c2", "row_4"))
                .addMutateRow(row(colVal("c3", new byte[] { 2 }), colVal("c4", 104L))).execute();
            Assert.assertEquals(1, insertOrUpdateResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_4 = compareVal(ObCompareOp.EQ, "c4", 104L);
            tableQuery.setFilter(confirm_4);
            result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // InsertOrUpdate / Update
            insertOrUpdateResult = client.insertOrUpdate("test_mutation")
                .setRowKey(colVal("c1", 4L), colVal("c2", "row_4"))
                .addMutateRow(row(colVal("c3", new byte[1]), colVal("c4", 104L))).execute();
            Assert.assertEquals(1, insertOrUpdateResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            confirm_4 = compareVal(ObCompareOp.EQ, "c4", 104L);
            tableQuery.setFilter(confirm_4);
            result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // increment / without filter
            // result will send back the latest mutated column
            MutationResult incrementResult = client.increment("test_mutation")
                .setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                .addMutateColVal(colVal("c4", 100L)).execute();
            Assert.assertEquals(1, incrementResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_5 = compareVal(ObCompareOp.EQ, "c4", 203L);
            tableQuery.setFilter(confirm_5);
            result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // increment / with filter hit
            // result will send back the value before increment
            incrementResult = client.increment("test_mutation")
                .setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                .addMutateColVal(colVal("c4", 100L))
                .setFilter(andList(compareVal(ObCompareOp.EQ, "c4", 203L))).execute();
            Assert.assertEquals(1, incrementResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_6 = compareVal(ObCompareOp.EQ, "c4", 303L);
            tableQuery.setFilter(confirm_6);
            result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // increment / with filter not hit
            incrementResult = client.increment("test_mutation")
                .setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                .addMutateColVal(colVal("c4", 100L))
                .setFilter(andList(compareVal(ObCompareOp.EQ, "c4", 203L))).execute();
            Assert.assertEquals(0, incrementResult.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            ObTableValueFilter confirm_7 = compareVal(ObCompareOp.EQ, "c4", 303L);
            tableQuery.setFilter(confirm_7);
            result_ = tableQuery.execute();
            Assert.assertEquals(1, result_.cacheSize());

            // append / without filter
            // result will send back the latest mutated column
            MutationResult appendResult = client.append("test_mutation")
                .setRowKey(colVal("c1", 4L), colVal("c2", "row_4"))
                .addMutateColVal(colVal("c3", new byte[1])).execute();
            Assert.assertEquals(1, appendResult.getAffectedRows());

            // append / with filter hit
            // result will send back the value before append
            appendResult = client.append("test_mutation")
                .setRowKey(colVal("c1", 4L), colVal("c2", "row_4"))
                .addMutateColVal(colVal("c3", new byte[1]))
                .setFilter(andList(compareVal(ObCompareOp.EQ, "c4", 104L))).execute();
            Assert.assertEquals(1, appendResult.getAffectedRows());

            // append / with filter not hit
            appendResult = client.append("test_mutation")
                .setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                .addMutateColVal(colVal("c3", new byte[0]))
                .setFilter(andList(compareVal(ObCompareOp.EQ, "c4", 203L))).execute();
            Assert.assertEquals(0, appendResult.getAffectedRows());

            // increment non-integer column without filter
            try {
                incrementResult = client.increment("test_mutation")
                    .setRowKey(colVal("c1", 0L), colVal("c2", "row_0"))
                    .addMutateColVal(colVal("c3", 100L)).execute();
                Assert.assertTrue(false);
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_OBJ_TYPE_ERROR.errorCode,
                    ((ObTableException) e).getErrorCode());
            }
            // increment non-integer column with filter
            try {
                incrementResult = client.increment("test_mutation")
                    .setRowKey(colVal("c1", 0L), colVal("c2", "row_0"))
                    .addMutateColVal(colVal("c3", 100L))
                    .setFilter(compareVal(ObCompareOp.EQ, "c4", 200L)).execute();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_OBJ_TYPE_ERROR.errorCode,
                    ((ObTableException) e).getErrorCode());
            }

            // increment integer column with string
            try {
                incrementResult = client.increment("test_mutation")
                    .setRowKey(colVal("c1", 0L), colVal("c2", "row_0"))
                    .addMutateColVal(colVal("c4", "hello world"))
                    .setFilter(compareVal(ObCompareOp.EQ, "c4", 200L)).execute();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_OBJ_TYPE_ERROR.errorCode,
                    ((ObTableException) e).getErrorCode());
            }

            // append non-string column without filter
            try {
                appendResult = client.append("test_mutation")
                    .setRowKey(colVal("c1", 0L), colVal("c2", "row_0"))
                    .addMutateColVal(colVal("c4", new byte[1])).execute();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_OBJ_TYPE_ERROR.errorCode,
                    ((ObTableException) e).getErrorCode());
            }

            // append non-string column with filter
            try {
                appendResult = client.append("test_mutation")
                    .setRowKey(colVal("c1", 0L), colVal("c2", "row_0"))
                    .addMutateColVal(colVal("c4", new byte[1]))
                    .setFilter(compareVal(ObCompareOp.EQ, "c4", 200L)).execute();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_OBJ_TYPE_ERROR.errorCode,
                    ((ObTableException) e).getErrorCode());
            }

            // append string column with integer
            try {
                appendResult = client.append("test_mutation")
                    .setRowKey(colVal("c1", 0L), colVal("c2", "row_0"))
                    .addMutateColVal(colVal("c3", new byte[1]))
                    .setFilter(compareVal(ObCompareOp.EQ, "c4", 200L)).execute();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTableException);
                Assert.assertEquals(ResultCodes.OB_OBJ_TYPE_ERROR.errorCode,
                    ((ObTableException) e).getErrorCode());
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
            client.delete("test_mutation").setRowKey(colVal("c1", 4L), colVal("c2", "row_4"))
                .execute();
        }
    }

    @Test
    public void testBatchMutation() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        TableQuery tableQuery = client.query("test_mutation");
        tableQuery.addScanRange(new Object[] { 0L, "\0" }, new Object[] { 200L, "\254" });
        tableQuery.select("c1", "c2", "c3", "c4");

        try {
            // prepare data with insert
            client.insert("test_mutation").setRowKey(row(colVal("c1", 0L), colVal("c2", "row_0")))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 100L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 1L), colVal("c2", "row_1"))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 101L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 2L), colVal("c2", "row_2"))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 102L)).execute();
            client.insert("test_mutation").setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 103L)).execute();

            Insert insert_0 = insert().setRowKey(row(colVal("c1", 0L), colVal("c2", "row_0")))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 100L));
            Insert insert_1 = insert().setRowKey(row(colVal("c1", 4L), colVal("c2", "row_4")))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 104L));
            Update update_0 = update().setRowKey(row(colVal("c1", 4L), colVal("c2", "row_4")))
                .addMutateColVal(colVal("c3", new byte[] { 1 }))
                .addMutateColVal(colVal("c4", 204L));
            TableQuery query_0 = query().setRowKey(row(colVal("c1", 4L), colVal("c2", "row_4")))
                .select("c3", "c4");

            BatchOperationResult batchResult = client.batchOperation("test_mutation")
                .addOperation(insert_0, insert_1, update_0).addOperation(query_0).execute();
            Assert.assertEquals(1, batchResult.getWrongCount());
            Assert.assertEquals(3, batchResult.getCorrectCount());
            Assert.assertEquals(0, batchResult.getWrongIdx()[0]);
            Assert.assertEquals(1, batchResult.getCorrectIdx()[0]);
            Assert.assertEquals(1, batchResult.get(1).getAffectedRows());
            Assert.assertEquals(1, batchResult.get(2).getAffectedRows());
            OperationResult opResult = batchResult.get(3);
            Assert.assertEquals(204L, opResult.getOperationRow().get("c4"));
            Assert.assertEquals(204L, batchResult.get(3).getOperationRow().get("c4"));
            opResult = batchResult.get(2);
            Assert.assertNull(opResult.getOperationRow().get("c4"));
            Assert.assertNull(batchResult.get(2).getOperationRow().get("c4"));

            long c1Vals[] = { 0L, 1L, 2L };
            String c2Vals[] = { "row_0", "row_1", "row_2" };
            byte[] c3Val = new byte[] { 1 };
            long c4Vals[] = { 100L, 101L, 102L };
            BatchOperation batchOperation = client.batchOperation("test_mutation");
            for (int i = 0; i < c1Vals.length; i++) {
                Row rowKey1 = row(colVal("c1", c1Vals[i]), colVal("c2", c2Vals[i]));
                TableQuery query = query().setRowKey(rowKey1);
                batchOperation.addOperation(query);
            }
            BatchOperationResult result = batchOperation.execute();
            for (int i = 0; i < c2Vals.length; i++) {
                Row row = result.get(i).getOperationRow();
                Assert.assertEquals(4, row.size());
                Assert.assertEquals(c1Vals[i], row.get("c1"));
                Assert.assertEquals(c2Vals[i], row.get("c2"));
                Assert.assertTrue(Arrays.equals(c3Val, (byte[]) row.get("c3")));
                Assert.assertEquals(c4Vals[i], row.get("c4"));
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            client.delete("test_mutation").setRowKey(colVal("c1", 0L), colVal("c2", "row_0"))
                .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 1L), colVal("c2", "row_1"))
                .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 2L), colVal("c2", "row_2"))
                .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 3L), colVal("c2", "row_3"))
                .execute();
            client.delete("test_mutation").setRowKey(colVal("c1", 4L), colVal("c2", "row_4"))
                .execute();
        }
    }

    @Test
    public void testMutationWithScanRange() throws Exception {

        //        CREATE TABLE `test_mutation_with_range` (
        //            `c1` bigint NOT NULL,
        //            `c1sk` varchar(20) DEFAULT NULL,
        //            `c2` varbinary(1024) DEFAULT NULL,
        //            `c3` varchar(20) DEFAULT NULL,
        //            `c4` bigint DEFAULT NULL,
        //                    PRIMARY KEY(`c1`, `c1sk`)) partition by range columns (`c1`) (
        //                    PARTITION p0 VALUES LESS THAN (300),
        //                    PARTITION p1 VALUES LESS THAN (1000),
        //                    PARTITION p2 VALUES LESS THAN MAXVALUE);

        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        try {
            ((ObTableClient) client).addRowKeyElement("test_mutation_with_range", new String[] {
                    "c1", "c1sk" });
            client.insert("test_mutation_with_range", new Object[] { 0L, "c0" }, new String[] {
                    "c2", "c3" }, new Object[] { new byte[] { 1 }, "row1" });
            client.insert("test_mutation_with_range", new Object[] { 1L, "c1" }, new String[] {
                    "c2", "c3" }, new Object[] { new byte[] { 1 }, "row2" });
            client.insert("test_mutation_with_range", new Object[] { 2L, "c2" }, new String[] {
                    "c2", "c3" }, new Object[] { new byte[] { 1 }, "row3" });
            client.insert("test_mutation_with_range", new Object[] { 3L, "c3" }, new String[] {
                    "c2", "c3", "c4" }, new Object[] { new byte[] { 1 }, "row4", 4L });
            client.insert("test_mutation_with_range", new Object[] { 4L, "c4" }, new String[] {
                    "c2", "c3", "c4" }, new Object[] { new byte[] { 1 }, "row5", 4L });
            client.insert("test_mutation_with_range", new Object[] { 5L, "c5" }, new String[] {
                    "c2", "c3" }, new Object[] { new byte[] { 1 }, "row6" });
            client.insert("test_mutation_with_range")
                .setRowKey(row(colVal("c1", 10L), colVal("c1sk", "c10")))
                .addMutateColVal(new ColumnValue("c2", new byte[] { 1 }))
                .addMutateColVal(new ColumnValue("c3", "z_row"))
                .addMutateColVal(new ColumnValue("c4", 10L)).execute();

            TableQuery tableQuery = client.query("test_mutation_with_range");
            /* Scan range must in one partition */
            tableQuery.addScanRange(new Object[] { 0L, "A" }, new Object[] { 200L, "z" });
            tableQuery.select("c1", "c2", "c3", "c4");

            ObTableValueFilter c1_EQ_0 = compareVal(ObCompareOp.EQ, "c1", 0);
            ObTableValueFilter c1_LE_4 = compareVal(ObCompareOp.LE, "c1", 4);
            ObTableValueFilter c1_GE_3 = compareVal(ObCompareOp.GE, "c1", 3);
            ObTableFilterList filters_0 = andList();
            ObTableFilterList filters_1 = andList();

            // c1 = 0 && c1 = 0
            if (((ObTableClient) client).isOdpMode()) {
                filters_0.addFilter(c1_EQ_0, c1_EQ_0);
                MutationResult updateResult = client.update("test_mutation_with_range")
                    .setFilter(filters_0)
                    .addMutateRow(row(colVal("c2", new byte[] { 1 }), colVal("c3", "update1")))
                    .setScanRangeColumns("c1")
                    .addScanRange(new Object[] { 0L }, new Object[] { 200L }).execute();
                Assert.assertEquals(1, updateResult.getAffectedRows());
                /* To confirm changing. re-query to get the latest data */
                ObTableValueFilter confirm_0 = compareVal(ObCompareOp.EQ, "c3", "update1");
                tableQuery.setFilter(confirm_0);
                QueryResultSet result = tableQuery.execute();
                Assert.assertEquals(1, result.cacheSize());
            } else {
                filters_0.addFilter(c1_EQ_0, c1_EQ_0);
                MutationResult updateResult = client.update("test_mutation_with_range")
                    .setFilter(filters_0)
                    .addMutateRow(row(colVal("c2", new byte[] { 1 }), colVal("c3", "update1")))
                    .setScanRangeColumns("c1", "c1sk")
                    .addScanRange(new Object[] { 0L, "A" }, new Object[] { 200L, "z" }).execute();
                Assert.assertEquals(1, updateResult.getAffectedRows());
                /* To confirm changing. re-query to get the latest data */
                ObTableValueFilter confirm_0 = compareVal(ObCompareOp.EQ, "c3", "update1");
                tableQuery.setFilter(confirm_0);
                QueryResultSet result = tableQuery.execute();
                Assert.assertEquals(1, result.cacheSize());
            }

            // c1 >= 3 && c1 <= 4 ( scan range c1 > 4 )
            if (((ObTableClient) client).isOdpMode()) {
                filters_1.addFilter(c1_GE_3, c1_LE_4);
                MutationResult updateResult = client.increment("test_mutation_with_range")
                    .setFilter(filters_1).addMutateRow(row(colVal("c4", 100)))
                    .setScanRangeColumns("c1")
                    .addScanRange(new Object[] { 4L }, new Object[] { 200L }).execute();
                Assert.assertEquals(1, updateResult.getAffectedRows());
                /* To confirm changing. re-query to get the latest data */
                ObTableValueFilter confirm_1 = compareVal(ObCompareOp.EQ, "c4", 104L);
                tableQuery.setFilter(confirm_1);
                QueryResultSet result = tableQuery.execute();
                Assert.assertEquals(1, result.cacheSize());
            } else {
                filters_1.addFilter(c1_GE_3, c1_LE_4);
                MutationResult updateResult = client.increment("test_mutation_with_range")
                    .setFilter(filters_1).addMutateRow(row(colVal("c4", 100)))
                    .setScanRangeColumns("c1", "c1sk")
                    .addScanRange(new Object[] { 4L, "A" }, new Object[] { 200L, "z" }).execute();
                Assert.assertEquals(1, updateResult.getAffectedRows());
                /* To confirm changing. re-query to get the latest data */
                ObTableValueFilter confirm_1 = compareVal(ObCompareOp.EQ, "c4", 104L);
                tableQuery.setFilter(confirm_1);
                QueryResultSet result = tableQuery.execute();
                Assert.assertEquals(1, result.cacheSize());
            }

            // only scan
            if (((ObTableClient) client).isOdpMode()) {
                MutationResult updateResult = client.update("test_mutation_with_range")
                    .addMutateRow(row(colVal("c2", new byte[] { 1 }), colVal("c3", "update2")))
                    .setScanRangeColumns("c1")
                    .addScanRange(new Object[] { 4L }, new Object[] { 9L }).execute();
                Assert.assertEquals(2, updateResult.getAffectedRows());
                /* To confirm changing. re-query to get the latest data */
                ObTableValueFilter confirm_2 = compareVal(ObCompareOp.EQ, "c3", "update2");
                tableQuery.setFilter(confirm_2);
                QueryResultSet result = tableQuery.execute();
                Assert.assertEquals(2, result.cacheSize());

                try {
                    updateResult = client.update("test_mutation_with_range")
                        .addMutateRow(row(colVal("c2", new byte[] { 1 }), colVal("c3", "update2")))
                        .setFilter(filters_0).execute();
                    Assert.assertTrue(false);
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.assertTrue(e instanceof ObTableException);
                    Assert.assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode,
                        ((ObTableException) e).getErrorCode());
                }
            } else {
                MutationResult updateResult = client.update("test_mutation_with_range")
                    .addMutateRow(row(colVal("c2", new byte[] { 1 }), colVal("c3", "update2")))
                    .setScanRangeColumns("c1", "c1sk")
                    .addScanRange(new Object[] { 4L, "A" }, new Object[] { 9L, "z" }).execute();
                Assert.assertEquals(2, updateResult.getAffectedRows());
                /* To confirm changing. re-query to get the latest data */
                ObTableValueFilter confirm_2 = compareVal(ObCompareOp.EQ, "c3", "update2");
                tableQuery.setFilter(confirm_2);
                QueryResultSet result = tableQuery.execute();
                Assert.assertEquals(2, result.cacheSize());
            }
        } finally {
            client.delete("test_mutation_with_range", new Object[] { 0L, "c0" });
            client.delete("test_mutation_with_range", new Object[] { 1L, "c1" });
            client.delete("test_mutation_with_range", new Object[] { 2L, "c2" });
            client.delete("test_mutation_with_range", new Object[] { 3L, "c3" });
            client.delete("test_mutation_with_range", new Object[] { 4L, "c4" });
            client.delete("test_mutation_with_range", new Object[] { 5L, "c5" });
            client.delete("test_mutation_with_range", new Object[] { 10L, "c10" });
        }
    }
}

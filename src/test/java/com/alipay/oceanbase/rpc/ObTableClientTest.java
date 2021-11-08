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
import com.alipay.oceanbase.rpc.location.model.ObServerAddr;
import com.alipay.oceanbase.rpc.location.model.ServerRoster;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ObTableClientTest extends ObTableClientTestBase {
    @Before
    public void setup() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty("connectTimeout", "100000");
        obTableClient.addProperty("socketTimeout", "100000");
        obTableClient.addProperty("table.connection.pool.size", "3");
        obTableClient.init();

        client = obTableClient;
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

}

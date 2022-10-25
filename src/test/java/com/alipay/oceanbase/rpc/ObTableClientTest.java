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
import com.alipay.oceanbase.rpc.filter.obCompareOp;
import com.alipay.oceanbase.rpc.filter.obTableFilterList;
import com.alipay.oceanbase.rpc.filter.obTableValueFilter;
import com.alipay.oceanbase.rpc.location.model.ObServerAddr;
import com.alipay.oceanbase.rpc.location.model.ServerRoster;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;

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
        obTableClient.addProperty("connectTimeout", "100000");
        obTableClient.addProperty("socketTimeout", "100000");
        obTableClient.addProperty("table.connection.pool.size", "3");
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

            obTableClient.insert("test_increment", "test_normal", new String[]{"c2", "c3"},
                    new Object[]{1, 2});
            Map<String, Object> res = obTableClient.increment("test_increment", "test_normal",
                    new String[]{"c2", "c3"}, new Object[]{1, 2}, true);
            Assert.assertEquals(4, res.get("c3"));
            Assert.assertEquals(2, res.get("c2"));

            obTableClient.insert("test_increment", "test_null", new String[]{"c2", "c3"},
                    new Object[]{null, null});
            res = obTableClient.increment("test_increment", "test_null",
                    new String[]{"c2", "c3"}, new Object[]{1, 2}, true);
            Assert.assertEquals(2, res.get("c3"));
            Assert.assertEquals(1, res.get("c2"));

            res = obTableClient.increment("test_increment", "test_empty",
                    new String[]{"c2", "c3"}, new Object[]{1, 2}, true);
            Assert.assertEquals(2, res.get("c3"));
            Assert.assertEquals(1, res.get("c2"));

            res = obTableClient.increment("test_increment", "test_empty",
                    new String[]{"c2", "c3"}, new Object[]{1, 2}, false);
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

            client.insert("test_append", "test_normal", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "P"});
            Map<String, Object> res = client.append("test_append", "test_normal", new String[]{
                    "c2", "c3"}, new Object[]{new byte[]{2}, "Y"}, true);

            Assert.assertTrue(Arrays.equals(new byte[]{1, 2}, (byte[]) res.get("c2")));
            Assert.assertEquals("PY", res.get("c3"));

            client.insert("test_append", "test_null", new String[]{"c2", "c3"}, new Object[]{
                    null, null});
            res = client.append("test_append", "test_null", new String[]{"c2", "c3"},
                    new Object[]{new byte[]{1}, "P"}, true);
            Assert.assertTrue(Arrays.equals(new byte[]{1}, (byte[]) res.get("c2")));
            Assert.assertEquals("P", res.get("c3"));

            res = client.append("test_append", "test_empty", new String[]{"c2", "c3"},
                    new Object[]{new byte[]{1}, "P"}, true);
            Assert.assertTrue(Arrays.equals(new byte[]{1}, (byte[]) res.get("c2")));
            Assert.assertEquals("P", res.get("c3"));

            res = client.append("test_append", "test_empty", new String[]{"c2", "c3"},
                    new Object[]{new byte[]{1}, "P"}, false);
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

            obTableClient.insert("test_increment", "test_normal", new String[]{"c2", "c3"},
                    new Object[]{1, 2});
            TableBatchOps batchOps = obTableClient.batch("test_increment");
            batchOps.get("test_normal", new String[]{"c2", "c3"});
            batchOps.insert("test_insert", new String[]{"c2", "c3"}, new Object[]{2, 4});
            batchOps.increment("test_normal", new String[]{"c2", "c3"}, new Object[]{1, 2},
                    true);
            batchOps.increment("test_insert", new String[]{"c2", "c3"}, new Object[]{2, 4},
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
            batchOps.get("test_normal", new String[]{"c2", "c3"});
            batchOps.insert("test_insert_two", new String[]{"c2", "c3"}, new Object[]{2, 4});
            batchOps.increment("test_normal", new String[]{"c2", "c3"}, new Object[]{1, 2},
                    false);
            batchOps.increment("test_insert", new String[]{"c2", "c3"}, new Object[]{2, 4},
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

            obTableClient.insert("test_append", "test_normal", new String[]{"c2", "c3"},
                    new Object[]{new byte[]{1}, "P"});

            batchOps = obTableClient.batch("test_append");
            batchOps.get("test_normal", new String[]{"c2", "c3"});
            batchOps.insert("test_append", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{2}, "Q"});
            batchOps.append("test_normal", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "P"}, true);
            batchOps.append("test_append", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{2}, "Q"}, true);
            res = batchOps.execute();
            Assert.assertTrue(res.get(0) instanceof Map);
            Assert
                    .assertTrue(Arrays.equals(new byte[]{1}, (byte[]) ((Map) res.get(0)).get("c2")));
            Assert.assertEquals("P", ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[]{1, 1},
                    (byte[]) ((Map) res.get(2)).get("c2")));
            Assert.assertEquals("PP", ((Map) res.get(2)).get("c3"));
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[]{2, 2},
                    (byte[]) ((Map) res.get(3)).get("c2")));
            Assert.assertEquals("QQ", ((Map) res.get(3)).get("c3"));

            batchOps = obTableClient.batch("test_append");
            batchOps.get("test_normal", new String[]{"c2", "c3"});
            batchOps.insert("test_append_two", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{2}, "Q"});
            batchOps.append("test_normal", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "P"}, false);
            batchOps.append("test_append", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{2}, "Q"}, false);
            res = batchOps.execute();
            Assert.assertTrue(res.get(0) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[]{1, 1},
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

            obTableClient.insert("test_increment", "test_normal", new String[]{"c2", "c3"},
                    new Object[]{1, 2});
            TableBatchOps batchOps = obTableClient.batch("test_increment");
            batchOps.get("test_normal", new String[]{"c2", "c3"});
            batchOps.insert("test_insert", new String[]{"c2", "c3"}, new Object[]{2, 4});
            batchOps.increment("test_normal", new String[]{"c2", "c3"}, new Object[]{1, 2},
                    true);
            batchOps.increment("test_insert", new String[]{"c2", "c3"}, new Object[]{2, 4},
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
            batchOps.get("test_normal", new String[]{"c2", "c3"});
            batchOps.insert("test_insert_two", new String[]{"c2", "c3"}, new Object[]{2, 4});
            batchOps.increment("test_normal", new String[]{"c2", "c3"}, new Object[]{1, 2},
                    false);
            batchOps.increment("test_insert", new String[]{"c2", "c3"}, new Object[]{2, 4},
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

            obTableClient.insert("test_append", "test_normal", new String[]{"c2", "c3"},
                    new Object[]{new byte[]{1}, "P"});

            batchOps = obTableClient.batch("test_append");
            batchOps.get("test_normal", new String[]{"c2", "c3"});
            batchOps.insert("test_append", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{2}, "Q"});
            batchOps.append("test_normal", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "P"}, true);
            batchOps.append("test_append", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{2}, "Q"}, true);
            res = batchOps.execute();
            Assert.assertTrue(res.get(0) instanceof Map);
            Assert
                    .assertTrue(Arrays.equals(new byte[]{1}, (byte[]) ((Map) res.get(0)).get("c2")));
            Assert.assertEquals("P", ((Map) res.get(0)).get("c3"));
            Assert.assertTrue(res.get(1) instanceof Long);
            Assert.assertEquals(1L, res.get(1));
            Assert.assertTrue(res.get(2) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[]{1, 1},
                    (byte[]) ((Map) res.get(2)).get("c2")));
            Assert.assertEquals("PP", ((Map) res.get(2)).get("c3"));
            Assert.assertTrue(res.get(3) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[]{2, 2},
                    (byte[]) ((Map) res.get(3)).get("c2")));
            Assert.assertEquals("QQ", ((Map) res.get(3)).get("c3"));

            batchOps = obTableClient.batch("test_append");
            batchOps.get("test_normal", new String[]{"c2", "c3"});
            batchOps.insert("test_append_two", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{2}, "Q"});
            batchOps.append("test_normal", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "P"}, false);
            batchOps.append("test_append", new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{2}, "Q"}, false);
            res = batchOps.execute();
            Assert.assertTrue(res.get(0) instanceof Map);
            Assert.assertTrue(Arrays.equals(new byte[]{1, 1},
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
                    new String[]{"c2"}, new String[]{"bar"}));

            // sleep 2000 ms to let the server addr expired.
            Thread.sleep(2000);
            Map<String, Object> values = obTableClient.get("test_varchar_table", "bar",
                    new String[]{"c2"});
            assertNotNull(values);
            assertEquals(0, values.size());

            values = obTableClient.get("test_varchar_table", "foo", new String[]{"c2"});
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
            obTableValueFilter filter_0 = new obTableValueFilter(obCompareOp.EQ, "c3", "value");
            assertEquals("TableCompareFilter(=, 'c3:value')", filter_0.toString());
            obTableFilterList filterList = new obTableFilterList(obTableFilterList.operator.AND);
            obTableValueFilter filter_1 = new obTableValueFilter(obCompareOp.LE, "c2", 5);
            filterList.addFilter(filter_0);
            filterList.addFilter(filter_1);
            assertEquals("TableCompareFilter(=, 'c3:value') && TableCompareFilter(<=, 'c2:5')", filterList.toString());
            // test param null
            obTableValueFilter filter = new obTableValueFilter(obCompareOp.NE, null, null);
            obTableFilterList wrongList = new obTableFilterList(obTableFilterList.operator.OR);
            wrongList.addFilter(filter);
            assertEquals("", wrongList.toString());
            // test null
            try {
                tableQuery.setFilter(null);
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                obTableFilterList filterListNull = new obTableFilterList(null);
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                obTableFilterList filterListNull = new obTableFilterList(null, filter_0, null);
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                obTableFilterList filterListNull = new obTableFilterList(obTableFilterList.operator.AND, filter_0, null);
            } catch (Exception e) {
                assertTrue(true);
            }
            try {
                obTableFilterList filterListNull = new obTableFilterList();
                filterListNull.addFilter(filter_0, null);
            } catch (Exception e) {
                assertTrue(true);
            }
            // test nested
            obTableFilterList filterListNested_0 = new obTableFilterList(obTableFilterList.operator.OR);
            obTableFilterList filterListNested_1 = new obTableFilterList(obTableFilterList.operator.AND);
            filterListNested_0.addFilter(filter_0);
            filterListNested_0.addFilter(filter_1);
            filterListNested_1.addFilter(filterListNested_0);
            filterListNested_1.addFilter(filter_0);
            filterListNested_1.addFilter(filterListNested_0);
            assertEquals("(TableCompareFilter(=, 'c3:value') || TableCompareFilter(<=, 'c2:5')) && TableCompareFilter(=, 'c3:value') && (TableCompareFilter(=, 'c3:value') || TableCompareFilter(<=, 'c2:5'))",
                    filterListNested_1.toString());
            obTableFilterList filterListNested_2 = new obTableFilterList(obTableFilterList.operator.OR, filter_0);
            filterListNested_2.addFilter(filterListNested_1);
            assertEquals("TableCompareFilter(=, 'c3:value') || ((TableCompareFilter(=, 'c3:value') || TableCompareFilter(<=, 'c2:5')) && TableCompareFilter(=, 'c3:value') && (TableCompareFilter(=, 'c3:value') || TableCompareFilter(<=, 'c2:5')))",
                    filterListNested_2.toString());

            // 查询结果集
            QueryResultSet result = tableQuery.select("c2").primaryIndex().setBatchSize(2)
                .addScanRange(new Object[] { 123L }, new Object[] { 777L }).execute();

            for (int i = 0; i < 8; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get("c2"), c2[i]);
                System.out.println("c2:" + value.get("c2"));
            }
            Assert.assertFalse(result.next());

        } catch (Exception e) {
            assertTrue(true);
        } finally {
            for (int i = 0; i < 8; i++) {
                client1.delete("test_batch_query", new Object[]{c1[i]});
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
         *                                          `c3` varchar(20) NOT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client).addRowKeyElement("test_query_filter_mutate", new String[]{"c1"}); //同索引列的值一样

        client.insert("test_query_filter_mutate", new Object[]{0L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row1"});
        client.insert("test_query_filter_mutate", new Object[]{1L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row2"});
        client.insert("test_query_filter_mutate", new Object[]{2L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row3"});

        TableQuery tableQuery = client.query("test_query_filter_mutate");
        tableQuery.addScanRange(new Object[]{0L}, new Object[]{250L});
        tableQuery.select("c1", "c2", "c3");

        obTableValueFilter filter_0 = new obTableValueFilter(obCompareOp.GT, "c1", 0);
        obTableValueFilter filter_1 = new obTableValueFilter(obCompareOp.LE, "c1", 2);
        obTableValueFilter filter_2 = new obTableValueFilter(obCompareOp.GT, "c1", 1);

        try {
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
            client.delete("test_query_filter_mutate", new Object[]{0L});
            client.delete("test_query_filter_mutate", new Object[]{1L});
            client.delete("test_query_filter_mutate", new Object[]{2L});
        }
    }

    @Test
    public void testQueryAndAppend() throws Exception {

        /* 
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) NOT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client).addRowKeyElement("test_query_filter_mutate", new String[]{"c1"}); //同索引列的值一样

        client.insert("test_query_filter_mutate", new Object[]{0L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row1"});
        client.insert("test_query_filter_mutate", new Object[]{1L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row2"});
        client.insert("test_query_filter_mutate", new Object[]{2L}, new String[]{"c2", "c3"},
               new Object[]{new byte[]{1}, "row3"});
        TableQuery tableQuery = client.query("test_query_filter_mutate");
       /* Scan range must in one partition */
        tableQuery.addScanRange(new Object[]{0L}, new Object[]{200L});
        tableQuery.select("c1", "c2", "c3");

        /* Set Filter String */
        obTableValueFilter filter_0 = new obTableValueFilter(obCompareOp.GT, "c1", 0);
        obTableValueFilter filter_1 = new obTableValueFilter(obCompareOp.GE, "c3", "row3_append0");

        try {
            tableQuery.setFilter(filter_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client).obTableQueryAndAppend(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "_append0"}, true);
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(2, res.getAffectedRows());
            /* check value before append */
            Assert.assertEquals("row2", res.getAffectedEntity().getPropertiesRows().get(0).get(2).getValue());
            /* To confirm changing. re-query to get the latest data */;
            obTableValueFilter confirm_0 = new obTableValueFilter(obCompareOp.EQ, "c3", "row2_append0");
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

            tableQuery.setFilter(filter_1);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client).obTableQueryAndAppend(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "_append1"}, true);
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(1, res.getAffectedRows());
            obTableValueFilter confirm_1 = new obTableValueFilter(obCompareOp.EQ, "c3", "row3_append0_append1");
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
        } finally {
            client.delete("test_query_filter_mutate", new Object[]{0L});
            client.delete("test_query_filter_mutate", new Object[]{1L});
            client.delete("test_query_filter_mutate", new Object[]{2L});
        }
    }

    @Test
    public void testQueryAndIncrement() throws Exception {

        /* 
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) NOT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client).addRowKeyElement("test_query_filter_mutate", new String[]{"c1"}); //同索引列的值一样

        client.insert("test_query_filter_mutate", new Object[]{0L}, new String[]{"c2", "c3", "c4"},
                new Object[]{new byte[]{1}, "row1", 0L});
        client.insert("test_query_filter_mutate", new Object[]{1L}, new String[]{"c2", "c3", "c4"},
                new Object[]{new byte[]{1}, "row2", 10L});
        client.insert("test_query_filter_mutate", new Object[]{2L}, new String[]{"c2", "c3", "c4"},
                new Object[]{new byte[]{1}, "row3", 20L});

        TableQuery tableQuery = client.query("test_query_filter_mutate");
        /* Scan range must in one partition */
        tableQuery.addScanRange(new Object[]{0L}, new Object[]{200L});
        tableQuery.select("c1", "c2", "c3","c4");

        /* Set Filter String */
        obTableValueFilter filter_0 = new obTableValueFilter(obCompareOp.GT, "c1", 0);
        obTableValueFilter filter_1 = new obTableValueFilter(obCompareOp.LT, "c3", "row3");

        try {
            try {
                ObTableQueryAndMutateRequest request = ((ObTableClient) client).obTableQueryAndIncrement(tableQuery, null, null, true);
                ObPayload res_exec = ((ObTableClient) client).execute(request);
            } catch (Exception e) {
                assertTrue(true);
            }
            tableQuery.setFilter(filter_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client).obTableQueryAndIncrement(tableQuery, new String[]{"c4"}, new Object[]{
                   5L}, true);
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_0 = new obTableValueFilter(obCompareOp.GE, "c4", 15);
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            tableQuery.setFilter(filter_1);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client).obTableQueryAndIncrement(tableQuery, new String[]{"c4"}, new Object[]{
                    7L}, true);
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_1 = new obTableValueFilter(obCompareOp.EQ, "c4", 22);
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
        } finally {
            client.delete("test_query_filter_mutate", new Object[]{0L});
            client.delete("test_query_filter_mutate", new Object[]{1L});
            client.delete("test_query_filter_mutate", new Object[]{2L});
        }
    }

    @Test
    public void testQueryAndDelete() throws Exception {

        /* 
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) NOT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client).addRowKeyElement("test_query_filter_mutate", new String[]{"c1"}); //同索引列的值一样

        client.insert("test_query_filter_mutate", new Object[]{0L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row1"});
        client.insert("test_query_filter_mutate", new Object[]{1L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row2"});
        client.insert("test_query_filter_mutate", new Object[]{2L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row3"});

        TableQuery tableQuery = client.query("test_query_filter_mutate");
        /* Scan range must in one partition */
        tableQuery.addScanRange(new Object[]{0L}, new Object[]{200L});
        tableQuery.select("c1", "c2", "c3");

        /* Set Filter String */
        obTableValueFilter filter_0 = new obTableValueFilter(obCompareOp.EQ, "c1", 0);
        obTableFilterList filterList = new obTableFilterList(obTableFilterList.operator.AND);
        obTableValueFilter filter_1 = new obTableValueFilter(obCompareOp.GT, "c3", "ro");
        obTableValueFilter filter_2 = new obTableValueFilter(obCompareOp.LT, "c3", "row3");
        filterList.addFilter(filter_1);
        filterList.addFilter(filter_2);
        obTableValueFilter filter_3 = new obTableValueFilter(obCompareOp.LT, null, "row3");

        try {
            tableQuery.setFilter(filter_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client).obTableQueryAndDelete(tableQuery);
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_0 = new obTableValueFilter(obCompareOp.GE, "c1", 0);
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            tableQuery.setFilter(filterList);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client).obTableQueryAndDelete(tableQuery);
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_1 = new obTableValueFilter(obCompareOp.GE, "c1", 0);
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

            tableQuery.setFilter(filter_3);
            ObTableQueryAndMutateRequest request_2 = ((ObTableClient) client).obTableQueryAndDelete(tableQuery);
            ObPayload res_exec_2 = ((ObTableClient) client).execute(request_2);
            res = (ObTableQueryAndMutateResult) res_exec_2;
            Assert.assertEquals(1, res.getAffectedRows());
        } finally {
        }
    }

    @Test
    public void testQueryAndUpdate() throws Exception {

        /* 
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) NOT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client).addRowKeyElement("test_query_filter_mutate", new String[]{"c1"}); //同索引列的值一样

        client.insert("test_query_filter_mutate", new Object[]{0L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row1"});
        client.insert("test_query_filter_mutate", new Object[]{1L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row2"});
        client.insert("test_query_filter_mutate", new Object[]{2L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row3"});

        TableQuery tableQuery = client.query("test_query_filter_mutate");
        /* Scan range must in one partition */
        tableQuery.addScanRange(new Object[]{0L}, new Object[]{200L});
        tableQuery.select("c1", "c2", "c3");

        obTableValueFilter filter_0 = new obTableValueFilter(obCompareOp.GT, "c1", 0);
        obTableValueFilter filter_1 = new obTableValueFilter(obCompareOp.EQ, "c3", "update1");

        try {
            try {
                ObTableQueryAndMutateRequest request = ((ObTableClient) client).obTableQueryAndUpdate(tableQuery, null, null);
                ObPayload res_exec = ((ObTableClient) client).execute(request);
            } catch (Exception e) {
                assertTrue(true);
            }

            tableQuery.setFilter(filter_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client).obTableQueryAndUpdate(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "update1"});
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_0 = new obTableValueFilter(obCompareOp.EQ, "c3", "update1");
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            tableQuery.setFilter(filter_1);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client).obTableQueryAndUpdate(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "update2"});
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_1 = new obTableValueFilter(obCompareOp.EQ, "c3", "update2");
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());
        } finally {
            client.delete("test_query_filter_mutate", new Object[]{0L});
            client.delete("test_query_filter_mutate", new Object[]{1L});
            client.delete("test_query_filter_mutate", new Object[]{2L});
        }
    }

    @Test
    public void testQueryAndMutateComplex() throws Exception {

        /*
         * CREATE TABLE `test_query_filter_mutate` (`c1` bigint NOT NULL, `c2` varbinary(1024) DEFAULT NULL,
         *                                          `c3` varchar(20) NOT NULL,`c4` bigint DEFAULT NULL, PRIMARY KEY(`c1`))
         *                                          partition by range columns (`c1`) ( PARTITION p0 VALUES LESS THAN (300),
         *                                          PARTITION p1 VALUES LESS THAN (1000), PARTITION p2 VALUES LESS THAN MAXVALUE);
         */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        ((ObTableClient) client).addRowKeyElement("test_query_filter_mutate", new String[]{"c1"}); //同索引列的值一样

        client.insert("test_query_filter_mutate", new Object[]{0L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row1"});
        client.insert("test_query_filter_mutate", new Object[]{1L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row2"});
        client.insert("test_query_filter_mutate", new Object[]{2L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row3"});
        client.insert("test_query_filter_mutate", new Object[]{3L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row4"});
        client.insert("test_query_filter_mutate", new Object[]{4L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row5"});
        client.insert("test_query_filter_mutate", new Object[]{5L}, new String[]{"c2", "c3"},
                new Object[]{new byte[]{1}, "row6"});

        TableQuery tableQuery = client.query("test_query_filter_mutate");
        /* Scan range must in one partition */
        tableQuery.addScanRange(new Object[]{0L}, new Object[]{200L});
        tableQuery.select("c1", "c2", "c3");

        obTableValueFilter c1_GT_0 = new obTableValueFilter(obCompareOp.GT, "c1", 0);
        obTableValueFilter c1_EQ_0 = new obTableValueFilter(obCompareOp.EQ, "c1", 0);
        obTableValueFilter c1_LE_0 = new obTableValueFilter(obCompareOp.LE, "c1", 0);
        obTableValueFilter c1_LT_5 = new obTableValueFilter(obCompareOp.LT, "c1", 5);
        obTableValueFilter c1_LE_5 = new obTableValueFilter(obCompareOp.LE, "c1", 5);
        obTableValueFilter c1_GT_3 = new obTableValueFilter(obCompareOp.GT, "c1", 3);
        obTableValueFilter c1_LT_2 = new obTableValueFilter(obCompareOp.LT, "c1", 2);
        obTableValueFilter c1_EQ_5 = new obTableValueFilter(obCompareOp.EQ, "c1", 5);
        obTableValueFilter c3_GE = new obTableValueFilter(obCompareOp.GE, "c3", "update");
        obTableValueFilter c3_LT = new obTableValueFilter(obCompareOp.LT, "c3", "update4");
        obTableFilterList filters_0 = new obTableFilterList();
        obTableFilterList filters_1 = new obTableFilterList();
        obTableFilterList filters_2 = new obTableFilterList(obTableFilterList.operator.OR);


        try {
            // c1 = 0 && c1 = 0
            filters_0.addFilter(c1_EQ_0, c1_EQ_0);
            tableQuery.setFilter(filters_0);
            ObTableQueryAndMutateRequest request_0 = ((ObTableClient) client).obTableQueryAndUpdate(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "update1"});
            ObPayload res_exec_0 = ((ObTableClient) client).execute(request_0);
            ObTableQueryAndMutateResult res = (ObTableQueryAndMutateResult) res_exec_0;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_0 = new obTableValueFilter(obCompareOp.EQ, "c3", "update1");
            tableQuery.setFilter(confirm_0);
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

            // c1 = 0 && (c1 = 0 && c1 = 0)
            filters_1.addFilter(c1_EQ_0, filters_0);
            tableQuery.setFilter(filters_1);
            ObTableQueryAndMutateRequest request_1 = ((ObTableClient) client).obTableQueryAndUpdate(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "update2"});
            ObPayload res_exec_1 = ((ObTableClient) client).execute(request_1);
            res = (ObTableQueryAndMutateResult) res_exec_1;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_1 = new obTableValueFilter(obCompareOp.EQ, "c3", "update2");
            tableQuery.setFilter(confirm_1);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

            // c1 = 5 || (c1 > 3 && c1 <= 5)
            filters_0 = new obTableFilterList(obTableFilterList.operator.AND);
            filters_0.addFilter(c1_GT_3, c1_LE_5);
            filters_1 = new obTableFilterList(obTableFilterList.operator.OR);
            filters_1.addFilter(c1_EQ_5, filters_0);
            tableQuery.setFilter(filters_1);
            ObTableQueryAndMutateRequest request_2 = ((ObTableClient) client).obTableQueryAndUpdate(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "update3"});
            ObPayload res_exec_2 = ((ObTableClient) client).execute(request_2);
            res = (ObTableQueryAndMutateResult) res_exec_2;
            Assert.assertEquals(2, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_2 = new obTableValueFilter(obCompareOp.EQ, "c3", "update3");
            tableQuery.setFilter(confirm_2);
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());

            // (c1 > 0 && c1 < 5) || (c1 <= 0 || c1 < 2)
            filters_0 = new obTableFilterList(obTableFilterList.operator.AND);
            filters_0.addFilter(c1_GT_0);
            filters_0.addFilter(c1_LT_5);
            filters_1 = new obTableFilterList(obTableFilterList.operator.OR);
            filters_1.addFilter(c1_LE_0, c1_LT_2);
            filters_2.addFilter(filters_0, filters_1);
            tableQuery.setFilter(filters_2);
            ObTableQueryAndMutateRequest request_3 = ((ObTableClient) client).obTableQueryAndUpdate(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "update4"});
            ObPayload res_exec_3 = ((ObTableClient) client).execute(request_3);
            res = (ObTableQueryAndMutateResult) res_exec_3;
            Assert.assertEquals(5, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_3 = new obTableValueFilter(obCompareOp.EQ, "c3", "update4");
            tableQuery.setFilter(confirm_3);
            result = tableQuery.execute();
            Assert.assertEquals(5, result.cacheSize());

            // (c3 >= update && c3 < update4 && c1 < 2) || (c3 < update4 && c1 > 3)
            filters_0 = new obTableFilterList(obTableFilterList.operator.AND);
            filters_0.addFilter(c3_GE, c3_LT, c1_LT_2);
            filters_1 = new obTableFilterList(obTableFilterList.operator.AND);
            filters_1.addFilter(c3_LT, c1_GT_3);
            filters_2 = new obTableFilterList(obTableFilterList.operator.OR);
            filters_2.addFilter(filters_0, filters_1);
            tableQuery.setFilter(filters_2);
            ObTableQueryAndMutateRequest request_4 = ((ObTableClient) client).obTableQueryAndUpdate(tableQuery, new String[]{"c2", "c3"}, new Object[]{
                    new byte[]{1}, "update5"});
            ObPayload res_exec_4 = ((ObTableClient) client).execute(request_4);
            res = (ObTableQueryAndMutateResult) res_exec_4;
            Assert.assertEquals(1, res.getAffectedRows());
            /* To confirm changing. re-query to get the latest data */
            obTableValueFilter confirm_4 = new obTableValueFilter(obCompareOp.GE, "c3", "update5");
            tableQuery.setFilter(confirm_4);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());

        } finally {
            client.delete("test_query_filter_mutate", new Object[]{0L});
            client.delete("test_query_filter_mutate", new Object[]{1L});
            client.delete("test_query_filter_mutate", new Object[]{2L});
            client.delete("test_query_filter_mutate", new Object[]{3L});
            client.delete("test_query_filter_mutate", new Object[]{4L});
            client.delete("test_query_filter_mutate", new Object[]{5L});
        }
    }
}

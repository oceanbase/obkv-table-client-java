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

package com.alipay.oceanbase.rpc.util;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class YcsbBenchTest {

    private void testYcsb(DB db) {
        HashMap<String, ByteIterator> insertValues = new HashMap<String, ByteIterator>();
        insertValues.put("c2", new StringByteIterator("value1"));
        Status status = db.insert("test_varchar_table", "bench_foo1", insertValues);
        Assert.assertEquals(Status.OK, status);
        status = db.insert("test_varchar_table", "bench_foo2", insertValues);
        Assert.assertEquals(Status.OK, status);
        status = db.insert("test_varchar_table", "bench_foo3", insertValues);
        Assert.assertEquals(Status.OK, status);

        HashMap<String, ByteIterator> readValues = new HashMap<String, ByteIterator>();
        Set<String> fields = new HashSet<String>();
        fields.add("c2");
        status = db.read("test_varchar_table", "bench_foo1", fields, readValues);
        Assert.assertEquals(Status.OK, status);
        Assert.assertEquals(1, readValues.size());
        Assert.assertEquals("value1", readValues.get("c2").toString());

        HashMap<String, ByteIterator> updateValues = new HashMap<String, ByteIterator>();
        updateValues.put("c2", new StringByteIterator("value21"));
        status = db.update("test_varchar_table", "bench_foo1", updateValues);
        Assert.assertEquals(Status.OK, status);
        status = db.read("test_varchar_table", "bench_foo1", fields, readValues);
        Assert.assertEquals(Status.OK, status);
        Assert.assertEquals(1, readValues.size());
        Assert.assertEquals("value21", readValues.get("c2").toString());

        status = db.delete("test_varchar_table", "bench_foo1");
        Assert.assertEquals(Status.OK, status);
        status = db.delete("test_varchar_table", "bar");
        Assert.assertEquals(Status.ERROR, status);
    }

    @Test
    public void testYscbBenchClient() {
        Properties p = new Properties();
        p.setProperty(YcsbBenchClient.OB_TABLE_CLIENT_PARAM_URL, ObTableClientTestUtil.PARAM_URL);
        p.setProperty(YcsbBenchClient.OB_TABLE_CLIENT_FULL_USER_NAME,
            ObTableClientTestUtil.FULL_USER_NAME);
        p.setProperty(YcsbBenchClient.OB_TABLE_CLIENT_PASSWORD, ObTableClientTestUtil.PASSWORD);
        p.setProperty(YcsbBenchClient.OB_TABLE_CLIENT_SYS_USER_NAME,
            ObTableClientTestUtil.PROXY_SYS_USER_NAME);
        p.setProperty(YcsbBenchClient.OB_TABLE_CLIENT_SYS_ENC_PASSWORD,
            ObTableClientTestUtil.PROXY_SYS_USER_ENC_PASSWORD);
        YcsbBenchClient client = new YcsbBenchClient();
        client.setProperties(p);
        try {
            client.init();
            testYcsb(client);
            Vector<HashMap<String, ByteIterator>> scanValues = new Vector();
            Set<String> fields = new HashSet<String>();
            fields.add("c2");
            Status status = client.scan("test_varchar_table", "bench_foo", 2, fields, scanValues);
            Assert.assertEquals(Status.OK, status);
            Assert.assertEquals(2, scanValues.size());

        } catch (Exception e) {
            Assert.fail("YcsbBenchClient Hit Exception:" + e.getMessage());
        }
    }

    @Test
    public void testYscbBench() throws Exception {
        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        ObTable obTable = obTableClient.getTable("test_varchar_table", new Object[] { "abc" },
            true, true).getRight();

        Properties p = new Properties();
        p.setProperty(YcsbBench.DB_PROPERTY, obTable.getDatabase());
        p.setProperty(YcsbBench.HOST_PROPERTY, obTable.getIp());
        p.setProperty(YcsbBench.PASSWORD_PROPERTY, obTable.getPassword());
        p.setProperty(YcsbBench.PORT_PROPERTY, "" + obTable.getPort());
        p.setProperty(YcsbBench.TENANT_PROPERTY, obTable.getTenantName());
        p.setProperty(YcsbBench.USERNAME_PROPERTY, obTable.getUserName());

        YcsbBench ycsbBench = new YcsbBench();
        ycsbBench.setProperties(p);
        try {
            ycsbBench.init();
            testYcsb(ycsbBench);
        } catch (Exception e) {
            Assert.fail("YcsbBench Hit Exception:" + e.getMessage());
        }
    }
}

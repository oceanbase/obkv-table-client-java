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

import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class ObTableConnectionTest {
    public ObTableClient client;
    private int          connCnt   = 100;
    private boolean      isMannual = false;

    @Before
    public void setup() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(),
            Integer.toString(connCnt));
        obTableClient.init();

        this.client = obTableClient;
    }

    int getGvConnections() throws Exception {
        int resCnt = 0;
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement
            .execute("select count(*) from oceanbase.gv$ob_kv_connections group by svr_ip, svr_port limit 1");
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            resCnt = resultSet.getInt(1);
        }
        assertEquals(false, resultSet.next());
        return resCnt;
    }

    int getVConnections() throws Exception {
        int resCnt = 0;
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("select count(*) from oceanbase.v$ob_kv_connections");
        ResultSet resultSet = statement.getResultSet();
        while (resultSet.next()) {
            resCnt = resultSet.getInt(1);
        }
        assertEquals(false, resultSet.next());
        return resCnt;
    }

    /**
        CREATE TABLE `test_varchar_table` (
        `c1` varchar(20) NOT NULL,
        `c2` varchar(20) DEFAULT NULL,
        PRIMARY KEY (`c1`)
        ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
     **/
    @Test
    public void test_conncetion() throws Exception {
        String tableName = "test_varchar_table";
        long executeTime = 10000; // 10s
        int threadCnt = 10;
        ArrayList<GetWorker> workers = new ArrayList<>();
        if (client.isOdpMode()) {
            Assert.assertTrue(getGvConnections() > 0);
        } else {
            // when we run all tests, connection may exceed the connCnt
            if (isMannual) {
                assertEquals(this.connCnt, getGvConnections());
                assertEquals(this.connCnt, getVConnections());
            } else {
                Assert.assertTrue(getGvConnections() >= this.connCnt);
                Assert.assertTrue(getVConnections() >= this.connCnt);
            }
        }

        for (int i = 0; i < threadCnt; i++) {
            GetWorker worker = new GetWorker(i, tableName, this.client, executeTime);
            workers.add(worker);
        }
        for (int i = 0; i < threadCnt; i++) {
            workers.get(i).start();
        }
        if (client.isOdpMode()) {
            Assert.assertTrue(getGvConnections() > 0);
        } else {
            // when we run all tests, connection may exceed the connCnt
            if (isMannual) {
                assertEquals(this.connCnt, getGvConnections());
                assertEquals(this.connCnt, getVConnections());
            } else {
                Assert.assertTrue(getGvConnections() >= this.connCnt);
                Assert.assertTrue(getVConnections() >= this.connCnt);
            }
        }

        for (int i = 0; i < threadCnt; i++) {
            workers.get(i).join();
        }
        if (client.isOdpMode()) {
            Assert.assertTrue(getGvConnections() > 0);
        } else {
            // when we run all tests, connection may exceed the connCnt
            if (isMannual) {
                assertEquals(this.connCnt, getGvConnections());
                assertEquals(this.connCnt, getVConnections());
            } else {
                Assert.assertTrue(getGvConnections() >= this.connCnt);
                Assert.assertTrue(getVConnections() >= this.connCnt);
            }
        }

        this.client.close();
        // when we run all tests, connection may exceed 0
        if (isMannual) {
            assertEquals(0, getVConnections());
            assertEquals(0, getGvConnections());
        }
    }

    class GetWorker extends Thread {
        private int           id;
        private String        tableName;
        private ObTableClient obTableClient;
        private long          executeTime;  // in millisecond

        public GetWorker(int id, String tableName, ObTableClient obTableClient, long executeTime) {
            this.id = id;
            this.tableName = tableName;
            this.obTableClient = obTableClient;
            this.executeTime = executeTime;
        }

        public void run() {
            long start = System.currentTimeMillis();
            while ((System.currentTimeMillis() - start) < executeTime) {
                try {
                    obTableClient.get(tableName, new String[] { "k1" }, new String[] { "c1" });
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("thread " + id + " get occurs exception !");
                } finally {
                }
            }
        }
    }
}

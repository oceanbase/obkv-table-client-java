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

import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.*;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.*;

/**
 CREATE TABLE IF NOT EXISTS `test_group_commit` (
     `c1` bigint NOT NULL,
     `c2` bigint NOT NULL,
     primary key(`c1`)) PARTITION BY KEY(`c1`) PARTITIONS 32;
 **/
public class ObTableGroupTest {
    ObTableClient        client;
    static OpsController c = new OpsController();
    public static String tableName = "test_group_commit";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.addProperty(Property.RPC_OPERATION_TIMEOUT.getKey(), "10000");
        obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), "10000");
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { "c1" });
    }

    @BeforeClass
    public static void setupBeforeClass() throws Exception {
        System.out.println("setupBeforeClass");
        setMinimalImage();
        enableGroupCommit();
        c.enableGroupCommitWork();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        System.out.println("tearDownAfterClass");
        setFullImage();
        disableGroupCommit();
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

    private static void enableGroupCommit() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter system set enable_kv_group_commit=true;");
    }

    private static void disableGroupCommit() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter system set enable_kv_group_commit=false;");
    }

    class PutTask implements Runnable {
        public PutTask(long index) {
            this.index = index;
        }
        private long index;
        @Override
        public void run() {
            try {
                client.put(tableName).setRowKey(colVal("c1", index))
                        .addMutateColVal(colVal("c2", 1L))
                        .execute();
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(false);
            }
        }
    }

    class PutFailTask implements Runnable {
        public PutFailTask(long index) {
            this.index = index;
        }
        private long index;
        @Override
        public void run() {
            try {
                client.put(tableName).setRowKey(colVal("c1", index))
                        .addMutateColVal(colVal("c2", "c2 is bigint"))
                        .execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class GetTask implements Runnable {
        public GetTask(long index) {
            this.index = index;
        }
        private long index;
        @Override
        public void run() {
            try {
                client.get(tableName, new Object[]{index}, new String[]{"c1"});
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(false);
            }
        }
    }

    class GetFailTask implements Runnable {
        public GetFailTask(long index) {
            this.index = index;
        }
        private long index;
        @Override
        public void run() {
            try {
                client.get(tableName, new Object[]{index}, new String[]{"not exist column"});
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    class InsertOrUpdateTask implements Runnable {
        public InsertOrUpdateTask(long index) {
            this.index = index;
        }
        private long index;
        @Override
        public void run() {
            try {
                client.insertOrUpdate(tableName).setRowKey(colVal("c1", index))
                        .addMutateColVal(colVal("c2", 1L))
                        .execute();
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(false);
            }
        }
    }

    @Test
    public void testPut1() throws Exception {
        try {
            client.put(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            Map<String, Object> res = client.get(tableName, new Object[]{1L}, new String[]{"c1"});
            assertEquals(1L, res.get("c1"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
        }
    }

    @Test
    public void testPut2() throws Exception {
        final int THREAD_CNT = 200;
        final int LOOP = 200;
        try {
            for (int i = 0; i < LOOP; i++) {
                Thread ts[] = new Thread[THREAD_CNT];
                for (int j = 0; j < THREAD_CNT; j++) {
                    ts[j] = new Thread(new PutTask(j));
                    ts[j].start();
                }
                for (int k = 0; k < THREAD_CNT; k++) {
                    ts[k].join();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 0; i < LOOP; i++) {
                client.delete(tableName).setRowKey(colVal("c1", i)).execute();
            }
        }
    }

    @Test
    public void testPut3() throws Exception {
        final int THREAD_CNT = 200;
        final int LOOP = 200;
        try {
            for (int i = 0; i < LOOP; i++) {
                Thread ts[] = new Thread[THREAD_CNT];
                for (int j = 0; j < THREAD_CNT; j++) {
                    if (j % 2 == 0) {
                        ts[j] = new Thread(new PutTask(j));
                    } else {
                        ts[j] = new Thread(new InsertOrUpdateTask(j));
                    }
                    ts[j].start();
                }
                for (int k = 0; k < THREAD_CNT; k++) {
                    ts[k].join();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 0; i < LOOP; i++) {
                client.delete(tableName).setRowKey(colVal("c1", i)).execute();
            }
        }
    }

    @Test
    public void testPut4() throws Exception {
        final int THREAD_CNT = 200;
        final int LOOP = 200;
        try {
            for (int i = 0; i < LOOP; i++) {
                Thread ts[] = new Thread[THREAD_CNT];
                for (int j = 0; j < THREAD_CNT; j++) {
                    if (j % 2 == 0) {
                        ts[j] = new Thread(new PutTask(j));
                    } else if (j % 3 == 0) {
                        ts[j] = new Thread(new PutFailTask(j));
                    } else {
                        ts[j] = new Thread(new InsertOrUpdateTask(j));
                    }
                    ts[j].start();
                }
                for (int k = 0; k < THREAD_CNT; k++) {
                    ts[k].join();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 0; i < LOOP; i++) {
                client.delete(tableName).setRowKey(colVal("c1", i)).execute();
            }
        }
    }

    @Test
    public void testGet1() throws Exception {
        try {
            client.put(tableName).setRowKey(colVal("c1", 1L))
                    .addMutateColVal(colVal("c2", 1L))
                    .execute();
            Map<String, Object> res = client.get(tableName, new Object[]{1L}, new String[]{"c1"});
            assertEquals(1L, res.get("c1"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", 1L)).execute();
        }
    }

    @Test
    public void testGet2() throws Exception {
        final int THREAD_CNT = 200;
        final int LOOP = 200;
        try {
            for (int i = 0; i < LOOP; i++) {
                Thread ts[] = new Thread[THREAD_CNT];
                for (int j = 0; j < THREAD_CNT; j++) {
                    ts[j] = new Thread(new GetTask(j));
                    ts[j].start();
                }
                for (int k = 0; k < THREAD_CNT; k++) {
                    ts[k].join();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 0; i < LOOP; i++) {
                client.delete(tableName).setRowKey(colVal("c1", i)).execute();
            }
        }
    }

    @Test
    public void testGet3() throws Exception {
        final int THREAD_CNT = 200;
        final int LOOP = 200;
        try {
            for (int i = 0; i < LOOP; i++) {
                Thread ts[] = new Thread[THREAD_CNT];
                for (int j = 0; j < THREAD_CNT; j++) {
                    if (j % 2 == 0) {
                        ts[j] = new Thread(new GetTask(j));
                    } else {
                        ts[j] = new Thread(new InsertOrUpdateTask(j));
                    }
                    ts[j].start();
                }
                for (int k = 0; k < THREAD_CNT; k++) {
                    ts[k].join();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 0; i < LOOP; i++) {
                client.delete(tableName).setRowKey(colVal("c1", i)).execute();
            }
        }
    }

    @Test
    public void testGet4() throws Exception {
        final int THREAD_CNT = 200;
        final int LOOP = 200;
        try {
            for (int i = 0; i < LOOP; i++) {
                Thread ts[] = new Thread[THREAD_CNT];
                for (int j = 0; j < THREAD_CNT; j++) {
                    if (j % 2 == 0) {
                        ts[j] = new Thread(new GetTask(j));
                    } else if (j % 3 == 0) {
                        ts[j] = new Thread(new GetFailTask(j));
                    } else {
                        ts[j] = new Thread(new InsertOrUpdateTask(j));
                    }
                    ts[j].start();
                }
                for (int k = 0; k < THREAD_CNT; k++) {
                    ts[k].join();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 0; i < LOOP; i++) {
                client.delete(tableName).setRowKey(colVal("c1", i)).execute();
            }
        }
    }
}

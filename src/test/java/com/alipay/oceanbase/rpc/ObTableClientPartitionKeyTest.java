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

import com.alipay.oceanbase.rpc.exception.ObTablePartitionConsistentException;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

public class ObTableClientPartitionKeyTest {

    private ObTableClient obTableClient;

    @Before
    public void setUp() throws Exception {
        /*
        *
        * create table testPartition (
            K varbinary(1024),
            Q varbinary(256),
            T bigint,
            V varbinary(1024),
            K_PREFIX varbinary(1024) generated always as (substring(K, 1, 4)),
            primary key(K, Q, T)
            ) partition by key(K_PREFIX) partitions 15;
        *
        * */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty("connectTimeout", "100000");
        obTableClient.addProperty("socketTimeout", "100000");
        obTableClient.setTableEntryAcquireSocketTimeout(10000);
        obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
        obTableClient.init();

        this.obTableClient = obTableClient;
    }

    @After
    public void close() throws Exception {
        if (null != this.obTableClient) {
            ((ObTableClient) this.obTableClient).close();
        }
    }

    @Test
    public void testInsert() throws Exception {
        long timestamp = System.currentTimeMillis();
        long affectRow = obTableClient.insert("testPartition", new Object[] { "partitionKey",
                "partition".getBytes(), timestamp }, new String[] { "V" },
            new Object[] { "aa".getBytes() });
        Assert.assertEquals(1, affectRow);

        affectRow = obTableClient.insertOrUpdate("testPartition",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timestamp },
            new String[] { "V" }, new Object[] { "bb".getBytes() });
        Assert.assertEquals(1, affectRow);

        Map<String, Object> result = obTableClient.get("testPartition", new Object[] {
                "partitionKey".getBytes(), "partition".getBytes(), timestamp }, new String[] { "K",
                "Q", "T", "V", "K_PREFIX" });
        Assert.assertEquals("partitionKey", new String((byte[]) result.get("K"), "UTF-8"));
        Assert.assertEquals("partition", new String((byte[]) result.get("Q"), "UTF-8"));
        Assert.assertEquals(timestamp, result.get("T"));
        Assert.assertEquals("bb", new String((byte[]) result.get("V"), "UTF-8"));

    }

    @Test
    public void testGet() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testPartition",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        Map<String, Object> result = obTableClient.get("testPartition", new Object[] {
                "partitionKey".getBytes(), "partition".getBytes(), timeStamp }, new String[] { "K",
                "Q", "T", "V", "K_PREFIX" });
        Assert.assertEquals("partitionKey", new String((byte[]) result.get("K"), "UTF-8"));
        Assert.assertEquals("partition", new String((byte[]) result.get("Q"), "UTF-8"));
        Assert.assertEquals(timeStamp, result.get("T"));
        Assert.assertEquals("value", new String((byte[]) result.get("V"), "UTF-8"));
    }

    @Test
    public void testUpdate() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testPartition",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        long affectedRow = obTableClient.update("testPartition",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value1".getBytes() });
        Assert.assertEquals(1, affectedRow);
        Map<String, Object> result = obTableClient.get("testPartition", new Object[] {
                "partitionKey".getBytes(), "partition".getBytes(), timeStamp }, new String[] { "K",
                "Q", "T", "V", "K_PREFIX" });
        Assert.assertEquals(timeStamp, result.get("T"));
        Assert.assertEquals("value1", new String((byte[]) result.get("V"), "UTF-8"));
    }

    @Test
    public void testReplace() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testPartition",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        long affectedRow = obTableClient.replace("testPartition",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value1".getBytes() });
        Assert.assertEquals(2, affectedRow);
        Map<String, Object> result = obTableClient.get("testPartition", new Object[] {
                "partitionKey".getBytes(), "partition".getBytes(), timeStamp }, new String[] { "K",
                "Q", "T", "V", "K_PREFIX" });
        Assert.assertEquals(timeStamp, result.get("T"));
        Assert.assertEquals("value1", new String((byte[]) result.get("V"), "UTF-8"));
    }

    @Test
    public void testDelete() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testPartition",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        long affectedRow = obTableClient.delete("testPartition",
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp });
        Assert.assertEquals(1, affectedRow);
        Map<String, Object> result = obTableClient.get("testPartition", new Object[] {
                "partitionKey".getBytes(), "partition".getBytes(), timeStamp }, new String[] { "K",
                "Q", "T", "V", "K_PREFIX" });
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testQuery() throws Exception {
        long timeStamp = System.currentTimeMillis();
        String tableName = null;
        if (!obTableClient.isOdpMode()) {
            tableName = "testPartition";
            obTableClient.insert(tableName,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(tableName,
                    new Object[] { "key1_5".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });

            TableQuery tableQuery = obTableClient.query(tableName);
            tableQuery.addScanRange(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                            timeStamp },
                    new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp });
            tableQuery.select("K", "Q", "T", "V", "K_PREFIX");
            try {
                tableQuery.execute();
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTablePartitionConsistentException);
            }
            tableQuery = obTableClient.query(tableName);
            tableQuery.addScanRange(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                            timeStamp },
                    new Object[] { "key1_3".getBytes(), "partition".getBytes(), timeStamp });
            tableQuery.select("K", "Q", "T", "V");
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
            result.next();
            Map<String, Object> row = result.getRow();
            Assert.assertEquals("key1_1", new String((byte[]) row.get("K")));
            Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
            Assert.assertEquals(timeStamp, row.get("T"));
            Assert.assertEquals("value1", new String((byte[]) row.get("V")));

            tableQuery = obTableClient.query(tableName);
            tableQuery.addScanRange(
                    new Object[] { "key1_1".getBytes(), ObObj.getMin(), ObObj.getMin() }, new Object[] {
                            "key1_8".getBytes(), ObObj.getMax(), ObObj.getMax() });
            tableQuery.select("K", "Q", "T", "V");
            result = tableQuery.execute();
            Assert.assertTrue(result.cacheSize() >= 2);
        } else {
            // TODO: generated column is not supported in ODP mode
            timeStamp = System.currentTimeMillis();
            tableName = "testKey";
            try {
                obTableClient.insert(tableName,
                        new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                        new String[] { "V" }, new Object[] { "value1".getBytes() });
                obTableClient.insert(tableName,
                        new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp + 1 },
                        new String[] { "V" }, new Object[] { "value2".getBytes() });
                obTableClient.insert(tableName,
                        new Object[] { "key1_2".getBytes(), "partition".getBytes(), timeStamp },
                        new String[] { "V" }, new Object[] { "value3".getBytes() });

                TableQuery tableQuery = obTableClient.query(tableName);
                tableQuery.addScanRange(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                                timeStamp },
                        new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp });
                tableQuery.select("K", "Q", "T", "V", "K_PREFIX");
                try {
                    tableQuery.execute();
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(e instanceof ObTableUnexpectedException);
                    Assert.assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode,
                            ((ObTableUnexpectedException) e).getErrorCode());
                }
                tableQuery = obTableClient.query(tableName);
                tableQuery.addScanRange(new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                        new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp + 1 });
                tableQuery.select("K", "Q", "T", "V");
                QueryResultSet result = tableQuery.execute();
                Assert.assertEquals(2, result.cacheSize());

                Assert.assertEquals(true, result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals("key1_1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp, row.get("T"));
                Assert.assertEquals("value1", new String((byte[]) row.get("V")));

                Assert.assertEquals(true, result.next());
                row = result.getRow();
                Assert.assertEquals("key1_1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + 1, row.get("T"));
                Assert.assertEquals("value2", new String((byte[]) row.get("V")));
            } catch (Exception e){
                e.printStackTrace();
                Assert.assertTrue(false);
            } finally {
                obTableClient.delete(tableName,
                        new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
                obTableClient.delete(tableName,
                        new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp + 1 });
                obTableClient.delete(tableName,
                        new Object[] { "key1_2".getBytes(), "partition".getBytes(), timeStamp });
            }
        }
    }

    @Test
    public void testQueryLocalIndex() throws Exception {
        // TODO: client route is wrong when execute query on key partitioned table using index
        if (!obTableClient.isOdpMode()) {
            return;
        }
        long timeStamp = System.currentTimeMillis();
        String tableName = obTableClient.isOdpMode() ? "testKey" : "testPartition";
        try {
            obTableClient.insert(tableName,
                    new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp + 1},
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(tableName,
                    new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp + 2},
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            obTableClient.insert(tableName,
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp + 1},
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            obTableClient.insert(tableName,
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp + 2},
                    new String[] { "V" }, new Object[] { "value1".getBytes() });

            // key partitioned table do not support range query

            // query key2_1
            TableQuery tableQuery = obTableClient.query(tableName);
            tableQuery.addScanRange(new Object[] { "key2_1".getBytes(), "value0".getBytes() },
                                    new Object[] { "key2_1".getBytes(), "value9".getBytes() });
            // TODO: do param check, must specify select columns
            tableQuery.select("K", "Q", "T", "V");
            tableQuery.setScanRangeColumns("K", "V");
            tableQuery.indexName("i1");
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());
            for (int i = 1; i <= 2; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals("key2_1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + i, row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());

            tableQuery.scanOrder(false);
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());
            for (int i = 2; i >= 1; i--) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals("key2_1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + i, row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());

            // query key3_1
            tableQuery = obTableClient.query(tableName);
            tableQuery.addScanRange(new Object[] { "key3_1".getBytes(), "value0".getBytes() },
                                    new Object[] { "key3_1".getBytes(), "value9".getBytes() });
            tableQuery.select("K", "Q", "T", "V");
            tableQuery.setScanRangeColumns("K", "V");
            tableQuery.indexName("i1");
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());
            for (int i = 1; i <= 2; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals("key3_1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + 3 - i, row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());

            tableQuery.scanOrder(false);
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());
            for (int i = 2; i >= 1; i--) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals("key3_1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + 3 - i, row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());
        } catch (Exception e){
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete(tableName,
                    new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp + 1 });
            obTableClient.delete(tableName,
                    new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp + 2 });
            obTableClient.delete(tableName,
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp + 1});
            obTableClient.delete(tableName,
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp + 2});
        }
    }

    @Test
    public void testBatch() throws Exception {
        long timeStamp = System.currentTimeMillis();
        String tableName = obTableClient.isOdpMode() ? "testKey" : "testPartition";
        try {
            obTableClient.insert(tableName,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(tableName,
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch(tableName);
            tableBatchOps
                    .delete(new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            tableBatchOps.insert(
                    new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get(tableName,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp }, new String[] {
                            "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get(tableName, new Object[] { "key2_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key2_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get(tableName, new Object[] { "key3_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key3_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete(tableName, new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(tableName, new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(tableName, new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testBatchConcurrent() throws Exception {
        long timeStamp = System.currentTimeMillis();
        String tableName = obTableClient.isOdpMode() ? "testKey" : "testPartition";
        // This opetion is useless in ODP mode
        obTableClient.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
        try {
            obTableClient.insert(tableName,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(tableName,
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch(tableName);
            tableBatchOps
                    .delete(new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            tableBatchOps.insert(
                    new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get(tableName,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp }, new String[] {
                            "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get(tableName, new Object[] { "key2_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key2_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get(tableName, new Object[] { "key3_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key3_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

        } catch (Exception e) {
           e.printStackTrace();
           Assert.assertTrue(false);
        } finally {
            obTableClient.delete(tableName, new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(tableName, new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(tableName, new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testBatchConcurrentWithPriority() throws Exception {
        long timeStamp = System.currentTimeMillis();
        String tableName = obTableClient.isOdpMode() ? "testKey" : "testPartition";
        // This following two option in ODP mode is useless.
        ThreadLocalMap.setProcessHighPriority();
        obTableClient.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
        try {
            obTableClient.insert(tableName,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(tableName,
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch(tableName);
            tableBatchOps
                    .delete(new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            tableBatchOps.insert(
                    new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(
                    new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get(tableName,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp }, new String[] {
                            "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get(tableName, new Object[] { "key2_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key2_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get(tableName, new Object[] { "key3_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key3_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete(tableName, new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(tableName, new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(tableName, new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp });
        }
    }

}

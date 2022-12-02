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

import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Executors;

public class ObTableClientPartitionHashTest {

    private ObTableClient obTableClient;

    @Before
    public void setup() throws Exception {
        /*
         *
         * drop table if exists testHash;
         * create table testHash(K bigint, Q varbinary(256) ,T bigint , V varbinary(1024),primary key(K, Q, T)) partition by hash(K) partitions 16;
         *
         * */
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.setTableEntryAcquireSocketTimeout(10000);
        obTableClient.addProperty("connectTimeout", "1000000");
        obTableClient.addProperty("socketTimeout", "1000000");
        obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);
        obTableClient.init();

        this.obTableClient = obTableClient;
    }

    @Test
    public void testInsert() throws Exception {
        long timestamp = System.currentTimeMillis();
        long affectRow = obTableClient.insert("testHash", new Object[] { 1L,
                "partition".getBytes(), timestamp }, new String[] { "V" },
            new Object[] { "aa".getBytes() });
        Assert.assertEquals(1L, affectRow);

        affectRow = obTableClient.insertOrUpdate("testHash",
            new Object[] { 1L, "partition".getBytes(), timestamp }, new String[] { "V" },
            new Object[] { "bb".getBytes() });
        Assert.assertEquals(1L, affectRow);

        Map<String, Object> result = obTableClient.get("testHash",
            new Object[] { 1L, "partition".getBytes(), timestamp }, new String[] { "K", "Q", "T",
                    "V" });
        Assert.assertEquals(1L, result.get("K"));
        Assert.assertEquals("partition", new String((byte[]) result.get("Q"), "UTF-8"));
        Assert.assertEquals(timestamp, result.get("T"));
        Assert.assertEquals("bb", new String((byte[]) result.get("V"), "UTF-8"));

    }

    @Test
    public void testGet() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testHash", new Object[] { 1L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        Map<String, Object> result = obTableClient.get("testHash",
            new Object[] { 1L, "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T",
                    "V" });
        Assert.assertEquals(1L, result.get("K"));
        Assert.assertEquals("partition", new String((byte[]) result.get("Q"), "UTF-8"));
        Assert.assertEquals(timeStamp, result.get("T"));
        Assert.assertEquals("value", new String((byte[]) result.get("V"), "UTF-8"));
    }

    @Test
    public void testUpdate() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testHash", new Object[] { 1L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        long affectedRow = obTableClient.update("testHash",
            new Object[] { 1L, "partition".getBytes(), timeStamp }, new String[] { "V" },
            new Object[] { "value1L".getBytes() });
        Assert.assertEquals(1L, affectedRow);
        Map<String, Object> result = obTableClient.get("testHash",
            new Object[] { 1L, "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T",
                    "V" });
        Assert.assertEquals(timeStamp, result.get("T"));
        Assert.assertEquals("value1L", new String((byte[]) result.get("V"), "UTF-8"));
    }

    @Test
    public void testReplace() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testHash", new Object[] { 1L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        long affectedRow = obTableClient.replace("testHash",
            new Object[] { 1L, "partition".getBytes(), timeStamp }, new String[] { "V" },
            new Object[] { "value1L".getBytes() });
        Assert.assertEquals(2, affectedRow);
        Map<String, Object> result = obTableClient.get("testHash",
            new Object[] { 1L, "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T",
                    "V" });
        Assert.assertEquals(timeStamp, result.get("T"));
        Assert.assertEquals("value1L", new String((byte[]) result.get("V"), "UTF-8"));
    }

    @Test
    public void testDelete() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testHash", new Object[] { 1L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        long affectedRow = obTableClient.delete("testHash",
            new Object[] { 1L, "partition".getBytes(), timeStamp });
        Assert.assertEquals(1L, affectedRow);
        Map<String, Object> result = obTableClient.get("testHash",
            new Object[] { 1L, "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T",
                    "V" });
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testQuery() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testHash",
                new Object[] { timeStamp, "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value0".getBytes() });
        obTableClient.insert("testHash",
                new Object[] { timeStamp + 1, "partition".getBytes(), timeStamp + 1 },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
        obTableClient.insert("testHash",
                new Object[] { timeStamp + 2, "partition".getBytes(), timeStamp + 2 },
                new String[] { "V" }, new Object[] { "value2".getBytes() });
        obTableClient.insert("testHash",
                new Object[] { timeStamp + 3, "partition".getBytes(), timeStamp + 3 },
                new String[] { "V" }, new Object[] { "value3".getBytes() });
        obTableClient.insert("testHash",
                new Object[] { timeStamp + 4, "partition".getBytes(), timeStamp + 4 },
                new String[] { "V" }, new Object[] { "value4".getBytes() });

        // query with one partition
        TableQuery tableQuery = obTableClient.query("testHash");
        tableQuery.addScanRange(new Object[] { timeStamp, "partition".getBytes(), timeStamp},
                                new Object[] { timeStamp, "partition".getBytes(), timeStamp});
        tableQuery.select("K", "Q", "T", "V");
        QueryResultSet result = tableQuery.execute();
        Assert.assertEquals(1L, result.cacheSize());

        Assert.assertTrue(result.next());
        Map<String, Object> row = result.getRow();
        Assert.assertEquals(timeStamp, row.get("K"));
        Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
        Assert.assertEquals(timeStamp, row.get("T"));
        Assert.assertEquals("value0", new String((byte[]) row.get("V")));
        Assert.assertFalse(result.next());

        // query with multiply partitions
        tableQuery = obTableClient.query("testHash");
        tableQuery.addScanRange(new Object[] { timeStamp, "partition".getBytes(), timeStamp },
                                new Object[] { timeStamp + 10, "partition".getBytes(), timeStamp });
        tableQuery.select("K", "Q", "T", "V");
        result = tableQuery.execute();
        Assert.assertEquals(5, result.cacheSize());
    }

    @Test
    public void testQueryLocalIndex() throws Exception {
        // TODO: client route is wrong when execute query on hash partitioned table using index
        if (!obTableClient.isOdpMode()) {
            return;
        }
        long timeStamp = System.currentTimeMillis();
        String tableName = "testHash";
        try {
            obTableClient.insert("testHash",
                    new Object[] { timeStamp + 1, "partition".getBytes(), timeStamp + 1 },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
            obTableClient.insert("testHash",
                    new Object[] { timeStamp + 1, "partition".getBytes(), timeStamp + 2 },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert("testHash",
                    new Object[] { timeStamp + 2, "partition".getBytes(), timeStamp + 1 },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert("testHash",
                    new Object[] { timeStamp + 2, "partition".getBytes(), timeStamp + 2 },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });

            // query timestamp + 1
            TableQuery tableQuery = obTableClient.query(tableName);
            tableQuery.addScanRange(new Object[] { timeStamp + 1, "value0".getBytes() },
                                    new Object[] { timeStamp + 1, "value9".getBytes() });
            tableQuery.select("K", "Q", "T", "V");
            tableQuery.scanOrder(false);
            tableQuery.useIndex("i1", new String[] {"K", "V"});
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());
            for (int i = 1; i <= 2; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals(timeStamp + 1, row.get("K"));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + i, row.get("T"));
                Assert.assertEquals("value" + ( 3 - i), new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());

            // query with mutliply partition
            tableQuery = obTableClient.query(tableName);
            tableQuery.addScanRange(new Object[] { timeStamp + 1, "value0".getBytes() },
                                    new Object[] { timeStamp + 3, "value9".getBytes() });
            tableQuery.select("K", "Q", "T", "V");
            tableQuery.useIndex("i1", new String[] {"K", "V"});
            result = tableQuery.execute();
            Assert.assertEquals(4, result.cacheSize());

            // sort result by K, T
            List<Map<String, Object>> resultList = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue(result.next());
                resultList.add(result.getRow());
            }
            Assert.assertFalse(result.next());
            Collections.sort(resultList, new Comparator<Map<String, Object>>() {
                @Override
                public int compare(Map<String, Object> o1, Map<String, Object> o2) {
                    int firstCmp = (int)(((long)o1.get("K") - (long)o2.get("K")));
                    if (firstCmp == 0) {
                        return (int)(((long)o1.get("T")) - ((long)o2.get("T")));
                    }
                    return firstCmp;
                }
            });
            int[] orderedDeltaKeys = {1, 1, 2, 2};
            int[] orderedDeltaTs = {1, 2, 1, 2};
            String[] orderedValues = {"value2", "value1", "value1", "value2"};
            for (int i = 0; i < 4; i++) {
                Assert.assertEquals(timeStamp + orderedDeltaKeys[i], resultList.get(i).get("K"));
                Assert.assertEquals("partition", new String((byte[]) resultList.get(i).get("Q")));
                Assert.assertEquals(timeStamp + orderedDeltaTs[i], resultList.get(i).get("T"));
                Assert.assertEquals(orderedValues[i], new String((byte[]) resultList.get(i).get("V")));
            }
            Assert.assertFalse(result.next());

        } catch (Exception e){
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete("testHash",
                    new Object[] { timeStamp + 1, "partition".getBytes(), timeStamp + 1 });
            obTableClient.delete("testHash",
                    new Object[] { timeStamp + 1, "partition".getBytes(), timeStamp + 2 });
            obTableClient.delete("testHash",
                    new Object[] { timeStamp + 2, "partition".getBytes(), timeStamp + 1 });
            obTableClient.delete("testHash",
                    new Object[] { timeStamp + 2, "partition".getBytes(), timeStamp + 2 });
        }
    }

    @Test
    public void testBatch() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert("testHash", new Object[] { timeStamp + 1L, "partition".getBytes(),
                timeStamp }, new String[] { "V" }, new Object[] { "value1L".getBytes() });
        obTableClient.insert("testHash", new Object[] { timeStamp + 5L, "partition".getBytes(),
                timeStamp }, new String[] { "V" }, new Object[] { "value1L".getBytes() });
        TableBatchOps tableBatchOps = obTableClient.batch("testHash");
        tableBatchOps.delete(new Object[] { timeStamp + 1L, "partition".getBytes(), timeStamp });
        tableBatchOps.insert(new Object[] { timeStamp + 3L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value2".getBytes() });
        tableBatchOps.replace(new Object[] { timeStamp + 5L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value2".getBytes() });
        List<Object> batchResult = tableBatchOps.execute();
        Assert.assertEquals(3, batchResult.size());
        Assert.assertEquals(1L, batchResult.get(0));
        Assert.assertEquals(1L, batchResult.get(1));
        Assert.assertEquals(2L, batchResult.get(2));

        Map<String, Object> getResult = obTableClient.get("testHash", new Object[] {
                timeStamp + 1L, "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T",
                "V" });

        Assert.assertEquals(0, getResult.size());

        getResult = obTableClient.get("testHash",
            new Object[] { timeStamp + 3L, "partition".getBytes(), timeStamp }, new String[] { "K",
                    "Q", "T", "V" });

        Assert.assertEquals(4, getResult.size());

        Assert.assertEquals(timeStamp + 3L, getResult.get("K"));
        Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
        Assert.assertEquals(timeStamp, getResult.get("T"));
        Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

        getResult = obTableClient.get("testHash",
            new Object[] { timeStamp + 5L, "partition".getBytes(), timeStamp }, new String[] { "K",
                    "Q", "T", "V" });

        Assert.assertEquals(4, getResult.size());

        Assert.assertEquals(timeStamp + 5L, getResult.get("K"));
        Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
        Assert.assertEquals(timeStamp, getResult.get("T"));
        Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));
    }

    @Test
    public void testBatchConcurrent() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
        obTableClient.insert("testHash", new Object[] { timeStamp + 1L, "partition".getBytes(),
                timeStamp }, new String[] { "V" }, new Object[] { "value1L".getBytes() });
        obTableClient.insert("testHash", new Object[] { timeStamp + 5L, "partition".getBytes(),
                timeStamp }, new String[] { "V" }, new Object[] { "value1L".getBytes() });
        TableBatchOps tableBatchOps = obTableClient.batch("testHash");
        tableBatchOps.delete(new Object[] { timeStamp + 1L, "partition".getBytes(), timeStamp });
        tableBatchOps.insert(new Object[] { timeStamp + 3L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value2".getBytes() });
        tableBatchOps.replace(new Object[] { timeStamp + 5L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value2".getBytes() });
        List<Object> batchResult = tableBatchOps.execute();
        Assert.assertEquals(3, batchResult.size());
        Assert.assertEquals(1L, batchResult.get(0));
        Assert.assertEquals(1L, batchResult.get(1));
        Assert.assertEquals(2L, batchResult.get(2));

        Map<String, Object> getResult = obTableClient.get("testHash", new Object[] {
                timeStamp + 1L, "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T",
                "V" });

        Assert.assertEquals(0, getResult.size());

        getResult = obTableClient.get("testHash",
            new Object[] { timeStamp + 3L, "partition".getBytes(), timeStamp }, new String[] { "K",
                    "Q", "T", "V" });

        Assert.assertEquals(4, getResult.size());

        Assert.assertEquals(timeStamp + 3L, getResult.get("K"));
        Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
        Assert.assertEquals(timeStamp, getResult.get("T"));
        Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

        getResult = obTableClient.get("testHash",
            new Object[] { timeStamp + 5L, "partition".getBytes(), timeStamp }, new String[] { "K",
                    "Q", "T", "V" });

        Assert.assertEquals(4, getResult.size());

        Assert.assertEquals(timeStamp + 5L, getResult.get("K"));
        Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
        Assert.assertEquals(timeStamp, getResult.get("T"));
        Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));
    }

    @Test
    public void testBatchConcurrentWithPriority() throws Exception {
        long timeStamp = System.currentTimeMillis();
        ThreadLocalMap.setProcessHighPriority();
        obTableClient.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
        obTableClient.insert("testHash", new Object[] { timeStamp + 1L, "partition".getBytes(),
                timeStamp }, new String[] { "V" }, new Object[] { "value1L".getBytes() });
        obTableClient.insert("testHash", new Object[] { timeStamp + 5L, "partition".getBytes(),
                timeStamp }, new String[] { "V" }, new Object[] { "value1L".getBytes() });
        TableBatchOps tableBatchOps = obTableClient.batch("testHash");
        tableBatchOps.delete(new Object[] { timeStamp + 1L, "partition".getBytes(), timeStamp });
        tableBatchOps.insert(new Object[] { timeStamp + 3L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value2".getBytes() });
        tableBatchOps.replace(new Object[] { timeStamp + 5L, "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value2".getBytes() });
        List<Object> batchResult = tableBatchOps.execute();
        Assert.assertEquals(3, batchResult.size());
        Assert.assertEquals(1L, batchResult.get(0));
        Assert.assertEquals(1L, batchResult.get(1));
        Assert.assertEquals(2L, batchResult.get(2));

        Map<String, Object> getResult = obTableClient.get("testHash", new Object[] {
                timeStamp + 1L, "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T",
                "V" });

        Assert.assertEquals(0, getResult.size());

        getResult = obTableClient.get("testHash",
            new Object[] { timeStamp + 3L, "partition".getBytes(), timeStamp }, new String[] { "K",
                    "Q", "T", "V" });

        Assert.assertEquals(4, getResult.size());

        Assert.assertEquals(timeStamp + 3L, getResult.get("K"));
        Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
        Assert.assertEquals(timeStamp, getResult.get("T"));
        Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

        getResult = obTableClient.get("testHash",
            new Object[] { timeStamp + 5L, "partition".getBytes(), timeStamp }, new String[] { "K",
                    "Q", "T", "V" });

        Assert.assertEquals(4, getResult.size());

        Assert.assertEquals(timeStamp + 5L, getResult.get("K"));
        Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
        Assert.assertEquals(timeStamp, getResult.get("T"));
        Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));
    }

}

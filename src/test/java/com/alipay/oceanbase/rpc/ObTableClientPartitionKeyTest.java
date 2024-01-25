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
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.andList;
import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.compareVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.cleanTable;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.generateRandomStringByUUID;

public class ObTableClientPartitionKeyTest {

    private ObTableClient obTableClient;
    private ObTableClient normalClient;
    private String        TEST_TABLE = null;

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

        final ObTableClient normalClient = ObTableClientTestUtil.newTestClient();
        normalClient.setMetadataRefreshInterval(100);
        normalClient.addProperty("connectTimeout", "100000");
        normalClient.addProperty("socketTimeout", "100000");
        normalClient.setTableEntryAcquireSocketTimeout(10000);
        normalClient.setRunningMode(ObTableClient.RunningMode.NORMAL);
        normalClient.init();

        if (obTableClient.isOdpMode()) {
            TEST_TABLE = "testKey";
        } else {
            TEST_TABLE = "testPartition";
            obTableClient.addRowKeyElement("testPartition", new String[] { "K", "Q", "T" });
        }

        this.obTableClient = obTableClient;
        this.normalClient = normalClient;
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
        try {
            long affectRow = obTableClient.insert(TEST_TABLE, new Object[] { "partitionKey",
                    "partition".getBytes(), timestamp }, new String[] { "V" },
                new Object[] { "aa".getBytes() });
            Assert.assertEquals(1, affectRow);

            affectRow = obTableClient.insertOrUpdate(TEST_TABLE,
                new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timestamp },
                new String[] { "V" }, new Object[] { "bb".getBytes() });
            Assert.assertEquals(1, affectRow);

            Map<String, Object> result = obTableClient.get(TEST_TABLE, new Object[] {
                    "partitionKey".getBytes(), "partition".getBytes(), timestamp }, new String[] {
                    "K", "Q", "T", "V" });
            Assert.assertEquals("partitionKey", new String((byte[]) result.get("K"), "UTF-8"));
            Assert.assertEquals("partition", new String((byte[]) result.get("Q"), "UTF-8"));
            Assert.assertEquals(timestamp, result.get("T"));
            Assert.assertEquals("bb", new String((byte[]) result.get("V"), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete(TEST_TABLE, new Object[] { "partitionKey", "partition".getBytes(),
                    timestamp });
        }
    }

    @Test
    public void testGet() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            obTableClient.insert(TEST_TABLE,
                new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value".getBytes() });
            Map<String, Object> result = obTableClient.get(TEST_TABLE, new Object[] {
                    "partitionKey".getBytes(), "partition".getBytes(), timeStamp }, new String[] {
                    "K", "Q", "T", "V" });
            Assert.assertEquals("partitionKey", new String((byte[]) result.get("K"), "UTF-8"));
            Assert.assertEquals("partition", new String((byte[]) result.get("Q"), "UTF-8"));
            Assert.assertEquals(timeStamp, result.get("T"));
            Assert.assertEquals("value", new String((byte[]) result.get("V"), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete(TEST_TABLE, new Object[] { "partitionKey", "partition".getBytes(),
                    timeStamp });
        }
    }

    @Test
    public void testUpdate() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            obTableClient.insert(TEST_TABLE,
                new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value".getBytes() });
            long affectedRow = obTableClient.update(TEST_TABLE,
                new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            Assert.assertEquals(1, affectedRow);
            Map<String, Object> result = obTableClient.get(TEST_TABLE, new Object[] {
                    "partitionKey".getBytes(), "partition".getBytes(), timeStamp }, new String[] {
                    "K", "Q", "T", "V" });
            Assert.assertEquals(timeStamp, result.get("T"));
            Assert.assertEquals("value1", new String((byte[]) result.get("V"), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete(TEST_TABLE, new Object[] { "partitionKey", "partition".getBytes(),
                    timeStamp });
        }
    }

    @Test
    public void testReplace() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            obTableClient.insert(TEST_TABLE,
                new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value".getBytes() });
            long affectedRow = obTableClient.replace(TEST_TABLE,
                new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            Assert.assertEquals(2, affectedRow);
            Map<String, Object> result = obTableClient.get(TEST_TABLE, new Object[] {
                    "partitionKey".getBytes(), "partition".getBytes(), timeStamp }, new String[] {
                    "K", "Q", "T", "V" });
            Assert.assertEquals(timeStamp, result.get("T"));
            Assert.assertEquals("value1", new String((byte[]) result.get("V"), "UTF-8"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete(TEST_TABLE, new Object[] { "partitionKey", "partition".getBytes(),
                    timeStamp });
        }
    }

    @Test
    public void testDelete() throws Exception {
        long timeStamp = System.currentTimeMillis();
        obTableClient.insert(TEST_TABLE,
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "V" }, new Object[] { "value".getBytes() });
        long affectedRow = obTableClient.delete(TEST_TABLE,
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp });
        Assert.assertEquals(1, affectedRow);
        Map<String, Object> result = obTableClient.get(TEST_TABLE,
            new Object[] { "partitionKey".getBytes(), "partition".getBytes(), timeStamp },
            new String[] { "K", "Q", "T", "V" });
        Assert.assertEquals(0, result.size());
    }

    @Test
    public void testQuery() throws Exception {
        long timeStamp = System.currentTimeMillis();
        cleanTable(TEST_TABLE);
        if (!obTableClient.isOdpMode()) {
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key1_5".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value2".getBytes() });

            TableQuery tableQuery = obTableClient.query(TEST_TABLE);
            tableQuery.addScanRange(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                    timeStamp }, new Object[] { "key2_1".getBytes(), "partition".getBytes(),
                    timeStamp });
            try {
                QueryResultSet resultSet = tableQuery.execute();
                int resultCount = 0;
                while (resultSet.next()) {
                    Map<String, Object> value = resultSet.getRow();
                    resultCount += 1;
                }
                Assert.assertFalse(resultSet.next());
                Assert.assertEquals(2, resultCount);
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(false);
            }
            tableQuery = obTableClient.query(TEST_TABLE);
            tableQuery.addScanRange(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                    timeStamp }, new Object[] { "key1_3".getBytes(), "partition".getBytes(),
                    timeStamp });
            tableQuery.select("T", "V", "Q", "K");
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
            result.next();
            Map<String, Object> row = result.getRow();
            Assert.assertEquals(4, row.size());
            Assert.assertEquals("key1_1", new String((byte[]) row.get("K")));
            Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
            Assert.assertEquals(timeStamp, row.get("T"));
            Assert.assertEquals("value1", new String((byte[]) row.get("V")));

            tableQuery = obTableClient.query(TEST_TABLE);
            tableQuery.addScanRange(
                new Object[] { "key1_1".getBytes(), ObObj.getMin(), ObObj.getMin() }, new Object[] {
                        "key1_8".getBytes(), ObObj.getMax(), ObObj.getMax() });
            tableQuery.select("Q", "T", "K", "V");
            result = tableQuery.execute();
            Assert.assertTrue(result.cacheSize() >= 2);
        } else {
            // TODO: generated column is not supported in ODP mode
            timeStamp = System.currentTimeMillis();
            try {
                obTableClient.insert(TEST_TABLE,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value1".getBytes() });
                obTableClient.insert(TEST_TABLE,
                    new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp + 1 },
                    new String[] { "V" }, new Object[] { "value2".getBytes() });
                obTableClient.insert(TEST_TABLE,
                    new Object[] { "key1_2".getBytes(), "partition".getBytes(), timeStamp },
                    new String[] { "V" }, new Object[] { "value3".getBytes() });

                TableQuery tableQuery = obTableClient.query(TEST_TABLE);
                tableQuery.addScanRange(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                        timeStamp }, new Object[] { "key2_1".getBytes(), "partition".getBytes(),
                        timeStamp });
                try {
                    tableQuery.execute();
                    Assert.fail();
                } catch (Exception e) {
                    Assert.assertTrue(e instanceof ObTableUnexpectedException);

                    if (obTableClient.isOdpMode()) {
                        Assert.assertEquals(ResultCodes.OB_ERR_UNEXPECTED.errorCode,
                            ((ObTableUnexpectedException) e).getErrorCode());
                    } else {
                        Assert.assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode,
                            ((ObTableUnexpectedException) e).getErrorCode());
                    }
                }

                tableQuery = obTableClient.query(TEST_TABLE);
                tableQuery.addScanRange(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                        timeStamp }, new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                        timeStamp + 1 });
                QueryResultSet result = tableQuery.execute();
                Assert.assertEquals(2, result.cacheSize());

                long expTimeStamp[] = { timeStamp, timeStamp + 1 };
                String expValues[] = { "value1", "value2" };

                for (int i = 0; i < 2; i++) {
                    Assert.assertEquals(true, result.next());
                    Map<String, Object> row = result.getRow();
                    Assert.assertEquals(4, row.size());
                    Assert.assertEquals("key1_1", new String((byte[]) row.get("K")));
                    Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                    Assert.assertEquals(expTimeStamp[i], row.get("T"));
                    Assert.assertEquals(expValues[i], new String((byte[]) row.get("V")));
                }
                Assert.assertFalse(result.next());

                // prefix range scan with scan range columns K
                tableQuery = obTableClient.query(TEST_TABLE);
                tableQuery.addScanRange(new Object[] { "key1_1".getBytes() },
                    new Object[] { "key1_1".getBytes() });
                tableQuery.setScanRangeColumns("K");
                tableQuery.select("T", "K", "V");
                result = tableQuery.execute();
                Assert.assertEquals(2, result.cacheSize());

                for (int i = 0; i < 2; i++) {
                    Assert.assertEquals(true, result.next());
                    Map<String, Object> row = result.getRow();
                    Assert.assertEquals(3, row.size());
                    Assert.assertEquals("key1_1", new String((byte[]) row.get("K")));
                    Assert.assertEquals(expTimeStamp[i], row.get("T"));
                    Assert.assertEquals(expValues[i], new String((byte[]) row.get("V")));
                }
                Assert.assertFalse(result.next());

                // scan using empty range on key partitioned table
                tableQuery = obTableClient.query(TEST_TABLE);
                result = tableQuery.execute();
                Assert.assertEquals(3, result.cacheSize());
                Assert.assertTrue(result.next());
                Assert.assertEquals(4, result.getRow().size());
            } catch (Exception e) {
                e.printStackTrace();
                Assert.assertTrue(false);
            } finally {
                cleanTable(TEST_TABLE);
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
        try {
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp + 1 },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp + 2 },
                new String[] { "V" }, new Object[] { "value2".getBytes() });
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp + 1 },
                new String[] { "V" }, new Object[] { "value2".getBytes() });
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp + 2 },
                new String[] { "V" }, new Object[] { "value1".getBytes() });

            // key partitioned table do not support range query

            // query key2_1
            TableQuery tableQuery = obTableClient.query(TEST_TABLE);
            tableQuery.addScanRange(new Object[] { "key2_1".getBytes(), "value0".getBytes() },
                new Object[] { "key2_1".getBytes(), "value9".getBytes() });
            // TODO: do param check, must specify select columns
            tableQuery.setScanRangeColumns("K", "V");
            tableQuery.select("K", "V", "T");
            tableQuery.indexName("i1");
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());
            for (int i = 1; i <= 2; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals(3, row.size());
                Assert.assertEquals("key2_1", new String((byte[]) row.get("K")));
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
                Assert.assertEquals(timeStamp + i, row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());

            // query key3_1
            tableQuery = obTableClient.query(TEST_TABLE);
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
                Assert.assertEquals(4, row.size());
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
                Assert.assertEquals(4, row.size());
                Assert.assertEquals("key3_1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + 3 - i, row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }

            // query key2_1 using K prefix
            tableQuery = obTableClient.query(TEST_TABLE);
            tableQuery.addScanRange(new Object[] { "key2_1".getBytes() },
                new Object[] { "key2_1".getBytes() });
            tableQuery.setScanRangeColumns("K");
            tableQuery.indexName("i1");
            result = tableQuery.execute();
            Assert.assertEquals(2, result.cacheSize());
            for (int i = 1; i <= 2; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals(4, row.size());
                Assert.assertEquals("key2_1", new String((byte[]) row.get("K")));
                Assert.assertEquals("partition", new String((byte[]) row.get("Q")));
                Assert.assertEquals(timeStamp + i, row.get("T"));
                Assert.assertEquals("value" + i, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp + 1 });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp + 2 });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp + 1 });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp + 2 });
        }
    }

    @Test
    public void testLargeQuery() throws Exception {
        int batchSize = 120;
        String key = "key";
        String qualifier = "partition";
        String value = "V";
        int columnSize = 0;
        try {
            for (long i = 0; i < batchSize; i++) {
                obTableClient.insert(TEST_TABLE,
                    new Object[] { key.getBytes(), qualifier.getBytes(), i }, new String[] { "V" },
                    new Object[] { value.getBytes() });
            }
            TableQuery tableQuery = obTableClient.query(TEST_TABLE);
            // fixme: generated column is not supported by odp mode
            if (obTableClient.isOdpMode()) {
                columnSize = 4;
                tableQuery.setScanRangeColumns("K");
                tableQuery.addScanRange(new Object[] { "key".getBytes() },
                    new Object[] { "key".getBytes() });
                if (ObGlobal.obVsnMajor() < 4) {
                    tableQuery.select("K", "Q", "T", "V");
                }
            } else {
                // todo: scan_range_columns cannot be used for routing
                columnSize = 5;
                tableQuery.addScanRange(new Object[] { "key".getBytes(), "a".getBytes(),
                        Long.MIN_VALUE }, new Object[] { "key".getBytes(), "z".getBytes(),
                        Long.MAX_VALUE });
                if (ObGlobal.obVsnMajor() < 4) {
                    tableQuery.select("K", "Q", "T", "V", "K_PREFIX");
                }
            }
            QueryResultSet result = tableQuery.execute();
            Assert.assertEquals(batchSize, result.cacheSize());

            for (long i = 0; i < batchSize; i++) {
                Assert.assertEquals(true, result.next());
                Map<String, Object> row = result.getRow();
                Assert.assertEquals(columnSize, row.size());
                Assert.assertEquals(key, new String((byte[]) row.get("K")));
                Assert.assertEquals(qualifier, new String((byte[]) row.get("Q")));
                Assert.assertEquals(i, row.get("T"));
                Assert.assertEquals(value, new String((byte[]) row.get("V")));
            }
            Assert.assertFalse(result.next());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 0; i < batchSize; i++) {
                obTableClient.delete(TEST_TABLE,
                    new Object[] { key.getBytes(), qualifier.getBytes(), i });
            }
        }
    }

    @Test
    public void testBatch() throws Exception {
        long timeStamp = System.currentTimeMillis();
        try {
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch(TEST_TABLE);
            tableBatchOps.delete(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                    timeStamp });
            tableBatchOps.insert(new Object[] { "key2_1".getBytes(), "partition".getBytes(),
                    timeStamp }, new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(new Object[] { "key3_1".getBytes(), "partition".getBytes(),
                    timeStamp }, new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get(TEST_TABLE, new Object[] { "key2_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key2_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get(TEST_TABLE, new Object[] { "key3_1".getBytes(),
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
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testBatchConcurrent() throws Exception {
        long timeStamp = System.currentTimeMillis();
        // This opetion is useless in ODP mode
        obTableClient.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
        try {
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch(TEST_TABLE);
            tableBatchOps.delete(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                    timeStamp });
            tableBatchOps.insert(new Object[] { "key2_1".getBytes(), "partition".getBytes(),
                    timeStamp }, new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(new Object[] { "key3_1".getBytes(), "partition".getBytes(),
                    timeStamp }, new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get(TEST_TABLE, new Object[] { "key2_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key2_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get(TEST_TABLE, new Object[] { "key3_1".getBytes(),
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
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testBatchConcurrentWithPriority() throws Exception {
        long timeStamp = System.currentTimeMillis();
        // This following two option in ODP mode is useless.
        ThreadLocalMap.setProcessHighPriority();
        obTableClient.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
        try {
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            obTableClient.insert(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value1".getBytes() });
            TableBatchOps tableBatchOps = obTableClient.batch(TEST_TABLE);
            tableBatchOps.delete(new Object[] { "key1_1".getBytes(), "partition".getBytes(),
                    timeStamp });
            tableBatchOps.insert(new Object[] { "key2_1".getBytes(), "partition".getBytes(),
                    timeStamp }, new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(new Object[] { "key3_1".getBytes(), "partition".getBytes(),
                    timeStamp }, new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = obTableClient.get(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp },
                new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(0, getResult.size());

            getResult = obTableClient.get(TEST_TABLE, new Object[] { "key2_1".getBytes(),
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals("key2_1", new String((byte[]) getResult.get("K")));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = obTableClient.get(TEST_TABLE, new Object[] { "key3_1".getBytes(),
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
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key1_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key2_1".getBytes(), "partition".getBytes(), timeStamp });
            obTableClient.delete(TEST_TABLE,
                new Object[] { "key3_1".getBytes(), "partition".getBytes(), timeStamp });
        }
    }

    @Test
    public void testPartitionLocation() throws Exception {
        obTableClient.setRunningMode(ObTableClient.RunningMode.NORMAL);
        String testTable = "testPartitionKeyComplex";
        obTableClient.addRowKeyElement(testTable,
            new String[] { "c0", "c1", "c2", "c3", "c4", "c5" });
        try {
            cleanTable(testTable);
            Connection connection = ObTableClientTestUtil.getConnection();
            Statement statement = connection.createStatement();
            for (int i = 0; i < 64; i++) {
                byte c0 = (byte) i;
                int c1 = i * (i + 1) * (i + 2);
                long c2 = i * (i + 1) * (i + 2);
                String c3 = generateRandomStringByUUID(10);
                String c4 = generateRandomStringByUUID(5) + c2 + generateRandomStringByUUID(5);
                String c5 = generateRandomStringByUUID(5) + c3 + generateRandomStringByUUID(5);

                // use sql to insert data
                statement.execute("insert into " + testTable
                                  + "(c0, c1, c2, c3, c4, c5, c6) values (" + c0 + "," + c1 + ","
                                  + c2 + ",'" + c3 + "','" + c4 + "','" + c5 + "'," + "'value')");

                // get data by obkv interface
                Map<String, Object> result = obTableClient.get(testTable, new Object[] { c0, c1,
                        c2, c3.getBytes(), c4, c5 }, new String[] { "c0", "c1", "c2", "c3", "c4",
                        "c5", "c6" });
                Assert.assertEquals(7, result.size());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            cleanTable(testTable);
        }
    }

    @Test
    public void testTwoPartitionQuery() throws Exception {
        obTableClient.setRunningMode(ObTableClient.RunningMode.NORMAL);
        String testTable = "testPartitionKeyComplex";
        obTableClient.addRowKeyElement(testTable,
            new String[] { "c0", "c1", "c2", "c3", "c4", "c5" });
        try {
            cleanTable(testTable);
            byte c0 = (byte) 0;
            int c1 = 10001;
            long c2 = 100001;
            String c3 = generateRandomStringByUUID(10);
            String c4 = generateRandomStringByUUID(5) + c2 + generateRandomStringByUUID(5);
            String c5 = generateRandomStringByUUID(5) + c3 + generateRandomStringByUUID(5);
            TableQuery query = obTableClient
                .query(testTable)
                .addScanRange(new Object[] { c0, c1, c2, c3, c4, c5 },
                    new Object[] { c0, c1, c2, c3, c4, c5 })
                .select("c1", "c2", "c3", "c4", "c5", "c6")
                .setFilter(
                    andList(compareVal(ObCompareOp.GE, "c2", 0),
                        compareVal(ObCompareOp.LE, "c3", 0)));
            QueryResultSet resultSet = query.execute();
            Assert.assertEquals(0, resultSet.cacheSize());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            cleanTable(testTable);
        }
    }

    private void insertMultiPartition(ObTableClient obTableClient, int count, String tableName)
                                                                                               throws Exception {
        int num = 1;
        if (count <= 5) {
            while (num <= count) {
                String uid = "GU1" + String.format("%04d", num);
                String object_id = "GO1" + String.format("%04d", num);
                String ver_oid = "GV1" + String.format("%04d", num);
                String data_id = "GD1" + String.format("%04d", num);
                Row rowKey = row(colVal("id", Integer.toUnsignedLong(num)), colVal("uid", uid),
                    colVal("object_id", object_id));
                Row rows = row(colVal("type", num), colVal("ver_oid", ver_oid),
                    colVal("ver_ts", System.currentTimeMillis()), colVal("data_id", data_id));
                MutationResult result = obTableClient.insert(tableName).setRowKey(rowKey)
                    .addMutateRow(rows).execute();
                Assert.assertEquals(1, result.getAffectedRows());
                num++;
            }
        } else {
            BatchOperation batchOperation = obTableClient.batchOperation(tableName);
            while (num <= count) {
                String uid = "GU1" + String.format("%04d", num);
                String object_id = "GO1" + String.format("%04d", num);
                String ver_oid = "GV1" + String.format("%04d", num);
                String data_id = "GD1" + String.format("%04d", num);
                Row rowKey = row(colVal("id", Integer.toUnsignedLong(num)), colVal("uid", uid),
                    colVal("object_id", object_id));
                Row rows = row(colVal("type", num), colVal("ver_oid", ver_oid),
                    colVal("ver_ts", System.currentTimeMillis()), colVal("data_id", data_id));
                batchOperation.addOperation(insert().setRowKey(rowKey).addMutateRow(rows));
                num++;
                if (num % 100 == 0 || num == count + 1) {
                    BatchOperationResult result = batchOperation.execute();
                    batchOperation = obTableClient.batchOperation(tableName);
                }
            }
        }
    }

    @Test
    public void testHashKeyQuery() throws Exception {
        String tableName = "hash_key_sub_part";
        try {
            normalClient.addRowKeyElement(tableName, new String[] { "id", "uid" });
            insertMultiPartition(normalClient, 20, tableName);
            TableQuery query = normalClient.query(tableName);
            QueryResultSet resultSet;
            query.clear();
            query.addScanRange(new Object[] { 1L, "GU20004", "GO10001" }, new Object[] { 30L,
                    "GU20004", "GO10030" });
            resultSet = query.execute();

            int resultCount = 0;
            while (resultSet.next()) {
                Map<String, Object> value = resultSet.getRow();
                resultCount += 1;
            }
            Assert.assertFalse(resultSet.next());
            Assert.assertEquals(19, resultCount);
        } finally {
            cleanTable(tableName);
        }
    }
}

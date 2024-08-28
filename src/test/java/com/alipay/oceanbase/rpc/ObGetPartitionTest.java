/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.location.model.partition.Partition;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import net.bytebuddy.implementation.auxiliary.MethodCallProxy;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.cleanTable;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.generateRandomStringByUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ObGetPartitionTest {

    public ObTableClient        client;
    private static final String TABLE_NAME  = "test_mutation";
    private static final String TABLE_NAME1 = "test_blob_table";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    /*
    CREATE TABLE `test_mutation` (
    `c1` bigint NOT NULL,
    `c2` varchar(20) NOT NULL,
    `c3` varbinary(1024) DEFAULT NULL,
    `c4` bigint DEFAULT NULL,
    PRIMARY KEY(`c1`, `c2`)) partition by range columns (`c1`) (
          PARTITION p0 VALUES LESS THAN (300),
          PARTITION p1 VALUES LESS THAN (1000),
          PARTITION p2 VALUES LESS THAN MAXVALUE);
     */
    @Test
    public void testWithOneLevelPartitionTable() throws Exception {
        BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
        Object values[][] = { { 1L, "c2_val", "c3_val", 100L }, { 400L, "c2_val", "c3_val", 100L },
                { 401L, "c2_val", "c3_val", 100L }, { 1000L, "c2_val", "c3_val", 100L },
                { 1001L, "c2_val", "c3_val", 100L }, { 1002L, "c2_val", "c3_val", 100L }, };
        int rowCnt = values.length;
        try {
            // batch insert
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("c1", curRow[0]), colVal("c2", curRow[1])));
                insertOrUpdate.addMutateRow(row(colVal("c3", curRow[2]), colVal("c4", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            Assert.assertEquals(rowCnt, batchOperationResult.size());
            // test get all partitions
            List<Partition> partitions = client.getPartition(TABLE_NAME);
            Assert.assertEquals(3, partitions.size());
            Map<Long, Partition> partIdMap = new HashMap<>();
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
                partIdMap.put(partition.getPartitionId(), partition);
            }

            // test get the partition
            Partition partition = client.getPartition(TABLE_NAME, row(colVal("c1", 1L), colVal("c2", "c2_val")));
            Assert.assertNotNull(partIdMap.get(partition.getPartitionId()));
            // test get the partition with only partition key
            Partition partition1 = client.getPartition(TABLE_NAME, row(colVal("c1", 1L)));
            Assert.assertEquals(partition.getPartitionId(), partition1.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME);
                delete.setRowKey(row(colVal("c1", values[j][0]), colVal("c2", values[j][1])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    /*
    CREATE TABLE IF NOT EXISTS `test_blob_table` (
    `c1` varchar(20) NOT NULL,
    `c2` blob DEFAULT NULL,
    PRIMARY KEY (`c1`)
    );
    */
    @Test
    public void testWithNonPartitionTable() throws Exception {
        try {
            client.insert(TABLE_NAME1, new Object[] { "foo" }, new String[] { "c2" },
                new Object[] { "bar".getBytes() });

            client.insert(TABLE_NAME1, new Object[] { "qux" }, new String[] { "c2" },
                new String[] { "qux" });

            Partition partition = client.getPartition(TABLE_NAME1, row(colVal("c1", "qux")));
            Assert.assertEquals(0L, partition.getPartitionId().longValue());
            System.out.println(partition.toString());

            List<Partition> partitions = client.getPartition(TABLE_NAME1);
            Assert.assertEquals(1L, partitions.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            Delete delete = client.delete("test_blob_table");
            delete.setRowKey(row(colVal("c1", "foo")));
            MutationResult res = delete.execute();
            Assert.assertEquals(1, res.getAffectedRows());

            Delete delete1 = client.delete("test_blob_table");
            delete1.setRowKey(row(colVal("c1", "qux")));
            res = delete1.execute();
            Assert.assertEquals(1, res.getAffectedRows());
        }
    }

    @Test
    public void testAddScanWithPartition() throws Exception {
        BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
        Object values[][] = { { 1L, "c2_val", "c3_val", 100L }, { 400L, "c2_val", "c3_val", 100L },
                { 401L, "c2_val", "c3_val", 100L }, { 1000L, "c2_val", "c3_val", 100L },
                { 1001L, "c2_val", "c3_val", 100L }, { 1002L, "c2_val", "c3_val", 100L }, };
        int rowCnt = values.length;
        try {
            // batch insert
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("c1", curRow[0]), colVal("c2", curRow[1])));
                insertOrUpdate.addMutateRow(row(colVal("c3", curRow[2]), colVal("c4", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            Assert.assertEquals(rowCnt, batchOperationResult.size());

            Partition partition = client.getPartition(TABLE_NAME,
                row(colVal("c1", 1L), colVal("c2", "c2_val")));
            System.out.println(partition.toString());
            QueryResultSet result = client.query(TABLE_NAME)
                .addScanRange(partition.start(), partition.end()).execute();
            Assert.assertEquals(1, result.cacheSize());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME);
                delete.setRowKey(row(colVal("c1", values[j][0]), colVal("c2", values[j][1])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    @Test
    public void testGetPartitionInBatchOperation() throws Exception {
        Object values[][] = { { 1L, "c2_val", "c3_val", 100L }, { 400L, "c2_val", "c3_val", 100L },
                { 1001L, "c2_val", "c3_val", 100L } };
        int rowCnt = values.length;
        try {
            List<Mutation> mutations = new ArrayList<>();
            Insert ins1 = client.insert(TABLE_NAME)
                    .setRowKey(row(colVal("c1", 1L), colVal("c2", "c2_val")))
                    .addMutateColVal(colVal("c3", "c3_val"), colVal("c4", 100L));
            mutations.add(ins1);
            Insert ins2 = client.insert(TABLE_NAME)
                    .setRowKey(row(colVal("c1", 400L), colVal("c2", "c2_val")))
                    .addMutateColVal(colVal("c3", "c3_val"), colVal("c4", 100L));
            mutations.add(ins2);
            Insert ins3 = client.insert(TABLE_NAME)
                    .setRowKey(row(colVal("c1", 1001L), colVal("c2", "c2_val")))
                    .addMutateColVal(colVal("c3", "c3_val"), colVal("c4", 100L));
            mutations.add(ins3);
            Update upd = client.update(TABLE_NAME)
                    .setRowKey(row(colVal("c1", 1L), colVal("c2", "c2_val")))
                    .addMutateRow(row(colVal("c3", "v3_v2")));
            mutations.add(upd);
            List<Partition> partitions = client.getPartition(TABLE_NAME);
            Assert.assertEquals(3, partitions.size());
            // build partitionId -> operations map
            Map<Long, List<Mutation>> partitionIdOperationMap = new HashMap<>();
            for (Mutation mutation : mutations) {
                Partition partition = client.getPartition(TABLE_NAME, mutation.getRowKey());
                List<Mutation> mutationsInSamePart = partitionIdOperationMap.get(partition.getPartId());
                if(mutationsInSamePart == null) {
                    mutationsInSamePart = new ArrayList<>();
                    mutationsInSamePart.add(mutation);
                    partitionIdOperationMap.put(partition.getPartId(), mutationsInSamePart);
                } else {
                    mutationsInSamePart.add(mutation);
                }
            }
            Assert.assertEquals(2, partitionIdOperationMap.get(partitions.get(0).getPartId()).size());
            Assert.assertEquals(1, partitionIdOperationMap.get(partitions.get(1).getPartId()).size());
            Assert.assertEquals(1, partitionIdOperationMap.get(partitions.get(2).getPartId()).size());
            // single-partition batch test
            for (Map.Entry<Long, List<Mutation>> entry : partitionIdOperationMap.entrySet()) {
                BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
                for (Mutation mutation : entry.getValue()) {
                    batchOperation.addOperation(mutation);
                }
                BatchOperationResult batchResult = batchOperation.execute();
                Assert.assertEquals(entry.getValue().size(), batchResult.size());
                for (Object result : batchResult.getResults()) {
                    Assert.assertTrue(result instanceof MutationResult);
                    Assert.assertEquals(1, ((MutationResult) result).getAffectedRows());
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME);
                delete.setRowKey(row(colVal("c1", values[j][0]), colVal("c2", values[j][1])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    @Test
    public void testGetPartitionWithRowKeyValues() throws Exception {
        client.addRowKeyElement(TABLE_NAME, new String[] { "c1", "c2" });
        Object values[][] = { { 1L, "c2_val", "c3_val", 100L }, { 400L, "c2_val", "c3_val", 100L },
                { 1001L, "c2_val", "c3_val", 100L } };
        int rowCnt = values.length;
        try {
            client.insert(TABLE_NAME, new Object[] { 1L, "c2_val" }, new String[] { "c3", "c4" },
                new Object[] { "c3_val", 100L });

            client.insert(TABLE_NAME, new Object[] { 400L, "c2_val" }, new String[] { "c3", "c4" },
                new Object[] { "c3_val", 100L });

            client.insert(TABLE_NAME, new Object[] { 1001L, "c2_val" },
                new String[] { "c3", "c4" }, new Object[] { "c3_val", 100L });

            List<Partition> partitions = client.getPartition(TABLE_NAME);
            Assert.assertEquals(3, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }

            Partition partition = client.getPartition(TABLE_NAME, new Object[] { 1L, "c2_val" });
            Assert.assertNotNull(partition.getPartitionId());
            // test get partition with partition key
            Partition partition_prefix = client.getPartition(TABLE_NAME, new Object[] { 1L });
            Assert.assertEquals(partition.getPartitionId(), partition_prefix.getPartitionId());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME);
                delete.setRowKey(row(colVal("c1", values[j][0]), colVal("c2", values[j][1])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    /*
    * CREATE TABLE IF NOT EXISTS `testSubStrKey` (
                    `K` varbinary(1024),
                    `Q` varbinary(256),
                    `T` bigint,
                    `V` varbinary(1024),
                    K_PREFIX varbinary(1024) generated always as (substring(`K`, 1, 4)),
                    PRIMARY KEY(`K`, `Q`, `T`)
                ) partition by key(K_PREFIX) partitions 15;
    * */
    @Test
    public void testOneLevelSubStrKeyPartitionWithBatch() throws Exception {
        String table_name = "testSubStrKey";
        BatchOperation batchOperation = client.batchOperation(table_name);
        client.setRunningMode(ObTableClient.RunningMode.HBASE);
        long firstTs = System.currentTimeMillis();
        Object values[][] = { { "K_val1", "Q_val1", firstTs, "V_val1" },
                { "K_val2", "Q_val2", System.currentTimeMillis(), "V_val2" },
                { "K_val3", "Q_val3", System.currentTimeMillis(), "V_val3" },
                { "K_val4", "Q_val4", System.currentTimeMillis(), "V_val4" },
                { "K_val5", "Q_val5", System.currentTimeMillis(), "V_val5" },
                { "K_val6", "Q_val6", System.currentTimeMillis(), "V_val6" },
                { "K_val1", "Q_val2", firstTs, "V_val1" } };
        int rowCnt = values.length;

        try {
            // batch insert
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("K", curRow[0]), colVal("Q", curRow[1]),
                    colVal("T", curRow[2])));
                insertOrUpdate.addMutateRow(row(colVal("V", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            Assert.assertEquals(rowCnt, batchOperationResult.size());
            // test get all partitions
            List<Partition> partitions = client.getPartition(table_name);
            Assert.assertEquals(15, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }
            // test get the first partition
            Partition partition1 = client.getPartition(table_name,
                row(colVal("K", "K_val1"), colVal("Q", "Q_val1"), colVal("T", firstTs)));
            // test get the partition with only partition key
            Partition partition2 = client.getPartition(table_name,
                row(colVal("K", "K_val1"), colVal("Q", "Q_val2"), colVal("T", firstTs)));
            Assert.assertEquals(partition1.getPartitionId(), partition2.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(table_name);
                delete.setRowKey(row(colVal("K", values[j][0]), colVal("Q", values[j][1]),
                    colVal("T", values[j][2])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    /*
    * CREATE TABLE IF NOT EXISTS `testHash`(
        `K` bigint,
        `Q` varbinary(256),
        `T` bigint,
        `V` varbinary(1024),
        INDEX i1(`K`, `V`) local,
        PRIMARY KEY(`K`, `Q`, `T`)
    ) partition by hash(`K`) partitions 15;
    * */
    @Test
    public void testGetPartitionInConcurrentOldBatch() throws Exception {
        long timeStamp = System.currentTimeMillis();
        client.setRunningMode(ObTableClient.RunningMode.HBASE);
        client.setRuntimeBatchExecutor(Executors.newFixedThreadPool(3));
        try {
            client.insert("testHash", new Object[] { timeStamp + 1L, "partition".getBytes(),
                    timeStamp }, new String[] { "V" }, new Object[] { "value1L".getBytes() });
            client.insert("testHash", new Object[] { timeStamp + 5L, "partition".getBytes(),
                    timeStamp }, new String[] { "V" }, new Object[] { "value1L".getBytes() });
            Partition partition = client.getPartition(
                "testHash",
                row(colVal("K", timeStamp + 1), colVal("Q", "partition".getBytes()),
                    colVal("T", timeStamp)));
            Assert.assertTrue(partition.getPartId() < 16);
            TableBatchOps tableBatchOps = client.batch("testHash");
            tableBatchOps
                .delete(new Object[] { timeStamp + 1L, "partition".getBytes(), timeStamp });
            tableBatchOps.insert(
                new Object[] { timeStamp + 3L, "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value2".getBytes() });
            tableBatchOps.replace(
                new Object[] { timeStamp + 5L, "partition".getBytes(), timeStamp },
                new String[] { "V" }, new Object[] { "value2".getBytes() });
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(3, batchResult.size());
            Assert.assertEquals(1L, batchResult.get(0));
            Assert.assertEquals(1L, batchResult.get(1));
            Assert.assertEquals(2L, batchResult.get(2));

            Map<String, Object> getResult = client.get("testHash", new Object[] { timeStamp + 1L,
                    "partition".getBytes(), timeStamp }, new String[] { "K", "Q", "T", "V" });
            Assert.assertEquals(0, getResult.size());
            Partition del_partition = client.getPartition(
                "testHash",
                row(colVal("K", timeStamp + 1), colVal("Q", "partition".getBytes()),
                    colVal("T", timeStamp)));
            Assert.assertTrue(del_partition.getPartId() < 16);

            getResult = client.get("testHash",
                new Object[] { timeStamp + 3L, "partition".getBytes(), timeStamp }, new String[] {
                        "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals(timeStamp + 3L, getResult.get("K"));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));

            getResult = client.get("testHash",
                new Object[] { timeStamp + 5L, "partition".getBytes(), timeStamp }, new String[] {
                        "K", "Q", "T", "V" });

            Assert.assertEquals(4, getResult.size());

            Assert.assertEquals(timeStamp + 5L, getResult.get("K"));
            Assert.assertEquals("partition", new String((byte[]) getResult.get("Q")));
            Assert.assertEquals(timeStamp, getResult.get("T"));
            Assert.assertEquals("value2", new String((byte[]) getResult.get("V")));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            cleanTable("testHash");
        }
    }

    /*
    * CREATE TABLE IF NOT EXISTS `testPartitionKeyComplex` (
        `c0` tinyint NOT NULL,
        `c1` int NOT NULL,
        `c2` bigint NOT NULL,
        `c3` varbinary(1024) NOT NULL,
        `c4` varchar(1024) NOT NULL,
        `c5` varchar(1024) NOT NULL,
        `c6` varchar(20) default NULL,
    PRIMARY KEY (`c0`, `c1`, `c2`, `c3`, `c4`, `c5`)
    ) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
    partition by key(`c0`, `c1`, `c2`, `c3`, `c4`) subpartition by key(`c5`) subpartitions 4 partitions 16;
    * */
    @Test
    public void testWithTwoLevelPartitionWithScan() throws Exception {
        client.setRunningMode(ObTableClient.RunningMode.NORMAL);
        String testTable = "testPartitionKeyComplex";
        client.addRowKeyElement(testTable, new String[] { "c0", "c1", "c2", "c3", "c4", "c5" });
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
                Partition partition = client.getPartition(testTable, new Object[] { c0, c1, c2, c3,
                        c4, c5 });
                System.out.println(partition.toString());
                QueryResultSet result = client.query(testTable)
                    .addScanRange(partition.start(), partition.end()).execute();
                Assert.assertTrue(result.cacheSize() >= 1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            cleanTable(testTable);
        }
    }

    /*
    *
    * CREATE TABLE IF NOT EXISTS `testRange` (
        `c1` int NOT NULL,
        `c2` varchar(20) NOT NULL,
        `c3` varbinary(1024) DEFAULT NULL,
        `c4` bigint DEFAULT NULL,
        PRIMARY KEY(`c1`, `c2`)) partition by range columns (`c1`, `c2`) (
              PARTITION p0 VALUES LESS THAN (300, 't'),
              PARTITION p1 VALUES LESS THAN (1000, 'T'),
              PARTITION p2 VALUES LESS THAN (MAXVALUE, MAXVALUE));
    *
    * CREATE TABLE IF NOT EXISTS `testHash`(
        `K` bigint,
        `Q` varbinary(256),
        `T` bigint,
        `V` varbinary(1024),
        INDEX i1(`K`, `V`) local,
        PRIMARY KEY(`K`, `Q`, `T`)
    ) partition by hash(`K`) partitions 15;
    *
    * CREATE TABLE IF NOT EXISTS `testKey` (
        `K` varbinary(1024),
        `Q` varbinary(256),
        `T` bigint,
        `V` varbinary(1024),
        PRIMARY KEY(`K`, `Q`, `T`)
    ) partition by key(K) partitions 15;
    * */
    @Test
    public void testConcurrentGetPartition() throws Exception {
        String[] table_names = { "testHash", "testKey", "testRange" };
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Random random = new Random();
        client.addRowKeyElement("testHash", new String[] { "K", "Q", "T" });
        client.addRowKeyElement("testKey", new String[] { "K", "Q", "T" });
        client.addRowKeyElement("testRange", new String[] { "c1", "c2" });
        AtomicInteger cnt = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(20);

        try {
            for (int i = 0; i < 20; ++i) {
                executorService.submit(() -> {
                    try {
                        cnt.getAndIncrement();
                        String table_name = table_names[random.nextInt(table_names.length)];
                        List<Partition> partitions = client.getPartition(table_name);
                        if (table_name.equalsIgnoreCase("testHash")) {
                            Assert.assertEquals(15, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testHash: " + partition.toString());
                            }
                            MutationResult resultSet = client.insert("testHash")
                                    .setRowKey(row(colVal("K", random.nextLong()), colVal("Q", "Q_val1"), colVal("T", System.currentTimeMillis())))
                                    .addMutateRow(row(colVal("V", "V_val1"))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                        } else if (table_name.equalsIgnoreCase("testKey")) {
                            Assert.assertEquals(15, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testKey: " + partition.toString());
                            }
                            byte[] bytes = new byte[10];
                            random.nextBytes(bytes);
                            MutationResult resultSet = client.insert("testKey")
                                    .setRowKey(row(colVal("K", bytes), colVal("Q", "Q_val1"), colVal("T", System.currentTimeMillis())))
                                    .addMutateRow(row(colVal("V", "V_val1"))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                        } else {
                            Assert.assertEquals(3, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testRange: " + partition.toString());
                            }
                            MutationResult resultSet = client.insert("testRange")
                                    .setRowKey(row(colVal("c1", random.nextInt()), colVal("c2", "c2_val1")))
                                    .addMutateRow(row(colVal("c3", "c3_val1"), colVal("c4", 10L))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assert.assertTrue(false);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
            Assert.assertEquals(20, cnt.get());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            executorService.shutdown();
            try {
                // wait for all tasks done
                if (!executorService.awaitTermination(2000L, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(2000L, TimeUnit.MILLISECONDS)) {
                        System.err.println("the thread pool did not shut down");
                        Assert.assertTrue(false);
                    }
                }
                cleanTable("testHash");
                cleanTable("testKey");
                cleanTable("testRange");
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
                Assert.assertTrue(false);
            }
        }
    }
}

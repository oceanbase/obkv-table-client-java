package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.location.model.partition.Partition;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Delete;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.ConcurrentTask;
import com.alipay.oceanbase.rpc.table.ConcurrentTaskExecutor;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.cleanTable;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.generateRandomStringByUUID;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;
import static java.lang.StrictMath.abs;

public class ODPGetPartitionMetaTest {
    public ObTableClient        client;

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    /*
    * CREATE TABLE IF NOT EXISTS `test_tinyint_table` (
        `c1` varchar(20) NOT NULL,
        `c2` tinyint(4) DEFAULT NULL,
        PRIMARY KEY (`c1`)
      );
    * */
    @Test
    public void testNonPartition() throws Exception {
        String table_name = "test_tinyint_table";
        BatchOperation batchOperation = client.batchOperation(table_name);
        client.setRunningMode(ObTableClient.RunningMode.NORMAL);
        Object values[][] = { { "c1_1", (byte) 1 }, { "c1_2", (byte) 11 },
                { "c1_3", (byte) 51 }, { "c1_4", (byte) 101 } };
        int rowCnt = values.length;

        try {
            // test batch insert
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("c1", curRow[0])));
                insertOrUpdate.addMutateRow(row(colVal("c2", curRow[1])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            // test get all partitions
            List<Partition> partitions = client.getPartition(table_name);
            Assert.assertEquals(1, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }
            Long lastPartId = -1L;
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                Partition partition = client.getPartition(table_name, row(colVal("c1", curRow[0])));
                if (lastPartId == -1L) {
                    lastPartId = partition.getPartitionId();
                }
                else {
                    Assert.assertEquals(lastPartId, partition.getPartitionId());
                }
            }
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(table_name);
                delete.setRowKey(row(colVal("c1", values[j][0])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    /*
    * CREATE TABLE IF NOT EXISTS `testKey` (
        `K` varbinary(1024),
        `Q` varbinary(256),
        `T` bigint,
        `V` varbinary(1024),
        PRIMARY KEY(`K`, `Q`, `T`)
    ) partition by key(K) partitions 15;
    * */
    @Test
    public void testOneLevelKeyPartition() throws Exception {
        String table_name = "testKey";
        BatchOperation batchOperation = client.batchOperation(table_name);
        client.setRunningMode(ObTableClient.RunningMode.HBASE);
        Object values[][] = { { "K_val1", "Q_val1", 1L, "V_val1" }, { "K_val2", "Q_val2", 101L, "V_val2" },
                { "K_val3", "Q_val3", 501L, "V_val3" }, { "K_val4", "Q_val4", 1001L, "V_val4" },
                { "K_val5", "Q_val5", 5001L, "V_val5" }, { "K_val6", "Q_val6", 10001L, "V_val6" }, };
        int rowCnt = values.length;

        try {
            // test batch insert
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("K", curRow[0]), colVal("Q", curRow[1]), colVal("T", curRow[2])));
                insertOrUpdate.addMutateRow(row(colVal("V", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            // test get all partitions
            List<Partition> partitions = client.getPartition(table_name);
            Assert.assertEquals(15, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }

            // test get the partition with only partition key with only partition key
            Partition first_partition = client.getPartition(table_name, row(colVal("K", "K_val1"), colVal("Q", "Q_val1"), colVal("T", 1L)));
            Partition part_key_partition = client.getPartition(table_name, row(colVal("K", "K_val1")));
            Assert.assertEquals(first_partition.getPartitionId(), part_key_partition.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(table_name);
                delete.setRowKey(row(colVal("K", values[j][0]), colVal("Q", values[j][1]), colVal("T", values[j][2])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    /*
    * CREATE TABLE IF NOT EXISTS `testHash` (
        `K` int,
        `Q` varbinary(256),
        `T` bigint,
        `V` varbinary(1024),
        PRIMARY KEY(`K`, `Q`, `T`)
    ) partition by hash(K) partitions 15;
    * */
    @Test
    public void testOneLevelHashPartition() throws Exception {
        String table_name = "testHash";
        BatchOperation batchOperation = client.batchOperation(table_name);
        client.setRunningMode(ObTableClient.RunningMode.HBASE);
        Object values[][] = { { 1, "Q_val1", 1L, "V_val1" }, { 10, "Q_val2", 101L, "V_val2" },
                { 501, "Q_val3", 501L, "V_val3" }, { 1001, "Q_val4", 1001L, "V_val4" },
                { 5001, "Q_val5", 5001L, "V_val5" }, { 10001, "Q_val6", 10001L, "V_val6" }, };
        int rowCnt = values.length;

        try {
            // batch insert
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("K", curRow[0]), colVal("Q", curRow[1]), colVal("T", curRow[2])));
                insertOrUpdate.addMutateRow(row(colVal("V", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            // test get all partitions
            List<Partition> partitions = client.getPartition(table_name);
            Assert.assertEquals(15, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }

            // test get the partition with only partition key with only partition key
            Partition first_partition = client.getPartition(table_name, row(colVal("K", 1), colVal("Q", "Q_val1"), colVal("T", 1L)));
            Partition part_key_partition = client.getPartition(table_name, row(colVal("K", 1)));
            Assert.assertEquals(first_partition.getPartitionId(), part_key_partition.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(table_name);
                delete.setRowKey(row(colVal("K", values[j][0]), colVal("Q", values[j][1]), colVal("T", values[j][2])));
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
    public void testOneLevelSubStrKeyPartition() throws Exception {
        String table_name = "testSubStrKey";
//        BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
        client.addRowKeyElement(table_name, new String[]{ "K", "Q", "T" });
        Object values[][] = { { "K_val1", "Q_val1", 1L, "V_val1" }, { "K_val2", "Q_val2", 101L, "V_val2" },
                { "K_val3", "Q_val3", 501L, "V_val3" }, { "K_val4", "Q_val4", 1001L, "V_val4" },
                { "K_val5", "Q_val5", 5001L, "V_val5" }, { "K_val6", "Q_val6", 10001L, "V_val6" }, };
        int rowCnt = values.length;

        try {
            // batch insert
//            for (int i = 0; i < rowCnt; i++) {
//                Object[] curRow = values[i];
//                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
//                insertOrUpdate.setRowKey(row(colVal("c1", curRow[0]), colVal("c2", curRow[1])));
//                insertOrUpdate.addMutateRow(row(colVal("c3", curRow[2]), colVal("c4", curRow[3])));
//                batchOperation.addOperation(insertOrUpdate);
//            }
//            BatchOperationResult batchOperationResult = batchOperation.execute();
//            MutationResult res = client.insert(table_name)
//                    .setRowKey(row(colVal("K", "K_val1"), colVal("Q", "Q_val1".getBytes()), colVal("T", 1L)))
//                    .addMutateRow(row(colVal("V", "V_val1".getBytes()))).execute();
//            Assert.assertEquals(1, res.getAffectedRows());
            // test get all partitions
            List<Partition> partitions = client.getPartition(table_name);
            Assert.assertEquals(15, partitions.size());


//            Map<Long, Partition> partIdMap = new HashMap<>();
//            for (Partition partition : partitions) {
//                partIdMap.put(partition.getPartitionId(), partition);
//            }
//
//            // test get the first partition
//            Partition first_partition = client.getPartition(TABLE_NAME, row(colVal("c1", 1L), colVal("c2", "c2_val")));
//            Assert.assertEquals(partitions.get(0).getPartitionId(), first_partition.getPartitionId());
//            // test get the second partition
//            Partition sec_partition = client.getPartition(TABLE_NAME, row(colVal("c1", 401L), colVal("c2", "c2_val")));
//            Assert.assertEquals(partitions.get(1).getPartitionId(), sec_partition.getPartitionId());
//            // test get the partition with only partition key
//            Partition partition1 = client.getPartition(TABLE_NAME, row(colVal("c1", 1L)));
//            Assert.assertEquals(first_partition.getPartitionId(), partition1.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
//            for (int j = 0; j < rowCnt; j++) {
//                Delete delete = client.delete(TABLE_NAME);
//                delete.setRowKey(row(colVal("K", values[j][0]), colVal("Q", values[j][1]), colVal("T", values[j][2])));
//                MutationResult res = delete.execute();
//                Assert.assertEquals(1, res.getAffectedRows());
//            }
        }
    }

    /*
    * CREATE TABLE IF NOT EXISTS `testRange` (
                `c1` int NOT NULL,
                `c2` varchar(20) NOT NULL,
                `c3` varbinary(1024) DEFAULT NULL,
                `c4` bigint DEFAULT NULL,
                PRIMARY KEY(`c1`, `c2`)) partition by range columns (`c1`, `c2`) (
                      PARTITION p0 VALUES LESS THAN (300, 't'),
                      PARTITION p1 VALUES LESS THAN (1000, 'T'),
                      PARTITION p2 VALUES LESS THAN (MAXVALUE, MAXVALUE));
    * */
    @Test
    public void testOneLevelRangePartition() throws Exception {
        String table_name = "testRange";
        BatchOperation batchOperation = client.batchOperation(table_name);
        Object values[][] = { { 1, "c2_val1", "c3_val1", 1L }, { 101, "c2_val1", "c3_val1", 101L },
                { 501, "c2_val1", "c3_val1", 501L }, { 901, "c2_val1", "c3_val1", 901L },
                { 1001, "c2_val1", "c3_val1", 1001L }, { 1501, "c2_val1", "c3_val1", 1501L }, };
        int rowCnt = values.length;

        try {
            // test batch insert in ODP mode
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("c1", curRow[0]), colVal("c2", curRow[1])));
                insertOrUpdate.addMutateRow(row(colVal("c3", curRow[2]), colVal("c4", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            // test get all partitions
            List<Partition> partitions = client.getPartition(table_name);
            Assert.assertEquals(3, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }

            // test get the first partition
            Partition first_partition = client.getPartition(table_name, row(colVal("c1", 1L), colVal("c2", "c2_val")));
            Assert.assertEquals(partitions.get(0).getPartitionId(), first_partition.getPartitionId());
            // test get the second partition
            Partition sec_partition = client.getPartition(table_name, row(colVal("c1", 401L), colVal("c2", "c2_val")));
            Assert.assertEquals(partitions.get(1).getPartitionId(), sec_partition.getPartitionId());
            // test get the same partition with the first partition key
            Partition partition1 = client.getPartition(table_name, row(colVal("c1", 101L), colVal("c2", "c2_val")));
            Assert.assertEquals(first_partition.getPartitionId(), partition1.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(table_name);
                delete.setRowKey(row(colVal("c1", values[j][0]), colVal("c2", values[j][1])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
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
    public void testTwoLevelKeyPartition() throws Exception {
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
                Assert.assertNotNull(partition);
                System.out.println(partition.toString());
                // test scan range with partition
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
   * CREATE TABLE IF NOT EXISTS `testPartitionRangeComplex` (
       `c1` int NOT NULL,
       `c2` bigint NOT NULL,
       `c3` varbinary(1024) NOT NULL,
       `c4` varchar(1024) NOT NULL,
       `c5` varchar(20) default NULL,
   PRIMARY KEY (`c1`, `c2`, `c3`, `c4`)
   ) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
   partition by range(`c1`) subpartition by range columns (`c2`, `c3`, `c4`) (
   PARTITION p0 VALUES LESS THAN (500)
   (
       SUBPARTITION p0sp0 VALUES LESS THAN (500, 't', 't'),
       SUBPARTITION p0sp1 VALUES LESS THAN (1000, 'T', 'T'),
       SUBPARTITION p0sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)
   ),
   PARTITION p1 VALUES LESS THAN (1000)
   (
       SUBPARTITION p1sp0 VALUES LESS THAN (500, 't', 't'),
       SUBPARTITION p1sp1 VALUES LESS THAN (1000, 'T', 'T'),
       SUBPARTITION p1sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)
   ),
   PARTITION p2 VALUES LESS THAN MAXVALUE
   (
       SUBPARTITION p2sp0 VALUES LESS THAN (500, 't', 't'),
       SUBPARTITION p2sp1 VALUES LESS THAN (1000, 'T', 'T'),
       SUBPARTITION p2sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)
   ));
   * */
    @Test
    public void testTwoLevelRangePartition() throws Exception {
        client.setRunningMode(ObTableClient.RunningMode.NORMAL);
        String testTable = "testPartitionRangeComplex";
        client.addRowKeyElement(testTable, new String[] { "c1", "c2", "c3", "c4" });
        Random rng = new Random();
        try {
            cleanTable(testTable);
            Connection connection = ObTableClientTestUtil.getConnection();
            Statement statement = connection.createStatement();
            for (int i = 0; i < 64; i++) {
                int c1 = abs(rng.nextInt()) % 2000;
                long c2 = abs(rng.nextLong()) % 2000;
                String c3 = generateRandomStringByUUID(10);
                String c4 = generateRandomStringByUUID(5) + c3 + generateRandomStringByUUID(5);

                // use sql to insert data
                statement.execute("insert into " + testTable + "(c1, c2, c3, c4, c5) values (" + c1
                        + "," + c2 + ",'" + c3 + "','" + c4 + "'," + "'value')");
                Partition partition = client.getPartition(testTable, new Object[] { c1, c2, c3, c4 });
                Assert.assertNotNull(partition);
                System.out.println(partition.toString());
                // test scan range with partition
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

    @Test
    public void testConcurrentGetPartition() throws Exception {
        String[] table_names = { "testHash", "testKey", "testRange" };
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Random random = new Random();

        try {
            for (int i = 0; i < 20; ++i) {
                executorService.submit(() -> {
                    try {
                        String table_name = table_names[random.nextInt(table_names.length)];
                        List<Partition> partitions = client.getPartition(table_name);
                        if (table_name.equalsIgnoreCase("testHash")) {
                            Assert.assertEquals(15, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testHash: " + partition.toString());
                            }
                            MutationResult resultSet = client.insertOrUpdate("testHash")
                                    .setRowKey(row(colVal("K", random.nextInt()), colVal("Q", "Q_val1"), colVal("T", System.currentTimeMillis())))
                                    .addMutateRow(row(colVal("V", "V_val1"))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                        } else if (table_name.equalsIgnoreCase("testKey")) {
                            Assert.assertEquals(15, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testKey: " + partition.toString());
                            }
                            byte[] bytes = new byte[]{};
                            random.nextBytes(bytes);
                            MutationResult resultSet = client.insertOrUpdate("testKey")
                                    .setRowKey(row(colVal("K", bytes), colVal("Q", "Q_val1"), colVal("T", System.currentTimeMillis())))
                                    .addMutateRow(row(colVal("V", "V_val1"))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                        } else {
                            Assert.assertEquals(3, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testRange: " + partition.toString());
                            }
                            MutationResult resultSet = client.insertOrUpdate("testRange")
                                    .setRowKey(row(colVal("c1", random.nextInt()), colVal("c2", "c2_val1")))
                                    .addMutateRow(row(colVal("c3", "c3_val1"), colVal("c4", 10L))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            executorService.shutdown();
            try {
                // wait for all tasks done
                if (!executorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                    if (!executorService.awaitTermination(500L, TimeUnit.MILLISECONDS)) {
                        System.err.println("the thread pool did not shut down");
                    }
                }
                cleanTable("testHash");
                cleanTable("testKey");
                cleanTable("testRange");
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    @Test
    public void testReFetchPartitionMeta() throws Exception {
        String table_name = "testRange";
        BatchOperation batchOperation = client.batchOperation(table_name);
        Object values[][] = { { 1, "c2_val1", "c3_val1", 1L }, { 101, "c2_val1", "c3_val1", 101L },
                { 501, "c2_val1", "c3_val1", 501L }, { 901, "c2_val1", "c3_val1", 901L },
                { 1001, "c2_val1", "c3_val1", 1001L }, { 1501, "c2_val1", "c3_val1", 1501L }, };
        int rowCnt = values.length;
        try {
            MutationResult resultSet = client.insertOrUpdate("testRange")
                    .setRowKey(row(colVal("c1", 10), colVal("c2", "c2_val1")))
                    .addMutateRow(row(colVal("c3", "c3_val1"), colVal("c4", 10L))).execute();
            Assert.assertEquals(1, resultSet.getAffectedRows());
            // need to manually breakpoint here to change table schema in database
            resultSet = client.insertOrUpdate("testRange")
                    .setRowKey(row(colVal("c1", 10), colVal("c2", "c2_val1")))
                    .addMutateRow(row(colVal("c3", "c3_val1"), colVal("c4", 10L))).execute();
            Assert.assertEquals(1, resultSet.getAffectedRows());

            // test batch insert in ODP mode
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("c1", curRow[0]), colVal("c2", curRow[1])));
                insertOrUpdate.addMutateRow(row(colVal("c3", curRow[2]), colVal("c4", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            Assert.assertEquals(rowCnt, batchOperationResult.size());
            for (int j = 0; j < rowCnt; j++) {
                Assert.assertEquals(1, batchOperationResult.get(j).getAffectedRows());
            }
            // need to manually breakpoint here to change table schema in database
            batchOperationResult = batchOperation.execute();
            Assert.assertEquals(rowCnt, batchOperationResult.size());
            for (int j = 0; j < rowCnt; j++) {
                Assert.assertEquals(1, batchOperationResult.get(j).getAffectedRows());
            }

            QueryResultSet result = client.query("testRange")
                    .addScanRange(new Object[] { 1, "c2_val1" },
                    new Object[] { 2000, "c2_val1" })
                    .select("c1", "c2", "c3", "c4").execute();
            Assert.assertEquals(rowCnt, result.cacheSize());
            // need to manually breakpoint here to change table schema in database
            result = client.query("testRange")
                    .addScanRange(new Object[] { 1, "c2_val1" },
                            new Object[] { 2000, "c2_val1" })
                    .select("c1", "c2", "c3", "c4").execute();
            Assert.assertEquals(1, result.cacheSize());


        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            cleanTable(table_name);
        }
    }

}

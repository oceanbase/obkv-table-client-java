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
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableValueFilter;
import com.alipay.oceanbase.rpc.location.model.partition.Partition;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Delete;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.ConcurrentTask;
import com.alipay.oceanbase.rpc.table.ConcurrentTaskExecutor;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.compareVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.cleanTable;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.generateRandomStringByUUID;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;
import static java.lang.StrictMath.abs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ODPGetPartitionMetaTest {
    public ObTableClient client;
    private static final String TABLE_NAME = "test_tinyint_table";
    private static final String TABLE_NAME1 = "testKey";
    private static final String TABLE_NAME2 = "testHash";
    private static final String TABLE_NAME3 = "testRange";
    private static final String TABLE_NAME4 = "testPartitionKeyComplex";
    private static final String TABLE_NAME5 = "testPartitionRangeComplex";
    private static final String TABLE_NAME6 = "test_auto_increment_rowkey";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("CREATE TABLE IF NOT EXISTS `test_tinyint_table` (\n" +
                "        `c1` varchar(20) NOT NULL,\n" +
                "        `c2` tinyint(4) DEFAULT NULL,\n" +
                "        PRIMARY KEY (`c1`)\n" +
                "      );");
        statement.execute("CREATE TABLE IF NOT EXISTS `testKey` (\n" +
                "        `K` varbinary(1024),\n" +
                "        `Q` varbinary(256),\n" +
                "        `T` bigint,\n" +
                "        `V` varbinary(1024),\n" +
                "        PRIMARY KEY(`K`, `Q`, `T`)\n" +
                "    ) partition by key(K) partitions 15;");
        statement.execute("CREATE TABLE IF NOT EXISTS `testHash`(\n" +
                "        `K` bigint,\n" +
                "        `Q` varbinary(256),\n" +
                "        `T` bigint,\n" +
                "        `V` varbinary(1024),\n" +
                "        INDEX i1(`K`, `V`) local,\n" +
                "        PRIMARY KEY(`K`, `Q`, `T`)\n" +
                "    ) partition by hash(`K`) partitions 16;");
        statement.execute("CREATE TABLE IF NOT EXISTS `testRange` (\n" +
                "                `c1` int NOT NULL,\n" +
                "                `c2` varchar(20) NOT NULL,\n" +
                "                `c3` varbinary(1024) DEFAULT NULL,\n" +
                "                `c4` bigint DEFAULT NULL,\n" +
                "                PRIMARY KEY(`c1`, `c2`)) partition by range columns (`c1`, `c2`) (\n" +
                "                      PARTITION p0 VALUES LESS THAN (300, 't'),\n" +
                "                      PARTITION p1 VALUES LESS THAN (1000, 'T'),\n" +
                "                      PARTITION p2 VALUES LESS THAN (MAXVALUE, MAXVALUE));");
        statement.execute("CREATE TABLE IF NOT EXISTS `testPartitionKeyComplex` (\n" +
                "        `c0` tinyint NOT NULL,\n" +
                "        `c1` int NOT NULL,\n" +
                "        `c2` bigint NOT NULL,\n" +
                "        `c3` varbinary(1024) NOT NULL,\n" +
                "        `c4` varchar(1024) NOT NULL,\n" +
                "        `c5` varchar(1024) NOT NULL,\n" +
                "        `c6` varchar(20) default NULL,\n" +
                "    PRIMARY KEY (`c0`, `c1`, `c2`, `c3`, `c4`, `c5`)\n" +
                "    ) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10\n" +
                "    partition by key(`c0`, `c1`, `c2`, `c3`, `c4`) subpartition by key(`c5`) subpartitions 4 partitions 16;");
        statement.execute("CREATE TABLE IF NOT EXISTS `testPartitionRangeComplex` (\n" +
                "       `c1` int NOT NULL,\n" +
                "       `c2` bigint NOT NULL,\n" +
                "       `c3` varbinary(1024) NOT NULL,\n" +
                "       `c4` varchar(1024) NOT NULL,\n" +
                "       `c5` varchar(20) default NULL,\n" +
                "    PRIMARY KEY (`c1`, `c2`, `c3`, `c4`)\n" +
                "    ) DEFAULT CHARSET = utf8mb4 ROW_FORMAT = DYNAMIC COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10\n" +
                "    partition by range(`c1`) subpartition by range columns (`c2`, `c3`, `c4`) (\n" +
                "    PARTITION p0 VALUES LESS THAN (500)\n" +
                "    (\n" +
                "       SUBPARTITION p0sp0 VALUES LESS THAN (500, 't', 't'),\n" +
                "       SUBPARTITION p0sp1 VALUES LESS THAN (1000, 'T', 'T'),\n" +
                "       SUBPARTITION p0sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)\n" +
                "    ),\n" +
                "    PARTITION p1 VALUES LESS THAN (1000)\n" +
                "    (\n" +
                "       SUBPARTITION p1sp0 VALUES LESS THAN (500, 't', 't'),\n" +
                "       SUBPARTITION p1sp1 VALUES LESS THAN (1000, 'T', 'T'),\n" +
                "       SUBPARTITION p1sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)\n" +
                "    ),\n" +
                "    PARTITION p2 VALUES LESS THAN MAXVALUE\n" +
                "    (\n" +
                "       SUBPARTITION p2sp0 VALUES LESS THAN (500, 't', 't'),\n" +
                "       SUBPARTITION p2sp1 VALUES LESS THAN (1000, 'T', 'T'),\n" +
                "       SUBPARTITION p2sp2 VALUES LESS THAN (MAXVALUE, MAXVALUE, MAXVALUE)\n" +
                "    ));");
        statement.execute("CREATE TABLE IF NOT EXISTS `test_auto_increment_rowkey` ("
                        + "`c1` int auto_increment,"
                        + "`c2` int NOT NULL,"
                        + "`c3` int DEFAULT NULL,"
                        + "`c4` varchar(255) DEFAULT NULL,"
                        + "PRIMARY KEY(`c1`, `c2`)) partition by range columns(`c2`)"
                        + "(PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (1000));");
        cleanTable(TABLE_NAME);
        cleanTable(TABLE_NAME1);
        cleanTable(TABLE_NAME2);
        cleanTable(TABLE_NAME3);
        cleanTable(TABLE_NAME4);
        cleanTable(TABLE_NAME5);
        cleanTable(TABLE_NAME6);
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
        BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
        client.setRunningMode(ObTableClient.RunningMode.NORMAL);
        Object values[][] = { { "c1_1", (byte) 1 }, { "c1_2", (byte) 11 }, { "c1_3", (byte) 51 },
                { "c1_4", (byte) 101 } };
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
            List<Partition> partitions = client.getPartition(TABLE_NAME, false);
            Assert.assertEquals(1, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }
            Long lastPartId = -1L;
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                Partition partition = client.getPartition(TABLE_NAME, row(colVal("c1", curRow[0])), false);
                if (lastPartId == -1L) {
                    lastPartId = partition.getPartitionId();
                } else {
                    Assert.assertEquals(lastPartId, partition.getPartitionId());
                }
            }
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME);
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
        BatchOperation batchOperation = client.batchOperation(TABLE_NAME1);
        Object values[][] = { { "K_val1", "Q_val1", 1L, "V_val1" },
                { "K_val2", "Q_val2", 101L, "V_val2" }, { "K_val3", "Q_val3", 501L, "V_val3" },
                { "K_val4", "Q_val4", 1001L, "V_val4" }, { "K_val5", "Q_val5", 5001L, "V_val5" },
                { "K_val6", "Q_val6", 10001L, "V_val6" }, };
        int rowCnt = values.length;

        try {
            // test batch insert
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("K", curRow[0]), colVal("Q", curRow[1]),
                    colVal("T", curRow[2])));
                insertOrUpdate.addMutateRow(row(colVal("V", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            // test get all partitions
            List<Partition> partitions = client.getPartition(TABLE_NAME1, false);
            Assert.assertEquals(15, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }

            // test get the partition with only partition key with only partition key
            Partition first_partition = client.getPartition(TABLE_NAME1,
                row(colVal("K", "K_val1"), colVal("Q", "Q_val1"), colVal("T", 1L)), false);
            Partition part_key_partition = client.getPartition(TABLE_NAME1,
                row(colVal("K", "K_val1")), false);
            Assert.assertEquals(first_partition.getPartitionId(),
                part_key_partition.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME1);
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
    ) partition by hash(`K`) partitions 16;
    * */
    @Test
    public void testOneLevelHashPartition() throws Exception {
        BatchOperation batchOperation = client.batchOperation(TABLE_NAME2);
        Object values[][] = { { 1L, "Q_val1", 1L, "V_val1" }, { 10L, "Q_val2", 101L, "V_val2" },
                { 501L, "Q_val3", 501L, "V_val3" }, { 1001L, "Q_val4", 1001L, "V_val4" },
                { 5001L, "Q_val5", 5001L, "V_val5" }, { 10001L, "Q_val6", 10001L, "V_val6" }, };
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
            // test get all partitions
            List<Partition> partitions = client.getPartition(TABLE_NAME2, false);
            Assert.assertEquals(16, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }

            // test get the partition with only partition key with only partition key
            Partition first_partition = client.getPartition(TABLE_NAME2,
                row(colVal("K", 1), colVal("Q", "Q_val1"), colVal("T", 1L)), false);
            Partition part_key_partition = client.getPartition(TABLE_NAME2, row(colVal("K", 1)), false);
            Assert.assertEquals(first_partition.getPartitionId(),
                part_key_partition.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME2);
                delete.setRowKey(row(colVal("K", values[j][0]), colVal("Q", values[j][1]),
                    colVal("T", values[j][2])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    /*
    * CREATE TABLE IF NOT EXISTS `testRange` (
            `K` varbinary(1024),
            `Q` varbinary(256),
            `T` bigint,
            `V` varbinary(10240),
            INDEX i1(`K`, `V`) local,
            PRIMARY KEY(`K`, `Q`, `T`)
        ) partition by range columns (`K`) (
            PARTITION p0 VALUES LESS THAN ('a'),
            PARTITION p1 VALUES LESS THAN ('w'),
            PARTITION p2 VALUES LESS THAN MAXVALUE
        );
    * */
    @Test
    public void testOneLevelRangePartition() throws Exception {
        BatchOperation batchOperation = client.batchOperation(TABLE_NAME3);
        Object values[][] = { { "ah", "c2_val1", 1L, "c3_val1" }, { "bw", "c2_val1", 101L, "c3_val1" },
                { "ht", "c2_val1", 501L, "c3_val1" }, { "tw", "c2_val1", 901L, "c3_val1" },
                { "xy", "c2_val1", 1001L, "c3_val1" }, { "zw", "c2_val1", 1501L, "c3_val1" } };
        int rowCnt = values.length;

        try {
            // test batch insert in ODP mode
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                insertOrUpdate.setRowKey(row(colVal("K", curRow[0]), colVal("Q", curRow[1]), colVal("T", curRow[2])));
                insertOrUpdate.addMutateRow(row(colVal("V", curRow[3])));
                batchOperation.addOperation(insertOrUpdate);
            }
            BatchOperationResult batchOperationResult = batchOperation.execute();
            // test get all partitions
            List<Partition> partitions = client.getPartition(TABLE_NAME3, false);
            Assert.assertEquals(3, partitions.size());
            for (Partition partition : partitions) {
                System.out.println(partition.toString());
            }

            // test get the first partition using nonexistent row
            Partition first_partition = client.getPartition(TABLE_NAME3,
                row(colVal("K", "A"), colVal("Q", "bw"), colVal("T", 1L)), false);
            Assert.assertEquals(partitions.get(0).getPartitionId(),
                first_partition.getPartitionId());
            // test get the second partition
            Partition sec_partition = client.getPartition(TABLE_NAME3,
                    row(colVal("K", "ah"), colVal("Q", "bw"), colVal("T", 1L)), false);
            Assert.assertEquals(partitions.get(1).getPartitionId(), sec_partition.getPartitionId());
            // test get the same partition with the first partition key
            Partition partition1 = client.getPartition(TABLE_NAME3,
                    row(colVal("K", "B")), false);
            Assert.assertEquals(first_partition.getPartitionId(), partition1.getPartitionId());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME3);
                delete.setRowKey(row(colVal("K", values[j][0]), colVal("Q", values[j][1]), colVal("T", values[j][2])));
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
        try {
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
                statement.execute("insert into " + TABLE_NAME4
                                  + "(c0, c1, c2, c3, c4, c5, c6) values (" + c0 + "," + c1 + ","
                                  + c2 + ",'" + c3 + "','" + c4 + "','" + c5 + "'," + "'value')");
                Partition partition = client.getPartition(
                        TABLE_NAME4,
                    row(colVal("c0", c0), colVal("c1", c1), colVal("c2", c2), colVal("c3", c3),
                        colVal("c4", c4), colVal("c5", c5)), false);
                Assert.assertNotNull(partition);
                System.out.println(partition.toString());
                // test scan range with partition
                QueryResultSet result = client.query(TABLE_NAME4)
                    .addScanRange(partition.start(), partition.end()).execute();
                Assert.assertTrue(result.cacheSize() >= 1);
            }

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            cleanTable(TABLE_NAME4);
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
        Random rng = new Random();
        try {
            Connection connection = ObTableClientTestUtil.getConnection();
            Statement statement = connection.createStatement();
            for (int i = 0; i < 64; i++) {
                int c1 = abs(rng.nextInt()) % 2000;
                long c2 = abs(rng.nextLong()) % 2000;
                String c3 = generateRandomStringByUUID(10);
                String c4 = generateRandomStringByUUID(5) + c3 + generateRandomStringByUUID(5);

                // use sql to insert data
                statement.execute("insert into " + TABLE_NAME5 + "(c1, c2, c3, c4, c5) values (" + c1
                                  + "," + c2 + ",'" + c3 + "','" + c4 + "'," + "'value')");
                Partition partition = client.getPartition(TABLE_NAME5,
                    row(colVal("c1", c1), colVal("c2", c2), colVal("c3", c3), colVal("c4", c4)), false);
                Assert.assertNotNull(partition);
                System.out.println(partition.toString());
                // test scan range with partition
                QueryResultSet result = client.query(TABLE_NAME5)
                    .addScanRange(partition.start(), partition.end()).execute();
                Assert.assertTrue(result.cacheSize() >= 1);
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            cleanTable(TABLE_NAME5);
        }
    }

    @Test
    public void testConcurrentGetPartition() throws Exception {
        String[] table_names = { TABLE_NAME2, TABLE_NAME1, TABLE_NAME3 };
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        Random random = new Random();
        AtomicInteger cnt = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(20);

        try {
            for (int i = 0; i < 20; ++i) {
                executorService.submit(() -> {
                    try {
                        String table_name = table_names[random.nextInt(table_names.length)];
                        if (table_name.equalsIgnoreCase(TABLE_NAME2)) {
                            MutationResult resultSet = client.insert(TABLE_NAME2)
                                    .setRowKey(row(colVal("K", random.nextLong()), colVal("Q", "Q_val1"), colVal("T", System.currentTimeMillis())))
                                    .addMutateRow(row(colVal("V", "V_val1"))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                            List<Partition> partitions = client.getPartition(table_name, false);
                            Assert.assertEquals(16, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testHash: " + partition.toString());
                            }
                            cnt.getAndIncrement();
                        } else if (table_name.equalsIgnoreCase(TABLE_NAME1)) {
                            byte[] bytes = new byte[10];
                            random.nextBytes(bytes);
                            MutationResult resultSet = client.insert(TABLE_NAME1)
                                    .setRowKey(row(colVal("K", bytes), colVal("Q", "Q_val1"), colVal("T", System.currentTimeMillis())))
                                    .addMutateRow(row(colVal("V", "V_val1"))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                            List<Partition> partitions = client.getPartition(table_name, false);
                            Assert.assertEquals(15, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testKey: " + partition.toString());
                            }
                            cnt.getAndIncrement();
                        } else {
                            byte[] bytes = new byte[10];
                            random.nextBytes(bytes);
                            MutationResult resultSet = client.insert(TABLE_NAME3)
                                    .setRowKey(row(colVal("K", bytes), colVal("Q", "c2_val1"), colVal("T", random.nextLong())))
                                    .addMutateRow(row(colVal("V", "c3_val1"))).execute();
                            Assert.assertEquals(1, resultSet.getAffectedRows());
                            List<Partition> partitions = client.getPartition(table_name, false);
                            Assert.assertEquals(3, partitions.size());
                            for (Partition partition : partitions) {
                                System.out.println("testRange: " + partition.toString());
                            }
                            cnt.getAndIncrement();
                        }
                    } catch (Throwable t) {
                        t.printStackTrace();
                        throw new RuntimeException(t);
                    } finally {
                        latch.countDown();
                    }
                });
            }
            latch.await();
            Assert.assertEquals(20, cnt.get());
        } catch (Throwable t) {
            t.printStackTrace();
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
                cleanTable(TABLE_NAME2);
                cleanTable(TABLE_NAME1);
                cleanTable(TABLE_NAME3);
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
                Assert.assertTrue(false);
            }
        }
    }

    @Ignore
    public void testReFetchPartitionMeta() throws Exception {
        String table_name = TABLE_NAME3;
        BatchOperation batchOperation = client.batchOperation(table_name);
        Object values[][] = { { "ah", "c2_val1", 1L, "c3_val1" }, { "bw", "c2_val1", 101L, "c3_val1" },
                { "ht", "c2_val1", 501L, "c3_val1" }, { "tw", "c2_val1", 901L, "c3_val1" },
                { "xy", "c2_val1", 1001L, "c3_val1" }, { "zw", "c2_val1", 1501L, "c3_val1" } };
        int rowCnt = values.length;
        try {
            MutationResult resultSet = client.insertOrUpdate(TABLE_NAME3)
                .setRowKey(row(colVal("K", "ah"), colVal("Q", "c2_val1"), colVal("T", 1L)))
                .addMutateRow(row(colVal("T", "c3_val1"))).execute();
            Assert.assertEquals(1, resultSet.getAffectedRows());
            // need to manually breakpoint here to change table schema in database
            resultSet = client.insertOrUpdate(TABLE_NAME3)
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

            QueryResultSet result = client.query(TABLE_NAME3)
                .addScanRange(new Object[] { 1, "c2_val1" }, new Object[] { 2000, "c2_val1" })
                .select("c1", "c2", "c3", "c4").execute();
            Assert.assertEquals(rowCnt, result.cacheSize());
            // need to manually breakpoint here to change table schema in database
            result = client.query(TABLE_NAME3)
                .addScanRange(new Object[] { 1, "c2_val1" }, new Object[] { 2000, "c2_val1" })
                .select("c1", "c2", "c3", "c4").execute();
            Assert.assertEquals(1, result.cacheSize());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            cleanTable(table_name);
        }
    }

    @Test
    public void testODPRangeQuery() throws Exception {
        // todo: only support in 4.x currently
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4000)) {
            return;
        }

        client.addRowKeyElement(TABLE_NAME6, new String[] { "c1", "c2" });

        try {
            client.insert(TABLE_NAME6, new Object[] { 0, 1 }, new String[] { "c3" },
                new Object[] { 1 });

            TableQuery tableQuery = client.query(TABLE_NAME6);
            tableQuery.select("c1", "c2", "c3");
            tableQuery.addScanRange(new Object[] { 1, 1 }, new Object[] { 200, 90 });
            ObTableValueFilter filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 1);
            tableQuery.setFilter(filter);
            QueryResultSet result = tableQuery.execute();
            Assert.assertTrue(result.next());
            Map<String, Object> value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(1, value.get("c3"));

            // test insert use user value
            client.insert(TABLE_NAME6, new Object[] { 100, 1 }, new String[] { "c3" },
                new Object[] { 1 });

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 100);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(1, value.get("c3"));

            // test insert sync global auto inc val
            client.insert(TABLE_NAME6, new Object[] { 0, 1 }, new String[] { "c3" },
                new Object[] { 1 });

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 101);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(1, value.get("c3"));

            // test delete
            client.delete(TABLE_NAME6, new Object[] { 101, 1 });

            // test confirm delete
            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 101);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertFalse(result.next());

            // test update
            ObTableValueFilter filter_3 = compareVal(ObCompareOp.EQ, "c3", 1);

            MutationResult updateResult = client.update(TABLE_NAME6)
                .setRowKey(colVal("c1", 1), colVal("c2", 1)).setFilter(filter_3)
                .addMutateRow(row(colVal("c3", 5))).execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 1);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(5, value.get("c3"));

            // test replace not exist, insert
            MutationResult theResult = client.replace(TABLE_NAME6)
                .setRowKey(colVal("c1", 0), colVal("c2", 1)).addMutateRow(row(colVal("c3", 2)))
                .execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 102);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(2, value.get("c3"));

            // test replace exist, replace
            theResult = client.replace(TABLE_NAME6).setRowKey(colVal("c1", 101), colVal("c2", 1))
                .addMutateRow(row(colVal("c3", 20))).execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 101);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(20, value.get("c3"));

            // test insertup not exist, insert
            theResult = client.insertOrUpdate(TABLE_NAME6)
                .setRowKey(colVal("c1", 0), colVal("c2", 1)).addMutateRow(row(colVal("c3", 5)))
                .execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 103);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(5, value.get("c3"));

            // test insertup exist, update
            theResult = client.insertOrUpdate(TABLE_NAME6)
                .setRowKey(colVal("c1", 103), colVal("c2", 1)).addMutateRow(row(colVal("c3", 50)))
                .execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 103);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(50, value.get("c3"));

            // test insertup exist, update again
            theResult = client.insertOrUpdate(TABLE_NAME6)
                .setRowKey(colVal("c1", 103), colVal("c2", 1)).addMutateRow(row(colVal("c3", 50)))
                .execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 103);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(50, value.get("c3"));

            // test increment not exist, insert
            value = client.increment(TABLE_NAME6, new Object[] { 0, 1 }, new String[] { "c3" },
                new Object[] { 6 }, true);

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 104);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(6, value.get("c3"));

            // test increment exist, increment
            value = client.increment(TABLE_NAME6, new Object[] { 104, 1 }, new String[] { "c3" },
                new Object[] { 6 }, true);

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 104);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals(12, value.get("c3"));

            // test illegal increment on auto increment column
            try {
                value = client.increment(TABLE_NAME6, new Object[] { 104, 1 },
                    new String[] { "c1" }, new Object[] { 1 }, true);
            } catch (ObTableException e) {
                assertNotNull(e);
                assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode, e.getErrorCode());
            }

            // test append not exist, insert
            Map<String, Object> res = client.append(TABLE_NAME6, new Object[] { 0, 1 },
                new String[] { "c4" }, new Object[] { "a" }, true);

            tableQuery.select("c1", "c2", "c3", "c4");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 105);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals("a", value.get("c4"));

            // test append exist, append
            res = client.append(TABLE_NAME6, new Object[] { 105, 1 }, new String[] { "c4" },
                new Object[] { "b" }, true);

            tableQuery.select("c1", "c2", "c3", "c4");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 105);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals("ab", value.get("c4"));

            //  the total number of data
            tableQuery.select("c1", "c2", "c3", "c4");
            filter = new ObTableValueFilter(ObCompareOp.LT, "c1", 300);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertEquals(7, result.cacheSize());
        } finally { // drop table
            Connection connection = ObTableClientTestUtil.getConnection();
            Statement statement = connection.createStatement();
            statement.execute("drop table " + TABLE_NAME6);
        }
    }

}

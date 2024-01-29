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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.Put;
import com.alipay.oceanbase.rpc.mutation.Update;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.*;

/**
 CREATE TABLE IF NOT EXISTS `group_put` (
 `pk` varchar(20) NOT NULL,
 `c1` bigint DEFAULT NULL,
 `c2` varchar(32) DEFAULT NULL,
 PRIMARY KEY(`pk`)) PARTITION BY KEY(`pk`) PARTITIONS 32;
 **/
public class ObTableGroupCommitTest {
    static ObTableClient    client;
    public static String    tableName = "group_put";
    public static String    pkColumnName = "pk";
    public static String    c1ColumnName = "c1";
    public static String    c2ColumnName = "c2";
    public static String    notExistColumnName = "unknown";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { pkColumnName });
    }

    private static void enableGroupCommit() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter system set enable_kv_group_commit=true");
    }

    private static void disableGroupCommit() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter system set enable_kv_group_commit=false");
    }

    private static boolean isGroupCommitEnable() throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select value from oceanbase.GV$OB_PARAMETERS where name='enable_kv_group_commit'");
        if (resultSet.next()) {
            return resultSet.getBoolean(1);
        } else {
            return false;
        }
    }

    private static void setGroupCommitSize(long size) throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter system set kv_group_commit_size=" + size);
    }

    private static void setGroupCommitAutoCloseThreshold(long size) throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter system set kv_group_auto_close_threshold=" + size);
    }

    private static void deleteTable(String tableName) throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("delete from " + tableName);
    }

    private static int countTable(String tableName) throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from " + tableName);
        if (resultSet.next()) {
            return resultSet.getInt(1);
        } else {
            return 0;
        }
    }
    
    private void PrepareRecords(int recordNum) throws Exception {
        for (long i = 0; i < recordNum; i++) {
            Put put = client.put(tableName);
            MutationResult res = put.setRowKey(colVal(pkColumnName, "pk" + i))
                    .addMutateColVal(colVal(c1ColumnName, i))
                    .addMutateColVal(colVal(c2ColumnName, "v" + i))
                    .execute();
            if (res.getAffectedRows() != 1) {
                throw new RuntimeException("unexpected affected rows");
            }
        }
    }

    static class PutTask implements Runnable {
        public PutTask(long i) {
            super();
            index=i;
        }
        public long index;
        @Override
        public void run() {
            try {
                Put put = client.put(tableName);
                MutationResult res = put.setRowKey(colVal(pkColumnName, "pk" + index))
                        .addMutateColVal(colVal(c1ColumnName, index))
                        .addMutateColVal(colVal(c2ColumnName, "v" + index))
                        .execute();
                if (res.getAffectedRows() != 1) {
                    throw new RuntimeException("unexpected affected rows");
                }
                System.out.println("put execute finish:" + index);
            } catch (Exception e) {
                System.out.println("execute fail: " + index);
                e.printStackTrace();
            }
        }
    }

    static class FailPutTask implements Runnable {
        public FailPutTask(long i) {
            super();
            index=i;
        }
        public long index;
        @Override
        public void run() {
            ObTableException thrown = assertThrows(
                    ObTableException.class,
                    () -> {
                        Put put = client.put(tableName);
                        MutationResult res = put.setRowKey(colVal(pkColumnName, "pk" + index))
                                .addMutateColVal(colVal(notExistColumnName, index))
                                .execute();
                    }
            );

            System.out.println(thrown.getMessage());
            assertTrue(thrown.getMessage().contains("[-5217][OB_ERR_BAD_FIELD_ERROR]"));
        }
    }

    static class GetTask implements Runnable {
        public GetTask(long i) {
            super();
            index=i;
        }
        public long index;
        @Override
        public void run() {
            try {
                Map<String, Object> res = client.get(tableName, "pk" + index, new String[] {pkColumnName});
                System.out.println("get execute finish: " + index);
                System.out.println("pk" + index + " result: " + res.get(pkColumnName));
                if (!("pk" + index).equals(res.get(pkColumnName))) {
                    throw new RuntimeException("unexpected get result");
                }
            } catch (Exception e) {
                System.out.println("execute fail: " + index);
                e.printStackTrace();
            }
        }
    }

    static class UpdateTask implements Runnable {
        public UpdateTask(long i) {
            super();
            index=i;
        }
        public long index;
        @Override
        public void run() {
            try {
                Update update = client.update(tableName);
                MutationResult res = update.setRowKey(colVal(pkColumnName, "pk" + index))
                        .addMutateColVal(colVal(c1ColumnName, index))
                        .addMutateColVal(colVal(c2ColumnName, "v" + index))
                        .execute();
                if (res.getAffectedRows() != 1) {
                    throw new RuntimeException("unexpected affected rows");
                }
                System.out.println("update execute finish:" + index);
            } catch (Exception e) {
                System.out.println("execute fail: " + index);
                e.printStackTrace();
            }
        }
    }

    @Test
    // testGroupPut1 operation number is less than group commit size
    public void testGroupPut1() throws Exception {
        long group_size = 10;
        int op_size = 9;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new PutTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }

            // check result
            Assert.assertEquals(op_size, countTable(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupPut2 operation number is equal to group commit size
    public void testGroupPut2() throws Exception {
        long group_size = 10;
        int op_size = 10;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new PutTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }

            // check result
            Assert.assertEquals(op_size, countTable(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupPut3 operation number is upper than group commit size
    public void testGroupPut3() throws Exception {
        long group_size = 10;
        int op_size = 11;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new PutTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }

            // check result
            Assert.assertEquals(op_size, countTable(tableName));
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupGet1 operation number is less than group commit size
    public void testGroupGet1() throws Exception {
        long group_size = 10;
        int op_size = 9;
        try {
            // prepare records
            PrepareRecords(op_size);
            
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new GetTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupGet2 operation number is equal to group commit size
    public void testGroupGet2() throws Exception {
        long group_size = 10;
        int op_size = 10;
        try {
            // prepare records
            PrepareRecords(op_size);

            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new GetTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupGet3 operation number is upper than group commit size
    public void testGroupGet3() throws Exception {
        long group_size = 10;
        int op_size = 11;
        try {
            // prepare records
            PrepareRecords(op_size);

            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new GetTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupMixed1 operation number is less than group commit size
    // some group op
    // some normal op
    public void testGroupMixed1() throws Exception {
        long group_size = 10;
        int op_size = 9;
        try {
            // prepare records
            PrepareRecords(op_size);

            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                if (i % 2 == 0) {
                    tasks[i] = new GetTask(i);
                } else {
                    tasks[i] = new UpdateTask(i);
                }
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupMixed2 operation number is equal to group commit size
    // some group op
    // some normal op
    public void testGroupMixed2() throws Exception {
        long group_size = 10;
        int op_size = 20;
        try {
            // prepare records
            PrepareRecords(op_size);

            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                if (i % 2 == 0) {
                    tasks[i] = new GetTask(i);
                } else {
                    tasks[i] = new UpdateTask(i);
                }
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupMixed3 operation number is upper to group commit size
    // some group op
    // some normal op
    public void testGroupMixed3() throws Exception {
        long group_size = 10;
        int op_size = 30;
        try {
            // prepare records
            PrepareRecords(op_size);

            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                if (i % 2 == 0) {
                    tasks[i] = new GetTask(i);
                } else {
                    tasks[i] = new UpdateTask(i);
                }
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupMixed4 all op are normal op
    public void testGroupMixed4() throws Exception {
        long group_size = 10;
        int op_size = 10;
        try {
            // prepare records
            PrepareRecords(op_size);

            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new UpdateTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupMixed5 all op are group op
    public void testGroupMixed5() throws Exception {
        long group_size = 10;
        int op_size = 10;
        try {
            // prepare records
            PrepareRecords(op_size);

            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                if (i % 2 == 0) {
                    tasks[i] = new GetTask(i);
                } else {
                    tasks[i] = new PutTask(i);
                }
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupSize1 size is 1
    public void testGroupSize1() throws Exception {
        long group_size = 1;
        int op_size = 100;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new PutTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupSize2 size is 2
    public void testGroupSize2() throws Exception {
        long group_size = 2;
        int op_size = 100;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new PutTask(i);
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupFail1 some op is invalid
    public void testGroupFail1() throws Exception {
        long group_size = 10;
        int op_size = 11;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new PutTask(i);
                if (i == op_size - 1) {
                    tasks[i] = new FailPutTask(i);
                }
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupFail2 many ops are invalid
    public void testGroupFail2() throws Exception {
        long group_size = 10;
        int op_size = 100;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new PutTask(i);
                if (i % 2 == 0) {
                    tasks[i] = new FailPutTask(i);
                }
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupBanning1 auto ban group commit
    public void testGroupBanning1() throws Exception {
        long group_size = 10;
        int op_size = 1;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);
            setGroupCommitAutoCloseThreshold(1);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            tasks[0] = new FailPutTask(0);
            threads[0] = new Thread(tasks[0]);

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }

            assertFalse(isGroupCommitEnable());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }

    @Test
    // testGroupBanning2 continue execute after auto ban group commit
    public void testGroupBanning2() throws Exception {
        long group_size = 10;
        int op_size = 100;
        try {
            // set up group commit
            enableGroupCommit();
            setGroupCommitSize(group_size);
            setGroupCommitAutoCloseThreshold(1);

            // create tasks and threads
            Runnable[] tasks = new Runnable[op_size];
            Thread[] threads = new Thread[op_size];
            for (int i = 0; i < op_size; i++) {
                tasks[i] = new PutTask(i);
                if (i % 2 == 0) {
                    tasks[i] = new FailPutTask(i);
                }
                threads[i] = new Thread(tasks[i]);
            }

            // start thread
            for (int i = 0; i < op_size; i++) {
                threads[i].start();
            }

            // wait thread finish
            for (int i = 0; i < op_size; i++) {
                threads[i].join();
            }
            assertFalse(isGroupCommitEnable());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            deleteTable(tableName);
            disableGroupCommit();
        }
    }
}

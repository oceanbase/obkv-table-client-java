package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import com.google.protobuf.MapEntry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

public class ObTableFullTextIndexTest {
    ObTableClient client;
    String noPartTableName = "tbl1";
    String partTableName = "part_tbl1";
    String ttlTableName = "ttl_tbl1";
    String createNoPartTableSQL = "CREATE TABLE IF NOT EXISTS tbl1(id INT, c2 INT, txt text, PRIMARY KEY (id), FULLTEXT INDEX full_idx1_tbl1(txt))";
    String createPartTableSQL = "CREATE TABLE IF NOT EXISTS part_tbl1(id INT, c2 INT, txt text, PRIMARY KEY (id), FULLTEXT INDEX full_idx1_tbl1(txt)) partition by key(id) partitions 3;";
    String createTTLTableSQL = "CREATE TABLE IF NOT EXISTS ttl_tbl1(id INT, c2 INT, txt text, expired_ts timestamp(6), PRIMARY KEY (id), FULLTEXT INDEX full_idx1_tbl1(txt)) TTL(expired_ts + INTERVAL 10 SECOND) partition by key(id) partitions 3;";
    String truncateNoPartTableSQL = "truncate table tbl1;";
    String truncatePartTableSQL = "truncate table part_tbl1;";
    String truncateTTLTableSQL = "truncate table ttl_tbl1;";
    String idCol = "id";
    String c2Col = "c2";
    String txtCol = "txt";
    String expireTsCol = "expired_ts";
    @Before
    public void setup() throws Exception {
        executeSQL(createNoPartTableSQL);
        executeSQL(createPartTableSQL);
        executeSQL(createTTLTableSQL);
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    @After
    public void teardown() throws Exception {
        executeSQL("drop table " + noPartTableName);
        executeSQL("drop table " + partTableName);
        executeSQL("drop table " + ttlTableName);
    }

    @Test
    public void testInsert() throws Exception {
        try{
            int id = 1;
            MutationResult res = client.insert(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, "OceanBase Database is a native, " +
                            "enterprise-level distributed database developed independently by the OceanBase team")))
                    .execute();
            Assert.assertEquals(1, res.getAffectedRows());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    @Test
    public void testGet() throws Exception {
        try{
            int id = 3;
            String txt = "OceanBase Database is a native, " +
                    "enterprise-level distributed database developed independently by the OceanBase team";
            MutationResult insRes = client.insert(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            Map<String, Object> res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    @Test
    public void testDel() throws Exception {
        try{
            int id = 4;
            String txt = "aaa asdjl asdjlakjsdl hello select new fine";
            MutationResult insRes = client.insert(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());
            // get
            Map<String, Object> res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // del
            MutationResult delRes = client.delete(partTableName).setRowKey(colVal(idCol, id)).execute();
            Assert.assertEquals(1, delRes.getAffectedRows());
            // get
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertTrue(res.isEmpty());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    @Test
    public void testUpd() throws Exception {
        try{
            int id = 5;
            String txt = "aaa asdjl asdjlakjsdl hello select new fine";
            MutationResult insRes = client.insert(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());
            // get
            Map<String, Object> res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // update with fulltext columns
            txt = "The sun was setting on the horizon, casting a warm golden glow over the tranquil ocean. ";
            MutationResult updRes = client.update(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get again
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // update with non-fulltext columns
            updRes = client.update(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 10)))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get again
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(10, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // update all columns
            txt = "As the day came to a close, the peaceful scene served as a reminder of the beauty and serenity that nature has to offer.";
            updRes = client.update(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 100),
                        colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get again
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(100, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    @Test
    public void testInsOrUpd() throws Exception {
        try {
            int id = 6;
            String txt = "The sun rose over the horizon, casting a warm glow across the meadow. ";
            // insertup-insert
            MutationResult insRes = client.insertOrUpdate(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());
            // get
            Map<String, Object> res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // insertup-update with non fulltext column
            MutationResult updRes = client.insertOrUpdate(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 100)))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(100, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // insertup-update with fulltext column
            txt =  "The birds chirped happily as they flew from tree to tree";
            updRes = client.insertOrUpdate(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(100, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // insertup-update with all column
            txt = " The birds chirped happily as they flew from tree to tree";
            updRes = client.insertOrUpdate(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 106),
                            colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(106, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    @Test
    public void testReplace() throws Exception {
        try {
            int id = 7;
            String txt = "The sun rose over the horizon, casting a warm glow across the meadow. ";
            // replace-insert
            MutationResult insRes = client.replace(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());
            // get
            Map<String, Object> res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // replace-conflict with non fulltext column
            MutationResult updRes = client.replace(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 100)))
                    .execute();
            Assert.assertEquals(2, updRes.getAffectedRows());
            // get
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(100, res.get(c2Col));
            Assert.assertEquals(null, res.get(txtCol));

            // replace-conflict with fulltext column
            txt = " The birds chirped happily as they flew from tree to tree";
            updRes = client.replace(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(2, updRes.getAffectedRows());
            // get
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

            // replace-conflict with all column
            txt = " The birds chirped happily as they flew from tree to tree";
            updRes = client.replace(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 100+id),
                            colVal(txtCol, txt)))
                    .execute();
            Assert.assertEquals(2, updRes.getAffectedRows());
            // get
            res = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(100+id, res.get(c2Col));
            Assert.assertEquals(txt, res.get(txtCol));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    @Test
    public void testTTLInsert() throws Exception {
        try {
            // 写入新行
            int id = 8;
            String txt1 = "The sun rose over the horizon, casting a warm glow across the meadow. ";
            Timestamp curTs = new Timestamp(System.currentTimeMillis());
            Timestamp expireTs = new Timestamp(System.currentTimeMillis() - 1000000);
            MutationResult insRes = client.insert(ttlTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(expireTsCol, expireTs),
                            colVal(txtCol, txt1)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            String txt2 = "The birds chirped happily as they flew from tree to tree";
            insRes = client.insert(ttlTableName).setRowKey(colVal(idCol, id + 1))
                    .addMutateRow(row(colVal(expireTsCol, curTs),
                            colVal(txtCol, txt2)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            // 过期，删除旧行，写入新行
            String txt3 = "Two roads diverged in a wood";
            insRes = client.insert(ttlTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(expireTsCol, curTs),
                            colVal(txtCol, txt3)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            // get
            Map<String, Object> res = client.get(ttlTableName, new Object[]{id}, null);
            Assert.assertEquals(4, res.size());
            Assert.assertEquals(id, res.get(idCol));
            Assert.assertEquals(curTs, res.get(expireTsCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt3, res.get(txtCol));

            // 未过期
            // - insert操作
            try {
                client.insert(ttlTableName).setRowKey(colVal(idCol, id + 1))
                        .addMutateRow(row(colVal(expireTsCol, curTs),
                                colVal(txtCol, "I took the one less traveled by")))
                        .execute();
            } catch (ObTableException e) {
                Assert.assertEquals(ResultCodes.OB_ERR_PRIMARY_KEY_DUPLICATE.errorCode, e.getErrorCode());
            }
            // - insertup操作
            String txt4 = "I took the one less traveled by";
            insRes = client.insertOrUpdate(ttlTableName).setRowKey(colVal(idCol, id + 1))
                    .addMutateRow(row(colVal(expireTsCol, curTs),
                            colVal(txtCol, txt4)))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            // get
            res = client.get(ttlTableName, new Object[]{id + 1}, null);
            Assert.assertEquals(id + 1, res.get(idCol));
            Assert.assertEquals(curTs, res.get(expireTsCol));
            Assert.assertEquals(null, res.get(c2Col));
            Assert.assertEquals(txt4, res.get(txtCol));
        } catch(Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncateTTLTableSQL);
        }
    }

    @Test
    public void testIncrment() throws Exception {
        try {
            // increment row not exist
            int id = 9;

            MutationResult res = client.increment(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 1)))
                    .execute();
            Assert.assertEquals(1, res.getAffectedRows());

            Map<String, Object> getRes = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, getRes.size());
            Assert.assertEquals(id, getRes.get(idCol));
            Assert.assertEquals(1, getRes.get(c2Col));
            Assert.assertEquals(null, getRes.get(txtCol));

            res = client.increment(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 1)))
                    .execute();
            Assert.assertEquals(1, res.getAffectedRows());

            getRes = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, getRes.size());
            Assert.assertEquals(id, getRes.get(idCol));
            Assert.assertEquals(2, getRes.get(c2Col));
            Assert.assertEquals(null, getRes.get(txtCol));
        } catch(Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    @Test
    public void testAppend() throws Exception {
        try {
            // append row not exist
            int id = 10;
            String txt1 = "We enjoyed a peaceful walk.";
            MutationResult res = client.append(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt1)))
                    .execute();
            Assert.assertEquals(1, res.getAffectedRows());

            Map<String, Object> getRes = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, getRes.size());
            Assert.assertEquals(id, getRes.get(idCol));
            Assert.assertEquals(null, getRes.get(c2Col));
            Assert.assertEquals(txt1, getRes.get(txtCol));

            String txt2 = "Can you pass me the salt, please?";
            res = client.append(partTableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, txt2)))
                    .execute();
            Assert.assertEquals(1, res.getAffectedRows());

            getRes = client.get(partTableName, new Object[] { id }, null);
            Assert.assertEquals(3, getRes.size());
            Assert.assertEquals(id, getRes.get(idCol));
            Assert.assertEquals(null, getRes.get(c2Col));
            Assert.assertEquals(txt1+txt2, getRes.get(txtCol));
        } catch(Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    private void loadData(String tableName) throws Exception {
        // load data
        client.insert(tableName).setRowKey(colVal(idCol, 1))
                .addMutateRow(row(colVal(c2Col, 1), colVal(txtCol, "hello world")))
                .execute();
        client.insert(tableName).setRowKey(colVal(idCol, 2))
                .addMutateRow(row(colVal(c2Col, 2), colVal(txtCol, "OceanBase Database is a native, enterprise-level distributed database developed independently by the OceanBase team")))
                .execute();
        client.insert(tableName).setRowKey(colVal(idCol, 3))
                .addMutateRow(row(colVal(c2Col, 3), colVal(txtCol, "Learn about SQL and database administration in oceanBase")))
                .execute();
        client.insert(tableName).setRowKey(colVal(idCol, 4))
                .addMutateRow(row(colVal(c2Col, 4), colVal(txtCol, "Master the art of full text searching")))
                .execute();
    }

    @Test
    public void testFTSQuery() throws Exception {
        try {
            executeSQL(truncatePartTableSQL);
            client.addRowKeyElement(partTableName, new String[] {"id"});
            //load data
            loadData(partTableName);
            //sync query
            QueryResultSet resultSet = client.query(partTableName)
                    .setSearchText("oceanbase")
                    .indexName("full_idx1_tbl1")
                    .execute();
            int count = 0;
            while(resultSet.next()) {
                count++;
                Map<String, Object> row = resultSet.getRow();
                Assert.assertEquals(3, row.size());
                int id = (int) row.get("id");
                Assert.assertEquals(id, row.get("c2"));
                Assert.assertTrue(((String)row.get("txt")).toLowerCase(Locale.ROOT).contains("oceanbase"));
            }
            Assert.assertTrue(2 == count);

            // async query
            QueryResultSet asyncResultSet = client.query(partTableName)
                    .indexName("full_idx1_tbl1")
                    .setSearchText("oceanbase")
                    .asyncExecute();
            count = 0;
            while(asyncResultSet.next()) {
                count++;
                Map<String, Object> row = asyncResultSet.getRow();
                Assert.assertEquals(3, row.size());
                int id = (int) row.get("id");
                Assert.assertEquals(id, row.get("c2"));
                Assert.assertTrue(((String)row.get("txt")).toLowerCase(Locale.ROOT).contains("oceanbase"));
            }
            Assert.assertTrue(2 == count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }

    private void loadDataWithTTL() throws Exception {
        // load data
        Timestamp curTs = new Timestamp(System.currentTimeMillis());
        Timestamp expireTs = new Timestamp(System.currentTimeMillis() - 1000000);
        client.insert(ttlTableName).setRowKey(colVal(idCol, 1))
                .addMutateRow(row(colVal(c2Col, 1),
                        colVal(expireTsCol, curTs),
                        colVal(txtCol, "Hello World")))
                .execute();
        client.insert(ttlTableName).setRowKey(colVal(idCol, 2))
                .addMutateRow(row(colVal(c2Col, 2),
                        colVal(expireTsCol, curTs),
                        colVal(txtCol, "OceanBase Database is a native, enterprise-level distributed database developed independently by the OceanBase team")))
                .execute();
        client.insert(ttlTableName).setRowKey(colVal(idCol, 3))
                .addMutateRow(row(colVal(c2Col, 3),
                        colVal(expireTsCol, expireTs),
                        colVal(txtCol, "Learn about SQL and database administration in oceanBase")))
                .execute();
        client.insert(ttlTableName).setRowKey(colVal(idCol, 4))
                .addMutateRow(row(colVal(c2Col, 4),
                        colVal(expireTsCol, expireTs),
                        colVal(txtCol, "Master the art of full text searching")))
                .execute();
    }

    @Test
    public void testFTSQueryWithTTL() throws Exception {
        try {
            executeSQL(truncateTTLTableSQL);
            client.addRowKeyElement(ttlTableName, new String[]{"id"});
            //load data
            loadDataWithTTL();
            //sync query
            QueryResultSet resultSet = client.query(ttlTableName)
                    .setSearchText("oceanbase")
                    .indexName("full_idx1_tbl1")
                    .execute();
            int count = 0;
            while(resultSet.next()) {
                count++;
                Map<String, Object> row = resultSet.getRow();
                Assert.assertEquals(4, row.size());
                int id = (int) row.get("id");
                Assert.assertEquals(id, row.get("c2"));
                Assert.assertTrue(((String)row.get("txt")).toLowerCase(Locale.ROOT).contains("oceanbase"));
            }
            Assert.assertTrue(1 == count);

            // async query
            QueryResultSet asyncResultSet = client.query(ttlTableName)
                    .indexName("full_idx1_tbl1")
                    .setSearchText("oceanbase")
                    .asyncExecute();
            count = 0;
            while(asyncResultSet.next()) {
                count++;
                Map<String, Object> row = asyncResultSet.getRow();
                Assert.assertEquals(4, row.size());
                int id = (int) row.get("id");
                Assert.assertEquals(id, row.get("c2"));
                Assert.assertTrue(((String)row.get("txt")).toLowerCase(Locale.ROOT).contains("oceanbase"));
            }
            Assert.assertTrue(1 == count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncateTTLTableSQL);
        }
    }

    @Test
    public void testFtsQuery() throws Exception {
        try {
            //sync query
            QueryResultSet resultSet = client.query(tableName)
                    .setSearchText("native")
                    .indexName("full_idx1_tbl1")
                    .execute();
            while(resultSet.next()) {
                Map<String, Object> row = resultSet.getRow();
                for (Map.Entry<String, Object> entry: row.entrySet()) {
                    System.out.println("colname: " + entry.getKey() + " \nvalue: " + entry.getValue());
                }
                System.out.println();
            }
            // async query
            System.out.println("========async query:=========");
            QueryResultSet asyncResultSet = client.query(tableName)
                    .indexName("full_idx1_tbl1")
                    .setSearchText("oceanbase")
                    .asyncExecute();
            while(asyncResultSet.next()) {
                Map<String, Object> row = asyncResultSet.getRow();
                for (Map.Entry<String, Object> entry: row.entrySet()) {
                    System.out.println("colname: " + entry.getKey() + " \nvalue: " + entry.getValue());
                }
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void executeSQL(String createSQL) throws SQLException {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(createSQL);
    }

    @Test
    public void testQueryWithLimitOffset() throws Exception {
        try {
            executeSQL(truncateNoPartTableSQL);
            client.addRowKeyElement(noPartTableName, new String[] {"id"});
            //load data
            loadData(noPartTableName);
            //sync query
            QueryResultSet resultSet = client.query(noPartTableName)
                    .setSearchText("oceanbase")
                    .indexName("full_idx1_tbl1")
                    .limit(0,1)
                    .execute();
            int count = 0;
            while(resultSet.next()) {
                count++;
                Map<String, Object> row = resultSet.getRow();
                Assert.assertEquals(3, row.size());
                int id = (int) row.get("id");
                Assert.assertEquals(id, row.get("c2"));
                Assert.assertTrue(((String)row.get("txt")).toLowerCase(Locale.ROOT).contains("oceanbase"));
            }
            Assert.assertTrue(1 == count);

            // async query
            QueryResultSet asyncResultSet = client.query(noPartTableName)
                    .indexName("full_idx1_tbl1")
                    .setSearchText("oceanbase")
                    .limit(0,1)
                    .asyncExecute();
            count = 0;
            while(asyncResultSet.next()) {
                count++;
                Map<String, Object> row = asyncResultSet.getRow();
                Assert.assertEquals(3, row.size());
                int id = (int) row.get("id");
                Assert.assertEquals(id, row.get("c2"));
                Assert.assertTrue(((String)row.get("txt")).toLowerCase(Locale.ROOT).contains("oceanbase"));
            }
            Assert.assertTrue(1 == count);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncateNoPartTableSQL);
        }
    }

    @Test
    public void testBatch() throws Exception {
        try {
            executeSQL(truncatePartTableSQL);
            int rowCnt = 6;
            Object values[][] = {{1, 1, "Learn about SQL and database administration in oceanBase"}, {2, 2, "Can you pass me the salt, please?"},
                    {3, 3, "OceanBase Database is a native, enterprise-level distributed database"}, {4, 4, "We enjoyed a peaceful walk."},
                    {5, 5, "Learn about SQL and database administration in oceanBase"}, {6, 6, "Master the art of full text searching"}};
            // multi insert
            {
                BatchOperation insBatchOps = client.batchOperation(partTableName);
                for (int i = 0; i < rowCnt; i++) {
                    Object[] curRow = values[i];
                    Insert insert = new Insert();
                    insert.setRowKey(row(colVal(idCol, curRow[0])));
                    insert.addMutateRow(row(colVal(c2Col, curRow[1]), colVal(txtCol, curRow[2])));
                    insBatchOps.addOperation(insert);
                }
                BatchOperationResult insRes = insBatchOps.execute();
                Assert.assertEquals(6, insRes.size());
            }
            // multi get
            {
                BatchOperation getBatchOps = client.batchOperation(partTableName);
                for (int i = 0; i < rowCnt; i++) {
                    Object[] curRow = values[i];
                    TableQuery query = client.query(partTableName).setRowKey(row(colVal(idCol, curRow[0])));
                    getBatchOps.addOperation(query);
                }
                BatchOperationResult getRes = getBatchOps.execute();
                Assert.assertEquals(6, getRes.size());
                for (int i = 0; i < rowCnt; i++) {
                    int idx = (int) getRes.get(i).getOperationRow().get(idCol) - 1;
                    Assert.assertEquals(idx + 1, (int) getRes.get(i).getOperationRow().get(c2Col));
                    Assert.assertEquals(values[idx][2], getRes.get(i).getOperationRow().get(txtCol));
                }
            }
            // hyper operation
            {
                BatchOperation hyperOps = client.batchOperation(partTableName);
                // insertup
                InsertOrUpdate insup = new InsertOrUpdate();
                insup.setRowKey(row(colVal(idCol, values[0][0])))
                        .addMutateRow(row(colVal(c2Col, (int)values[0][1] + 100),
                                colVal(txtCol, values[0][2] + " " + values[1][2])));
                hyperOps.addOperation(insup);
                // update
                Update upd = new Update();
                upd.setRowKey(row(colVal(idCol, values[1][0])))
                    .addMutateRow(row(colVal(c2Col, (int)values[1][1] + 100),
                            colVal(txtCol, values[1][2] + " " + values[2][2])));
                hyperOps.addOperation(upd);
                // replace
                Replace replace = new Replace();
                replace.setRowKey(row(colVal(idCol, values[2][0])))
                    .addMutateRow(row(colVal(c2Col, (int)values[2][1] + 100),
                            colVal(txtCol, values[2][2] + " " + values[3][2])));
                hyperOps.addOperation(replace);

                // increment
                Increment increment = new Increment();
                increment.setRowKey(row(colVal(idCol, values[3][0])))
                    .addMutateRow(row(colVal(c2Col, 100)));
                hyperOps.addOperation(increment);

                // append
                Append append = new Append();
                append.setRowKey(row(colVal(idCol, values[3][0])))
                        .addMutateRow(row(colVal(txtCol," " + values[4][2])));
                hyperOps.addOperation(append);

                BatchOperationResult hyperRes = hyperOps.execute();
                Assert.assertEquals(5, hyperRes.size());

                BatchOperation getBatchOps = client.batchOperation(partTableName);
                for (int i = 0; i < rowCnt - 2; i++) {
                    Object[] curRow = values[i];
                    TableQuery query = client.query(partTableName).setRowKey(row(colVal(idCol, curRow[0])));
                    getBatchOps.addOperation(query);
                }
                BatchOperationResult getRes = getBatchOps.execute();
                Assert.assertEquals(4, getRes.size());
                for (int i = 0; i < rowCnt - 2; i++) {
                    int idx = (int) getRes.get(i).getOperationRow().get(idCol) - 1;
                    Assert.assertEquals((int)values[idx][1] + 100, (int) getRes.get(i).getOperationRow().get(c2Col));
                    Assert.assertEquals(values[idx][2] + " " + values[idx + 1][2], getRes.get(i).getOperationRow().get(txtCol));
                }
            }
            // multi delete
            {
                BatchOperation delBatchOps = client.batchOperation(partTableName);
                for (int i = 0; i < rowCnt; i++) {
                    Object[] curRow = values[i];
                    Delete delete = new Delete();
                    delete.setRowKey(row(colVal(idCol, curRow[0])));
                    delBatchOps.addOperation(delete);
                }
                BatchOperationResult delRes = delBatchOps.setReturnOneResult(true).execute();
                Assert.assertEquals(1, delRes.size());
                Assert.assertEquals(6, delRes.get(0).getAffectedRows());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executeSQL(truncatePartTableSQL);
        }
    }
}

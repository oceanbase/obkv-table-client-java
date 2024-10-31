package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.table.ObTable;
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
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

public class ObTableFullTextIndexTest {
    ObTableClient client;
    String tableName = "tbl1";
    String ttlTableName = "ttl_tbl1";
    String createTableSQL = "CREATE TABLE IF NOT EXISTS tbl1(id INT, c2 INT, txt text, PRIMARY KEY (id), FULLTEXT INDEX full_idx1_tbl1(txt));";
    String createTTLTableSQL = "CREATE TABLE IF NOT EXISTS ttl_tbl1(id INT, c2 INT, txt text, expired_ts timestamp(6), PRIMARY KEY (id), FULLTEXT INDEX full_idx1_tbl1(txt)) TTL(expired_ts + INTERVAL 10 SECOND);;";
    String truncateTableSQL = "truncate table tbl1;";
    String truncateTTLTableSQL = "truncate table ttl_tbl1;";
    String dropTableSQL = "drop table tbl1";
    String idCol = "id";
    String c2Col = "c2";
    String txtCol = "txt";
    String expireTsCol = "expired_ts";
    @Before
    public void setup() throws Exception {
        executeSQL(createTableSQL);
        executeSQL(createTTLTableSQL);
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    @After
    public void teardown() throws Exception {
//        executeSQL(dropTableSQL);
    }

    @Test
    public void testInsert() throws Exception {
        try{
            MutationResult res = client.insert(tableName).setRowKey(colVal(idCol, 1))
                    .addMutateRow(row(colVal(txtCol, "OceanBase Database is a native, " +
                            "enterprise-level distributed database developed independently by the OceanBase team")))
                    .execute();
            Assert.assertEquals(1, res.getAffectedRows());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGet() throws Exception {
        try{
            MutationResult insRes = client.insert(tableName).setRowKey(colVal(idCol, 3))
                    .addMutateRow(row(colVal(txtCol, "OceanBase Database is a native, " +
                            "enterprise-level distributed database developed independently by the OceanBase team")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            Map<String, Object> res = client.get(tableName, new Object[] { 3 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testDel() throws Exception {
        try{
            MutationResult insRes = client.insert(tableName).setRowKey(colVal(idCol, 4))
                    .addMutateRow(row(colVal(txtCol, "aaa asdjl asdjlakjsdl hello select new fine")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());
            // get
            Map<String, Object> res = client.get(tableName, new Object[] { 1 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }
            // del
            MutationResult delRes = client.delete(tableName).setRowKey(colVal(idCol, 1)).execute();
            Assert.assertEquals(1, delRes.getAffectedRows());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testUpd() throws Exception {
        try{
            MutationResult insRes = client.insert(tableName).setRowKey(colVal(idCol, 5))
                    .addMutateRow(row(colVal(txtCol, "aaa asdjl asdjlakjsdl hello select new fine")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());
            // get
            Map<String, Object> res = client.get(tableName, new Object[] { 5 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

            // update with fulltext columns
            System.out.println("update with fulltext column");
            MutationResult updRes = client.update(tableName).setRowKey(colVal(idCol, 5))
                    .addMutateRow(row(colVal(txtCol, "The sun was setting on the horizon, casting a warm golden glow over the tranquil ocean. ")))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get again
            res = client.get(tableName, new Object[] { 5 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

            // update with non-fulltext columns
            System.out.println("update with non-fulltext column");
            updRes = client.update(tableName).setRowKey(colVal(idCol, 5))
                    .addMutateRow(row(colVal(c2Col, 10)))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get again
            res = client.get(tableName, new Object[] { 5 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

            // update all columns
            System.out.println("update all column");
            updRes = client.update(tableName).setRowKey(colVal(idCol, 5))
                    .addMutateRow(row(colVal(c2Col, 100),
                        colVal(txtCol, "As the day came to a close, the peaceful scene served as a reminder of the beauty and serenity that nature has to offer.")))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get again
            res = client.get(tableName, new Object[] { 5 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInsOrUpd() throws Exception {
        try {
            // insertup-insert
            MutationResult insRes = client.insertOrUpdate(tableName).setRowKey(colVal(idCol, 6))
                    .addMutateRow(row(colVal(txtCol, "The sun rose over the horizon, casting a warm glow across the meadow. ")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());
            // get
            Map<String, Object> res = client.get(tableName, new Object[] { 6 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

            // insertup-update with non fulltext column
            MutationResult updRes = client.insertOrUpdate(tableName).setRowKey(colVal(idCol, 6))
                    .addMutateRow(row(colVal(c2Col, 100)))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get
            res = client.get(tableName, new Object[] { 6 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }
            // insertup-update with fulltext column
            updRes = client.insertOrUpdate(tableName).setRowKey(colVal(idCol, 6))
                    .addMutateRow(row(colVal(txtCol, " The birds chirped happily as they flew from tree to tree")))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get
            res = client.get(tableName, new Object[] { 6 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

            // insertup-update with all column
            updRes = client.insertOrUpdate(tableName).setRowKey(colVal(idCol, 6))
                    .addMutateRow(row(colVal(c2Col, 106),
                            colVal(txtCol, " The birds chirped happily as they flew from tree to tree")))
                    .execute();
            Assert.assertEquals(1, updRes.getAffectedRows());
            // get
            res = client.get(tableName, new Object[] { 6 }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executeSQL(truncateTableSQL);
        }
    }

    @Test
    public void testReplace() throws Exception {
        try {
            int id = 7;
            // replace-insert
            MutationResult insRes = client.replace(tableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, "The sun rose over the horizon, casting a warm glow across the meadow. ")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());
            // get
            Map<String, Object> res = client.get(tableName, new Object[] { id }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

            // replace-conflict with non fulltext column
            MutationResult updRes = client.replace(tableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 100)))
                    .execute();
            Assert.assertEquals(2, updRes.getAffectedRows());
            // get
            res = client.get(tableName, new Object[] { id }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }
            // replace-conflict with fulltext column
            updRes = client.replace(tableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(txtCol, " The birds chirped happily as they flew from tree to tree")))
                    .execute();
            Assert.assertEquals(2, updRes.getAffectedRows());
            // get
            res = client.get(tableName, new Object[] { id }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

            // replace-conflict with all column
            updRes = client.replace(tableName).setRowKey(colVal(idCol, id))
                    .addMutateRow(row(colVal(c2Col, 100+id),
                            colVal(txtCol, " The birds chirped happily as they flew from tree to tree")))
                    .execute();
            Assert.assertEquals(2, updRes.getAffectedRows());
            // get
            res = client.get(tableName, new Object[] { id }, null);
            for (Map.Entry<String, Object> entry: res.entrySet()) {
                System.out.println("key: "+ entry.getKey()+" value: "+ entry.getValue());
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            executeSQL(truncateTableSQL);
        }
    }

    @Test
    public void testTTLInsert() throws Exception {
        try {
            // 写入新行
            int id = 8;
            Timestamp curTs = new Timestamp(System.currentTimeMillis());
            Timestamp expireTs = new Timestamp(System.currentTimeMillis() - 1000000);
            MutationResult insRes = client.insert(ttlTableName).setRowKey(colVal(idCol, id + 1))
                    .addMutateRow(row(colVal(expireTsCol, expireTs),
                            colVal(txtCol, "The sun rose over the horizon, casting a warm glow across the meadow. ")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            insRes = client.insert(ttlTableName).setRowKey(colVal(idCol, id + 2))
                    .addMutateRow(row(colVal(expireTsCol, curTs),
                            colVal(txtCol, "The birds chirped happily as they flew from tree to tree")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            // 过期，删除旧行，写入新行
            insRes = client.insert(ttlTableName).setRowKey(colVal(idCol, id + 1))
                    .addMutateRow(row(colVal(expireTsCol, curTs),
                            colVal(txtCol, "Two roads diverged in a wood")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            // get
            Map<String, Object> res = client.get(ttlTableName, new Object[]{id + 1}, null);
            for (Map.Entry<String, Object> entry : res.entrySet()) {
                System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
            }

            // 未过期
            // - insert操作
            try {
                client.insert(ttlTableName).setRowKey(colVal(idCol, id + 2))
                        .addMutateRow(row(colVal(expireTsCol, curTs),
                                colVal(txtCol, "I took the one less traveled by")))
                        .execute();
            } catch (ObTableException e) {
                Assert.assertEquals(ResultCodes.OB_ERR_PRIMARY_KEY_DUPLICATE.errorCode, e.getErrorCode());
            }
            // - insertup操作
            insRes = client.insertOrUpdate(ttlTableName).setRowKey(colVal(idCol, id + 2))
                    .addMutateRow(row(colVal(expireTsCol, curTs),
                            colVal(txtCol, "I took the one less traveled by")))
                    .execute();
            Assert.assertEquals(1, insRes.getAffectedRows());

            // get
            res = client.get(ttlTableName, new Object[]{id + 2}, null);
            for (Map.Entry<String, Object> entry : res.entrySet()) {
                System.out.println("key: " + entry.getKey() + " value: " + entry.getValue());
            }
        } catch(Exception e) {
            e.printStackTrace();
        } finally {
           // executeSQL(truncateTTLTableSQL);
        }
    }

    private void executeSQL(String createSQL) throws SQLException {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(createSQL);
    }
}

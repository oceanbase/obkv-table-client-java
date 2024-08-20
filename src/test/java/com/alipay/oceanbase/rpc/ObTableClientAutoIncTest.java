/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

import com.alipay.oceanbase.rpc.bolt.ObTableClientTestBase;
import com.alipay.oceanbase.rpc.bolt.ObTableTest;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableValueFilter;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.compareVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ObTableClientAutoIncTest {
    public Table client;

    public void setClient(Table client) {
        this.client = client;
    }

    @After
    public void close() throws Exception {
        if (null != this.client && this.client instanceof ObTableClient) {
            ((ObTableClient) this.client).close();
        }
    }

    @Before
    public void setup() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty(Property.RPC_CONNECT_TIMEOUT.getKey(), "800");
        obTableClient.addProperty(Property.RPC_LOGIN_TIMEOUT.getKey(), "800");
        obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), "1");
        obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), "3000");
        obTableClient.addProperty(Property.RUNTIME_BATCH_MAX_WAIT.getKey(), "3000");
        obTableClient.addProperty(Property.RUNTIME_BATCH_EXECUTOR.getKey(), "32");
        obTableClient.addProperty(Property.RPC_OPERATION_TIMEOUT.getKey(), "3000");
        obTableClient.init();
        obTableClient.addRowKeyElement("test_auto_increment_rowkey", new String[] { "c1", "c2" });
        obTableClient.addRowKeyElement("test_auto_increment_not_rowkey", new String[] { "c1" });

        this.client = obTableClient;
    }

    private void executeSQL(String createSQL) throws SQLException {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute(createSQL);
    }

    private void dropTable(String tableName) throws SQLException {
        // use sql to drop table
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("drop table " + tableName);
    }

    @Test
    // Test auto increment on rowkey
    public void testAutoIncrementRowkey() throws Exception {
        // todo: only support in 4.x currently
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4000)) {
            return;
        }

        final String TABLE_NAME = "test_auto_increment_rowkey";

        try {
            executeSQL("CREATE TABLE IF NOT EXISTS `test_auto_increment_rowkey` ("
                       + "`c1` int auto_increment,"
                       + "`c2` int NOT NULL,"
                       + "`c3` int DEFAULT NULL,"
                       + "`c4` varchar(255) DEFAULT NULL,"
                       + "PRIMARY KEY(`c1`, `c2`)) partition by range columns(`c2`)"
                       + "(PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (1000));");

            client.insert(TABLE_NAME, new Object[] { 0, 1 }, new String[] { "c3" },
                new Object[] { 1 });

            TableQuery tableQuery = client.query(TABLE_NAME);
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
            client.insert(TABLE_NAME, new Object[] { 100, 1 }, new String[] { "c3" },
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
            client.insert(TABLE_NAME, new Object[] { 0, 1 }, new String[] { "c3" },
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
            client.delete(TABLE_NAME, new Object[] { 101, 1 });

            // test confirm delete
            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 101);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertFalse(result.next());

            // test update
            ObTableValueFilter filter_3 = compareVal(ObCompareOp.EQ, "c3", 1);

            MutationResult updateResult = client.update(TABLE_NAME)
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
            MutationResult theResult = client.replace(TABLE_NAME)
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
            theResult = client.replace(TABLE_NAME).setRowKey(colVal("c1", 101), colVal("c2", 1))
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
            theResult = client.insertOrUpdate(TABLE_NAME)
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
            theResult = client.insertOrUpdate(TABLE_NAME)
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
            theResult = client.insertOrUpdate(TABLE_NAME)
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
            value = client.increment(TABLE_NAME, new Object[] { 0, 1 }, new String[] { "c3" },
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
            value = client.increment(TABLE_NAME, new Object[] { 104, 1 }, new String[] { "c3" },
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
                value = client.increment(TABLE_NAME, new Object[] { 104, 1 },
                    new String[] { "c1" }, new Object[] { 1 }, true);
            } catch (ObTableException e) {
                assertNotNull(e);
                assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode, e.getErrorCode());
            }

            // test append not exist, insert
            Map<String, Object> res = client.append(TABLE_NAME, new Object[] { 0, 1 },
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
            res = client.append(TABLE_NAME, new Object[] { 105, 1 }, new String[] { "c4" },
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
            dropTable(TABLE_NAME);
        }
    }

    @Test
    // Test auto increment on not rowkey
    public void testAutoIncrementNotRowkey() throws Exception {
        // todo: only support in 4.x currently
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4000)) {
            return;
        }

        final String TABLE_NAME = "test_auto_increment_not_rowkey";

        try {
            executeSQL("CREATE TABLE IF NOT EXISTS `test_auto_increment_not_rowkey` ("
                       + "`c1` int NOT NULL,"
                       + "`c2` int DEFAULT NULL,"
                       + "`c3` tinyint auto_increment,"
                       + "`c4` varchar(255) DEFAULT NULL,"
                       + "PRIMARY KEY(`c1`)) partition by range columns(`c1`)"
                       + "(PARTITION p0 VALUES LESS THAN (100), PARTITION p1 VALUES LESS THAN (1000));");

            client
                .insert(TABLE_NAME, new Object[] { 1 }, new String[] { "c2" }, new Object[] { 1 });

            TableQuery tableQuery = client.query(TABLE_NAME);
            tableQuery.select("c1", "c2", "c3");
            ObTableValueFilter filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 1);
            tableQuery.setFilter(filter);
            tableQuery.addScanRange(new Object[] { 0 }, new Object[] { 90 });
            QueryResultSet result = tableQuery.execute();
            Assert.assertTrue(result.next());
            Map<String, Object> value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 1, value.get("c3"));

            // test insert use user value
            client.insert(TABLE_NAME, new Object[] { 2 }, new String[] { "c2", "c3" },
                new Object[] { 1, (byte) 100 });

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 2);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 100, value.get("c3"));

            // test insert sync global auto inc val
            client
                .insert(TABLE_NAME, new Object[] { 3 }, new String[] { "c2" }, new Object[] { 1 });

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 3);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 101, value.get("c3"));

            // test delete
            client.delete(TABLE_NAME, new Object[] { 1 });

            // test confirm delete
            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 1);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertFalse(result.next());

            // test update
            ObTableValueFilter filter_3 = compareVal(ObCompareOp.EQ, "c2", 1);

            MutationResult updateResult = client.update(TABLE_NAME).setRowKey(colVal("c1", 3))
                .setFilter(filter_3).addMutateRow(row(colVal("c3", (byte) 5))).execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 3);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 5, value.get("c3"));

            // test replace not exist, insert
            MutationResult theResult = client.replace(TABLE_NAME).setRowKey(colVal("c1", 4))
                .addMutateRow(row(colVal("c2", 1))).execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 4);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 102, value.get("c3"));

            // test replace exist, replace
            theResult = client.replace(TABLE_NAME).setRowKey(colVal("c1", 3))
                .addMutateRow(row(colVal("c3", (byte) 20), colVal("c2", 1))).execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 3);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 20, value.get("c3"));

            // test insertup not exist, insert
            theResult = client.insertOrUpdate(TABLE_NAME).setRowKey(colVal("c1", 5))
                .addMutateRow(row(colVal("c2", 1))).execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 5);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 103, value.get("c3"));

            // test insertup exist, update
            theResult = client.insertOrUpdate(TABLE_NAME).setRowKey(colVal("c1", 5))
                .addMutateRow(row(colVal("c3", (byte) 50), colVal("c2", 1))).execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 5);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 50, value.get("c3"));

            // test insertup exist, update again
            theResult = client.insertOrUpdate(TABLE_NAME).setRowKey(colVal("c1", 5))
                .addMutateRow(row(colVal("c3", (byte) 50), colVal("c2", 1))).execute();

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 5);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(1, value.get("c2"));
            Assert.assertEquals((byte) 50, value.get("c3"));

            // test increment not exist, insert
            value = client.increment(TABLE_NAME, new Object[] { 6 }, new String[] { "c2" },
                new Object[] { 6 }, true);

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 6);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(6, value.get("c2"));
            Assert.assertEquals((byte) 104, value.get("c3"));

            // test increment exist, increment
            value = client.increment(TABLE_NAME, new Object[] { 6 }, new String[] { "c2" },
                new Object[] { 6 }, true);

            tableQuery.select("c1", "c2", "c3");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 6);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals(12, value.get("c2"));
            Assert.assertEquals((byte) 104, value.get("c3"));

            // test illegal increment on auto increment column
            try {
                value = client.increment(TABLE_NAME, new Object[] { 6 }, new String[] { "c3" },
                    new Object[] { (byte) 5 }, true);
            } catch (ObTableException e) {
                assertNotNull(e);
                assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode, e.getErrorCode());
            }

            // test append not exist, insert
            Map<String, Object> res = client.append(TABLE_NAME, new Object[] { 200 },
                new String[] { "c4" }, new Object[] { "a" }, true);

            tableQuery.select("c1", "c2", "c3", "c4");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 200);
            tableQuery.setFilter(filter);
            tableQuery.addScanRange(new Object[] { 100 }, new Object[] { 200 });
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals((byte) 105, value.get("c3"));
            Assert.assertEquals("a", value.get("c4"));

            // test append exist, append
            res = client.append(TABLE_NAME, new Object[] { 200 }, new String[] { "c4" },
                new Object[] { "b" }, true);

            tableQuery.select("c1", "c2", "c3", "c4");
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 200);
            tableQuery.setFilter(filter);
            tableQuery.addScanRange(new Object[] { 100 }, new Object[] { 200 });
            result = tableQuery.execute();
            Assert.assertTrue(result.next());
            value = result.getRow();
            Assert.assertEquals((byte) 105, value.get("c3"));
            Assert.assertEquals("ab", value.get("c4"));

            //  the total number of data
            tableQuery = client.query(TABLE_NAME);
            tableQuery.addScanRange(new Object[] { 0 }, new Object[] { 90 });
            filter = new ObTableValueFilter(ObCompareOp.LT, "c1", 90);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertEquals(5, result.cacheSize());

            //  the total number of data
            tableQuery = client.query(TABLE_NAME);
            tableQuery.addScanRange(new Object[] { 100 }, new Object[] { 300 });
            filter = new ObTableValueFilter(ObCompareOp.EQ, "c1", 200);
            tableQuery.setFilter(filter);
            result = tableQuery.execute();
            Assert.assertEquals(1, result.cacheSize());
        } finally { // drop table
            dropTable(TABLE_NAME);
        }
    }

    // CREATE TABLE IF NOT EXISTS `test_auto_increment_one_rowkey` (`c1` int auto_increment, `c2` int NOT NULL, PRIMARY KEY(`c1`));
    @Test
    // test insert null into auto increment column
    public void testAutoColumnRowKey() throws Exception {
        final String TABLE_NAME = "test_auto_increment_one_rowkey";
        try {
            final ObTableClient client = (ObTableClient) this.client;
            client.addRowKeyElement(TABLE_NAME, new String[] { "c1" });
            try {
                client.insert(TABLE_NAME, null, new String[] { "c2" }, new Object[] { 1 });
            } catch (Exception e) {
                Assert.assertEquals("Cannot read the array length because \"rowKeys\" is null",
                    ((NullPointerException) e).getMessage());
            }
        } finally {
        }
    }
}

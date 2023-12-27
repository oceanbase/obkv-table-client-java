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

package com.alipay.oceanbase.rpc.bolt;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableDuplicateKeyException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHTableFilter;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;

public abstract class ObTableClientTestBase {

    public Table client;

    @After
    public void close() throws Exception {
        if (null != this.client && this.client instanceof ObTableClient) {
            ((ObTableClient) this.client).close();
        }
    }

    @Test
    public void test_batch() throws Exception {
        /*
        CREATE TABLE `test_varchar_table` (
        `c1` varchar(20) NOT NULL,
        `c2` varchar(20) DEFAULT NULL,
        PRIMARY KEY (`c1`)
        ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
        */
        if (this.getClass().getSimpleName().equals("ObTableTest")) {
            // could not get tableId from ObTable
            return;
        }

        for (int i = 0; i < 100; i++) {
            TableBatchOps batchOps = client.batch("test_varchar_table");

            batchOps.insert("foo", new String[] { "c2" }, new String[] { "bar" });
            batchOps.delete("foo");
            List<Object> objectList = batchOps.execute();
            assertEquals(2, objectList.size());
            assertEquals(1L, objectList.get(0));
            assertEquals(1L, objectList.get(1));
            objectList = batchOps.execute();
            batchOps.insert("foo", new String[] { "c2" }, new String[] { "bar" });
            batchOps.delete("foo");
            assertEquals(2, objectList.size());
            assertEquals(1L, objectList.get(0));
            assertEquals(1L, objectList.get(1));
            objectList.clear();
            Thread.sleep(10);
        }
    }

    @Test
    public void test_varchar_all() throws Exception {
        try {
            test_varchar_insert();
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                test_varchar_get();
            }
            System.err.println("cost: " + (System.currentTimeMillis() - start));
            test_varchar_update();
            test_varchar_insertOrUpdate();
            test_varchar_replace();

            assertEquals(1L, client.delete("test_varchar_table", "foo"));
        } finally {
            client.delete("test_varchar_table", "foo");
            client.delete("test_varchar_table", "bar");
            client.delete("test_varchar_table", "baz");
        }
    }

    @Test
    public void test_blob_all() throws Exception {

        /*
        *
        CREATE TABLE `test_blob_table` (
        `c1` varchar(20) NOT NULL,
        `c2` blob DEFAULT NULL,
        PRIMARY KEY (`c1`)
        ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
        * */

        // NOTE: Use the default test_varchar_table ObTable to insert data for test_blob_table will causes OB_NOT_MASTER error
        // when their partition leader is not in the same observer
        if (client instanceof ObTable) {
            return;
        }
        try {
            test_blob_insert();
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                test_blob_get();
            }
            System.err.println("cost: " + (System.currentTimeMillis() - start));
            test_blob_update();
            test_blob_insertOrUpdate();
            test_blob_replace();

            assertEquals(1L, client.delete("test_blob_table", "foo"));
        } finally {
            client.delete("test_blob_table", "foo");
            client.delete("test_blob_table", "bar");
            client.delete("test_blob_table", "baz");
            client.delete("test_blob_table", "qux");
        }
    }

    @Test
    public void test_longblob_all() throws Exception {

        /*
        *
        CREATE TABLE `test_longblob_table` (
        `c1` varchar(20) NOT NULL,
        `c2` longblob DEFAULT NULL,
        PRIMARY KEY (`c1`)
        ) DEFAULT CHARSET = utf8mb4 COMPRESSION = 'lz4_1.0' REPLICA_NUM = 3 BLOCK_SIZE = 16384 USE_BLOOM_FILTER = FALSE TABLET_SIZE = 134217728 PCTFREE = 10
        * */
        // NOTE: Use the default test_varchar_table ObTable to insert data for test_blob_table will causes OB_NOT_MASTER error
        // when their partition leader is not in the same observer
        if (client instanceof ObTable) {
            return;
        }
        try {
            test_longblob_insert();
            long start = System.currentTimeMillis();
            for (int i = 0; i < 10; i++) {
                test_longblob_get();
                System.out.println(i);
            }
            System.err.println("cost: " + (System.currentTimeMillis() - start));
            test_longblob_update();
            test_longblob_insertOrUpdate();
            test_longblob_replace();
            assertEquals(1L, client.delete("test_longblob_table", "foo"));
        } finally {
            client.delete("test_longblob_table", "foo");
            client.delete("test_longblob_table", "bar");
            client.delete("test_longblob_table", "baz");
            client.delete("test_longblob_table", "qux");
        }
    }

    @Test
    public void test_varchar_exceptions() throws Exception {
        ObTableException exception = null;
        try {
            client.insert("not_exist_table", "foo", new String[] { "c2" }, new String[] { "bar" });
        } catch (ObTableException ex) {
            ex.printStackTrace();
            exception = ex;
        }
        assertNotNull(exception);
        if ((client instanceof ObTableClient) && ((ObTableClient) client).isOdpMode()) {
            assertEquals(ResultCodes.OB_TABLE_NOT_EXIST.errorCode, exception.getErrorCode());
        } else {
            assertEquals(ResultCodes.OB_ERR_UNKNOWN_TABLE.errorCode, exception.getErrorCode());
        }

        exception = null;
        try {
            client.insert("test_varchar_table_for_exception", "foo", new String[] { "c3" },
                new String[] { "bar" });
        } catch (ObTableException ex) {
            exception = ex;
        }
        assertNotNull(exception);
        System.out.printf("exception msg：%s\n", exception.getMessage());
        assertEquals(ResultCodes.OB_ERR_BAD_FIELD_ERROR.errorCode, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Unknown column"));

        exception = null;
        try {
            client.insert("test_varchar_table_for_exception", 1, new String[] { "c2" },
                new String[] { "bar" });
        } catch (ObTableException ex) {
            exception = ex;
        }
        assertNotNull(exception);
        System.out.printf("exception msg：%s\n", exception.getMessage());
        assertEquals(ResultCodes.OB_KV_COLUMN_TYPE_NOT_MATCH.errorCode, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Column type for 'c1' not match, schema column type is 'VARCHAR', input column type is 'INT'"));

        exception = null;
        try {
            client.insert("test_varchar_table_for_exception", "1", new String[] { "c2" },
                new Integer[] { 1 });
        } catch (ObTableException ex) {
            exception = ex;
        }
        assertNotNull(exception);
        System.out.printf("exception msg：%s\n", exception.getMessage());
        assertEquals(ResultCodes.OB_KV_COLUMN_TYPE_NOT_MATCH.errorCode, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Column type for 'c2' not match, schema column type is 'VARCHAR', input column type is 'INT'"));

        exception = null;
        try {
            client.insert("test_varchar_table_for_exception", "1", new String[] { "c2" },
                new String[] { null });
        } catch (ObTableException ex) {
            exception = ex;
        }
        assertNotNull(exception);
        System.out.printf("exception msg：%s\n", exception.getMessage());
        assertEquals(ResultCodes.OB_BAD_NULL_ERROR.errorCode, exception.getErrorCode());
        assertTrue(exception.getMessage().contains("Column 'c2' cannot be null"));

        // TODO timeout
    }

    private void test_varchar_insert() throws Exception {
        // test insert
        assertEquals(1L, client.insert("test_varchar_table", "foo", new String[] { "c2" },
            new String[] { "bar" }));

        ObTableException ex = null;
        try {
            assertEquals(1L, client.insert("test_varchar_table", "foo", new String[] { "c2" },
                new String[] { "baz" }));
        } catch (ObTableException t) {
            ex = t;
        }
        assertNotNull(ex);
        ex = null;
        try {
            assertEquals(1L, client.insert("test_varchar_table", "foo", new String[] { "c2" },
                new String[] { "bar" }));
        } catch (ObTableException t) {
            ex = t;
        }
        assertNotNull(ex);
    }

    private void test_blob_insert() throws Exception {
        // test insert
        assertEquals(
            1L,
            client.insert("test_blob_table", "foo", new String[] { "c2" },
                new Object[] { "bar".getBytes() }));

        ObTableException ex = null;
        try {
            assertEquals(
                1L,
                client.insert("test_blob_table", "foo", new String[] { "c2" },
                    new Object[] { "baz".getBytes() }));
        } catch (ObTableException t) {
            ex = t;
        }
        assertNotNull(ex);
        ex = null;
        try {
            assertEquals(
                1L,
                client.insert("test_blob_table", "foo", new String[] { "c2" },
                    new Object[] { "bar".getBytes() }));
        } catch (ObTableException t) {
            ex = t;
        }
        assertNotNull(ex);

        // test insert string
        assertEquals(1L,
            client.insert("test_blob_table", "qux", new String[] { "c2" }, new String[] { "qux" }));
    }

    private void test_longblob_insert() throws Exception {
        // test insert binary
        assertEquals(
            1L,
            client.insert("test_longblob_table", "foo", new String[] { "c2" },
                new Object[] { "bar".getBytes() }));

        ObTableException ex = null;
        try {
            assertEquals(1L, client.insert("test_longblob_table", "foo", new String[] { "c2" },
                new Object[] { "baz".getBytes() }));
        } catch (ObTableException t) {
            ex = t;
        }
        assertNotNull(ex);
        ex = null;
        try {
            assertEquals(1L, client.insert("test_longblob_table", "foo", new String[] { "c2" },
                new Object[] { "bar".getBytes() }));
        } catch (ObTableException t) {
            ex = t;
        }
        assertNotNull(ex);

        // test insert string
        assertEquals(1L, client.insert("test_longblob_table", "qux", new String[] { "c2" },
            new String[] { "qux" }));
    }

    private void test_varchar_get() throws Exception {

        // test_varchar_table
        Map<String, Object> values = client.get("test_varchar_table", "bar", new String[] { "c2" });
        assertNotNull(values);
        assertEquals(0, values.size());

        values = client.get("test_varchar_table", "foo", new String[] { "c2" });
        assertNotNull(values);
        assertEquals("bar", values.get("c2"));
    }

    private void test_blob_get() throws Exception {
        Map<String, Object> values = client.get("test_blob_table", "bar", new String[] { "c2" });
        assertNotNull(values);
        assertEquals(0, values.size());

        values = client.get("test_blob_table", "foo", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("bar", new String((byte[]) values.get("c2")));

        // test insert string and get
        values = client.get("test_blob_table", "qux", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("qux", new String((byte[]) values.get("c2")));
    }

    private void test_longblob_get() throws Exception {
        Map<String, Object> values = client
            .get("test_longblob_table", "bar", new String[] { "c2" });
        assertNotNull(values);
        assertEquals(0, values.size());

        values = client.get("test_longblob_table", "foo", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("bar", new String((byte[]) values.get("c2")));

        // test insert string and get
        values = client.get("test_longblob_table", "qux", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("qux", new String((byte[]) values.get("c2")));
    }

    private void test_varchar_update() throws Exception {
        assertEquals(1L, client.update("test_varchar_table", "foo", new String[] { "c2" },
            new String[] { "baz" }));

        Map<String, Object> values = client.get("test_varchar_table", "foo", new String[] { "c2" });
        assertEquals("baz", values.get("c2"));
    }

    private void test_blob_update() throws Exception {
        assertEquals(
            1L,
            client.update("test_blob_table", "foo", new String[] { "c2" },
                new Object[] { "baz".getBytes() }));

        Map<String, Object> values = client.get("test_blob_table", "foo", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("baz", new String((byte[]) values.get("c2")));

        assertEquals(1L,
            client.update("test_blob_table", "qux", new String[] { "c2" }, new Object[] { "bar" }));

        values = client.get("test_blob_table", "qux", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("bar", new String((byte[]) values.get("c2")));
    }

    private void test_longblob_update() throws Exception {
        assertEquals(
            1L,
            client.update("test_longblob_table", "foo", new String[] { "c2" },
                new Object[] { "baz".getBytes() }));

        Map<String, Object> values = client
            .get("test_longblob_table", "foo", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("baz", new String((byte[]) values.get("c2")));

        assertEquals(1L, client.update("test_longblob_table", "qux", new String[] { "c2" },
            new Object[] { "bar" }));

        values = client.get("test_longblob_table", "qux", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("bar", new String((byte[]) values.get("c2")));
    }

    private void test_varchar_insertOrUpdate() throws Exception {
        assertEquals(1L, client.insertOrUpdate("test_varchar_table", "foo", new String[] { "c2" },
            new String[] { "quux" }));

        Map<String, Object> values = client.get("test_varchar_table", "foo", new String[] { "c2" });
        assertEquals("quux", values.get("c2"));

        assertEquals(1L, client.insertOrUpdate("test_varchar_table", "bar", new String[] { "c2" },
            new String[] { "baz" }));

        values = client.get("test_varchar_table", "bar", new String[] { "c2" });
        assertEquals("baz", values.get("c2"));

    }

    private void test_blob_insertOrUpdate() throws Exception {
        assertEquals(1L, client.insertOrUpdate("test_blob_table", "foo", new String[] { "c2" },
            new Object[] { "quux".getBytes() }));

        Map<String, Object> values = client.get("test_blob_table", "foo", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("quux", new String((byte[]) values.get("c2")));

        assertEquals(1L, client.insertOrUpdate("test_blob_table", "bar", new String[] { "c2" },
            new String[] { "baz" }));

        values = client.get("test_blob_table", "bar", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("baz", new String((byte[]) values.get("c2")));

        assertEquals(1L, client.insertOrUpdate("test_blob_table", "qux", new String[] { "c2" },
            new String[] { "baz" }));

        values = client.get("test_blob_table", "bar", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("baz", new String((byte[]) values.get("c2")));
    }

    private void test_longblob_insertOrUpdate() throws Exception {
        assertEquals(1L, client.insertOrUpdate("test_longblob_table", "foo", new String[] { "c2" },
            new Object[] { "quux".getBytes() }));

        Map<String, Object> values = client
            .get("test_longblob_table", "foo", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("quux", new String((byte[]) values.get("c2")));

        assertEquals(1L, client.insertOrUpdate("test_longblob_table", "bar", new String[] { "c2" },
            new String[] { "baz" }));

        values = client.get("test_longblob_table", "bar", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("baz", new String((byte[]) values.get("c2")));

        assertEquals(1L, client.insertOrUpdate("test_longblob_table", "qux", new String[] { "c2" },
            new String[] { "baz" }));

        values = client.get("test_longblob_table", "bar", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("baz", new String((byte[]) values.get("c2")));
    }

    private void test_varchar_replace() throws Exception {
        assertEquals(2L, client.replace("test_varchar_table", "foo", new String[] { "c2" },
            new String[] { "bar" }));

        Map<String, Object> values = client.get("test_varchar_table", "foo", new String[] { "c2" });
        assertEquals("bar", values.get("c2"));

        assertEquals(1L, client.replace("test_varchar_table", "baz", new String[] { "c2" },
            new String[] { "bar" }));

        values = client.get("test_varchar_table", "baz", new String[] { "c2" });
        assertEquals("bar", values.get("c2"));
    }

    private void test_blob_replace() throws Exception {
        assertEquals(
            2L,
            client.replace("test_blob_table", "foo", new String[] { "c2" },
                new Object[] { "bar".getBytes() }));

        Map<String, Object> values = client.get("test_blob_table", "foo", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("bar", new String((byte[]) values.get("c2")));

        assertEquals(
            1L,
            client.replace("test_blob_table", "baz", new String[] { "c2" },
                new Object[] { "bar".getBytes() }));

        values = client.get("test_blob_table", "baz", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("bar", new String((byte[]) values.get("c2")));

        assertEquals(2L,
            client.replace("test_blob_table", "baz", new String[] { "c2" }, new String[] { "baz" }));

        values = client.get("test_blob_table", "baz", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("baz", new String((byte[]) values.get("c2")));
    }

    private void test_longblob_replace() throws Exception {
        assertEquals(2L, client.replace("test_longblob_table", "foo", new String[] { "c2" },
            new Object[] { "bar".getBytes() }));

        Map<String, Object> values = client
            .get("test_longblob_table", "foo", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("bar", new String((byte[]) values.get("c2")));

        assertEquals(1L, client.replace("test_longblob_table", "baz", new String[] { "c2" },
            new Object[] { "bar".getBytes() }));

        values = client.get("test_longblob_table", "baz", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("bar", new String((byte[]) values.get("c2")));

        assertEquals(2L, client.replace("test_longblob_table", "baz", new String[] { "c2" },
            new String[] { "baz" }));

        values = client.get("test_longblob_table", "baz", new String[] { "c2" });
        assertTrue(values.get("c2") instanceof byte[]);
        assertEquals("baz", new String((byte[]) values.get("c2")));
    }

    /**
     * 单 key 的方式，例如
     * scan[{"123"}, {"567"})
     * @throws Exception
     */
    @Test
    public void test_limit_query_1() throws Exception {
        Object[] c1 = new Object[] { "123", "124", "234", "456", "567" };
        Object[] c2 = new Object[] { "123c2", "124c2", "234c2", "456c2", "567c2" };
        if ((client instanceof ObTableClient) && ((ObTableClient) client).isOdpMode()) {
            // TODO: support stream result
        } else {
            try {
                for (int i = 0; i < 5; i++) {
                    client.insert("test_varchar_table", c1[i], new String[] { "c2" },
                        new Object[] { c2[i] });
                }
                // 123 <= xxx <= 567
                TableQuery tableQuery = client.queryByBatch("test_varchar_table");
                QueryResultSet result = tableQuery.setKeys("c1").select("c2").setBatchSize(1)
                    .addScanRange("123", true, "567", true).execute();
                for (int i = 0; i < 5; i++) {
                    Assert.assertTrue(result.next());
                    Map<String, Object> value = result.getRow();
                    assertEquals(value.get("c2"), c2[i]);
                }
                Assert.assertFalse(result.next());

                // 123 <= xxx < 567
                tableQuery = client.queryByBatch("test_varchar_table");
                result = tableQuery.setKeys("c1").select("c1", "c2").setBatchSize(1)
                    .addScanRange("123", true, "567", false).execute();
                for (int i = 0; i < 4; i++) {
                    Assert.assertTrue(result.next());
                    assertEquals(0, result.cacheSize());
                    Map<String, Object> value = result.getRow();
                    assertEquals(value.get("c2"), c2[i]);
                }
                Assert.assertFalse(result.next());
                result.close();
            } finally {
                for (int i = 0; i < 5; i++) {
                    client.delete("test_varchar_table", c1[i]);
                }
            }
        }
    }

    @Test
    public void test_limit_query_2() throws Exception {
        TableQuery tableQuery = client.queryByBatch("test_varchar_table");
        TableQuery tableQuery2 = client.queryByBatch("test_varchar_table");
        tableQuery.setOperationTimeout(100000);
        assertNotNull(tableQuery.getObTableQuery());
        tableQuery.setEntityType(new ObTableQueryRequest().getEntityType());
        assertNotNull(tableQuery.getEntityType());
        assertEquals("test_varchar_table", tableQuery.getTableName());
        tableQuery.addScanRange("1", "2");
        tableQuery2.addScanRangeStartsWith("1");
        tableQuery2.addScanRangeEndsWith("2");
        assertEquals(1, tableQuery.getObTableQuery().getKeyRanges().size());
        assertEquals(2, tableQuery2.getObTableQuery().getKeyRanges().size());

        try {
            tableQuery.scanOrder(true);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.indexName("test");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.primaryIndex();
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.filterString("111");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.setHTableFilter(new ObHTableFilter());
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.limit(10);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.limit(10, 10);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.addScanRange(new Object[] { "1" }, new Object[] { "3" }).setKeys("c1", "c1");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.setKeys("c1", "c3").select("c2", "c1");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.setMaxResultSize(100000);
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        if ((client instanceof ObTableClient) && ((ObTableClient) client).isOdpMode()) {
            // TODO: support stream result
        } else {
            ObTable obTable = new ObTable();
            try {
                tableQuery
                    .executeInit(new ObPair<Long, ObTableParam>(0L, new ObTableParam(obTable)));
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }

            try {
                tableQuery
                    .executeNext(new ObPair<Long, ObTableParam>(0L, new ObTableParam(obTable)));
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }
        }

        tableQuery.clear();
    }

    @Test
    public void test_batch_query() throws Exception {
        /*
        * CREATE TABLE `test_varchar_table` (
             `c1` varchar(20) NOT NULL,
             `c2` varchar(20) DEFAULT NULL,
            PRIMARY KEY (`c1`)); partition by KEY(`c1`) partitions 3;
            )*/

        // TODO: stream query is not supported in ODP mode
        if ((client instanceof ObTableClient) && ((ObTableClient) client).isOdpMode()) {
            return;
        }
        Object[] c1 = new Object[] { "123", "124", "234", "456", "567" };
        Object[] c2 = new Object[] { "123c2", "124c2", "234c2", "456c2", "567c2" };
        try {

            for (int i = 0; i < 5; i++) {
                client.insert("test_varchar_table", c1[i], new String[] { "c2" },
                    new Object[] { c2[i] });
            }
            //非阻塞query
            TableQuery tableQuery = client.queryByBatchV2("test_varchar_table");
            // 查询结果集
            tableQuery.select("c2");
            tableQuery.limit(5);
            tableQuery.primaryIndex();
            tableQuery.addScanRange("123", "567");
            tableQuery.setBatchSize(2);
            tableQuery.setMaxResultSize(10000);
            // 异步query start, 获取第一个batch的结果集
            QueryResultSet result = tableQuery.execute();

            for (int i = 0; i < 5; i++) {
                Assert.assertTrue(result.next());
                Map<String, Object> value = result.getRow();
                assertEquals(value.get("c2"), c2[i]);
                System.out.println("c2:" + value.get("c2"));
            }
            Assert.assertFalse(result.next());

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            for (int i = 0; i < 5; i++) {
                client.delete("test_varchar_table", c1[i]);
            }

        }
    }

    @Test
    public void test_batch_query_coverage() throws Exception {
        TableQuery tableQuery = client.queryByBatchV2("test_varchar_table");
        TableQuery tableQuery2 = client.queryByBatchV2("test_varchar_table");
        tableQuery.setOperationTimeout(100000);
        assertNotNull(tableQuery.getObTableQuery());
        tableQuery.setEntityType(new ObTableQueryRequest().getEntityType());
        assertNotNull(tableQuery.getEntityType());
        assertEquals("test_varchar_table", tableQuery.getTableName());
        tableQuery.addScanRange("1", "2");
        tableQuery2.addScanRangeStartsWith("1");
        tableQuery2.addScanRangeEndsWith("2");
        assertEquals(1, tableQuery.getObTableQuery().getKeyRanges().size());
        assertEquals(2, tableQuery2.getObTableQuery().getKeyRanges().size());

        tableQuery.scanOrder(true);
        tableQuery.indexName("test");
        tableQuery.primaryIndex();
        tableQuery.filterString("111");
        tableQuery.setHTableFilter(new ObHTableFilter());
        tableQuery.limit(10);
        tableQuery.limit(10, 10);
        tableQuery.setMaxResultSize(100000);

        try {
            tableQuery.addScanRange(new Object[] { "1" }, new Object[] { "3" }).setKeys("c1", "c1");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.setKeys("c1", "c3").select("c2", "c1");
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        ObTable obTable = new ObTable();
        try {
            tableQuery.executeInit(new ObPair<Long, ObTableParam>(0L, new ObTableParam(obTable)));
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }

        try {
            tableQuery.executeNext(new ObPair<Long, ObTableParam>(0L, new ObTableParam(obTable)));
            fail();
        } catch (Exception e) {
            assertTrue(true);
        }
        tableQuery.clear();
    }

    @Test
    public void test_query() throws Exception {
        /*
        * CREATE TABLE `test_varchar_table` (
            `c1` varchar(20) NOT NULL,
             `c2` varchar(20) DEFAULT NULL,
            PRIMARY KEY (`c1`)
            )*/
        try {
            client.delete("test_varchar_table", "125");
            client.insert("test_varchar_table", "125", new String[] { "c2" },
                new Object[] { "123c1" });
            TableQuery tableQuery = client.query("test_varchar_table");
            QueryResultSet resultSet = tableQuery.select("c2").primaryIndex()
                .addScanRange("123", "567").execute();
            assertEquals(1, resultSet.cacheSize());
            client.delete("test_varchar_table", "125");

            client.insert("test_varchar_table", "123", new String[] { "c2" },
                new Object[] { "123c2" });
            client.insert("test_varchar_table", "124", new String[] { "c2" },
                new Object[] { "124c2" });
            client.insert("test_varchar_table", "234", new String[] { "c2" },
                new Object[] { "234c2" });
            client.insert("test_varchar_table", "456", new String[] { "c2" },
                new Object[] { "456c2" });
            client.insert("test_varchar_table", "567", new String[] { "c2" },
                new Object[] { "567c2" });

            tableQuery = client.query("test_varchar_table");

            ObTable obTable = new ObTable();
            try {
                tableQuery
                    .executeInit(new ObPair<Long, ObTableParam>(0L, new ObTableParam(obTable)));
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }

            try {
                tableQuery
                    .executeNext(new ObPair<Long, ObTableParam>(0L, new ObTableParam(obTable)));
                fail();
            } catch (Exception e) {
                assertTrue(true);
            }
            tableQuery.setMaxResultSize(100000);
            tableQuery.clear();

            // >= 123 && <= 567
            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("123", "567").execute();
            assertEquals(5, resultSet.cacheSize());
            for (int i = 0; i < 5; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("123c2", value.get("c2"));
                        break;
                    case 1:
                        assertEquals("124c2", value.get("c2"));
                        break;
                    case 2:
                        assertEquals("234c2", value.get("c2"));
                        break;
                    case 3:
                        assertEquals("456c2", value.get("c2"));
                        break;
                    case 4:
                        assertEquals("567c2", value.get("c2"));
                        break;
                }
            }

            // >= 123 && <= 123
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("123", "123").execute();
            assertEquals(1, resultSet.cacheSize());
            Assert.assertTrue(resultSet.next());
            Map<String, Object> v = resultSet.getRow();
            assertEquals("123c2", v.get("c2"));

            // >= 124 && <= 456
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("124", "456").execute();
            assertEquals(3, resultSet.cacheSize());
            for (int i = 0; i < 3; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("124c2", value.get("c2"));
                        break;
                    case 1:
                        assertEquals("234c2", value.get("c2"));
                        break;
                    case 2:
                        assertEquals("456c2", value.get("c2"));
                        break;
                }
            }

            // > 123 && < 567
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex()
                .addScanRange(new Object[] { "123" }, false, new Object[] { "567" }, false)
                .execute();
            assertEquals(3, resultSet.cacheSize());
            for (int i = 0; i < 3; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("124c2", value.get("c2"));
                        break;
                    case 1:
                        assertEquals("234c2", value.get("c2"));
                        break;
                    case 2:
                        assertEquals("456c2", value.get("c2"));
                        break;
                }
            }

            // > 123 && <= 567
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex()
                .addScanRange(new Object[] { "123" }, false, new Object[] { "567" }, true)
                .execute();
            assertEquals(4, resultSet.cacheSize());
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("124c2", value.get("c2"));
                        break;
                    case 1:
                        assertEquals("234c2", value.get("c2"));
                        break;
                    case 2:
                        assertEquals("456c2", value.get("c2"));
                        break;
                    case 3:
                        assertEquals("567c2", value.get("c2"));
                        break;
                }
            }

            // >= 123 && < 567
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex()
                .addScanRange(new Object[] { "123" }, true, new Object[] { "567" }, false)
                .execute();
            assertEquals(4, resultSet.cacheSize());
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("123c2", value.get("c2"));
                        break;
                    case 1:
                        assertEquals("124c2", value.get("c2"));
                        break;
                    case 2:
                        assertEquals("234c2", value.get("c2"));
                        break;
                    case 3:
                        assertEquals("456c2", value.get("c2"));
                        break;
                }
            }

            // >= 12 && <= 126
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("12", "126").execute();
            assertEquals(2, resultSet.cacheSize());
            for (int i = 0; i < 2; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("123c2", value.get("c2"));
                        break;
                    case 1:
                        assertEquals("124c2", value.get("c2"));
                        break;
                }
            }

            // (>=12 && <=126) || (>="456" && <="567")
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("12", "126")
                .addScanRange("456", "567").execute();
            assertEquals(4, resultSet.cacheSize());
            for (int i = 0; i < 4; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("123c2", value.get("c2"));
                        break;
                    case 1:
                        assertEquals("124c2", value.get("c2"));
                        break;
                    case 2:
                        assertEquals("456c2", value.get("c2"));
                        break;
                    case 3:
                        assertEquals("567c2", value.get("c2"));
                        break;
                }
            }

            // (>=124 && <=124)
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("124", "124").execute();
            assertEquals(1, resultSet.cacheSize());
            for (int i = 0; i < 1; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("124c2", value.get("c2"));
                        break;
                }
            }

            // (>=124 && <=123)
            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("124", "123").execute();
            assertEquals(0, resultSet.cacheSize());

            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("12", "126")
                .addScanRange("456", "567").execute();
            assertEquals(4, resultSet.cacheSize());
            for (int i = 0; i < 1; i++) {
                Assert.assertTrue(resultSet.next());
                Map<String, Object> value = resultSet.getRow();
                switch (i) {
                    case 0:
                        assertEquals("123c2", value.get("c2"));
                        break;
                }
            }
            resultSet.close();

            try {
                resultSet.next();
                fail();
            } catch (IllegalStateException e) {
                Assert.assertTrue(e.getMessage().contains("closed"));
            }

            // TODO: add test to check query timeout

            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().addScanRange("12", "126")
                .addScanRange("456", "567").setOperationTimeout(3000).execute();
            assertEquals(4, resultSet.cacheSize());
            Assert.assertTrue(resultSet.next());
            Thread.sleep(2000);
            resultSet.next();
            resultSet.close();

            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().scanOrder(true)
                .addScanRangeStartsWith(new Object[] { "12" }).setOperationTimeout(3000).execute();
            assertEquals(5, resultSet.cacheSize());
            Assert.assertTrue(resultSet.next());
            Thread.sleep(2000);
            resultSet.next();
            resultSet.close();

            tableQuery.clear();
            resultSet = tableQuery.select("c2").primaryIndex().scanOrder(true)
                .addScanRangeEndsWith(new Object[] { "126" }).setOperationTimeout(3000).execute();
            assertEquals(2, resultSet.cacheSize());
            Assert.assertTrue(resultSet.next());
            Thread.sleep(2000);
            resultSet.next();
            resultSet.close();

            tableQuery.clear();
            try {
                tableQuery.select("c2").addScanRange("12", "126").addScanRange("456", "567")
                    .setBatchSize(1).execute();
            } catch (Exception e) {
                if (e instanceof ObTableException) {
                    assertTrue(true);
                } else {
                    System.out.println("Wrong Exception: " + e);
                    fail();
                }
            }
        } finally {
            client.delete("test_varchar_table", "123");
            client.delete("test_varchar_table", "124");
            client.delete("test_varchar_table", "125");
            client.delete("test_varchar_table", "234");
            client.delete("test_varchar_table", "456");
            client.delete("test_varchar_table", "567");
        }
    }

    private void test_varchar_get_helper(ObTableClient obTableClient, String key, String value)
                                                                                               throws Exception {
        Map<String, Object> values = obTableClient.get("test_varchar_table", key,
            new String[] { "c2" });
        assertNotNull(values);
        assertEquals(value, values.get("c2"));
    }

    private void test_varchar_insert_helper(ObTableClient obTableClient, String key, String value)
                                                                                                  throws Exception {
        long affectedRows = obTableClient.insert("test_varchar_table", key, new String[] { "c2" },
            new String[] { value });
        assertEquals(1L, affectedRows);
    }

    public void test_varchar_helper(ObTableClient obTableClient, String prefix, int count)
                                                                                          throws Exception {
        String keyPrefix = "K" + prefix;
        String valPrefix = "V" + prefix;
        try {
            for (int i = 0; i < count; i++) {
                test_varchar_insert_helper(obTableClient, keyPrefix + i, valPrefix + i);
            }
            for (int i = 0; i < count; i++) {
                test_varchar_get_helper(obTableClient, keyPrefix + i, valPrefix + i);
            }
        } finally {
            for (int i = 0; i < count; i++) {
                long affectedRows = obTableClient.delete("test_varchar_table", keyPrefix + i);
                assertEquals(1, affectedRows);
            }
        }
    }

    public void test_varchar_helper_thread(final ObTableClient obTableClient, final String prefix,
                                           final int count) throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    long start = System.currentTimeMillis();
                    test_varchar_helper(obTableClient, prefix, count);
                    System.err.println("cost: " + (System.currentTimeMillis() - start));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public void syncRefreshMetaHelper(final ObTableClient obTableClient) {
        if (obTableClient.isOdpMode()) {
            // do noting
        } else {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        try {
                            obTableClient.syncRefreshMetadata();
                        } catch (Exception e) {
                            e.printStackTrace();
                            Assert.fail();
                        }
                    }
                }
            }).start();
        }
    }
}

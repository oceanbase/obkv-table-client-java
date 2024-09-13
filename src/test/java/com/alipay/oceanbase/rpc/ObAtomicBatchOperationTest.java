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

import com.alipay.oceanbase.rpc.exception.ObTableDuplicateKeyException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Delete;
import com.alipay.oceanbase.rpc.mutation.Insert;
import com.alipay.oceanbase.rpc.mutation.Put;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

public class ObAtomicBatchOperationTest {
    private static final int    dataSetSize = 4;
    private static final String successKey  = "abc-5";
    private static final String failedKey   = "abc-7";

    protected ObTableClient     obTableClient;

    @Before
    public void setup() throws Exception {

        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty("connectTimeout", "100000");
        obTableClient.addProperty("socketTimeout", "100000");
        obTableClient.addProperty("table.connection.pool.size", "3");
        obTableClient.init();

        this.obTableClient = obTableClient;

        for (int i = 0; i < dataSetSize; i++) {
            String key = "abc-" + i;
            String val = "xyz-" + i;
            this.obTableClient.insert("test_varchar_table", key, new String[] { "c2" },
                new String[] { val });
        }
    }

    @After
    public void teardown() throws Exception {
        for (int i = 0; i < dataSetSize; i++) {
            String key = "abc-" + i;
            obTableClient.delete("test_varchar_table", key);
        }
        obTableClient.delete("test_varchar_table", successKey);
        for (int i = 1; i <= 9; i++) {
            String key = "abcd-" + i;
            obTableClient.delete("test_varchar_table", key);
        }
        obTableClient.close();
    }

    @Test
    public void testAtomic() {
        TableBatchOps batchOps = obTableClient.batch("test_varchar_table");
        // default: no atomic batch operation
        try {
            batchOps.clear();
            batchOps.insert("abc-1", new String[] { "c2" }, new String[] { "bar-1" });
            batchOps.get("abc-2", new String[] { "c2" });
            batchOps.insert("abc-3", new String[] { "c2" }, new String[] { "bar-3" });
            batchOps.insert(successKey, new String[] { "c2" }, new String[] { "bar-5" });
            List<Object> results = batchOps.execute();
            Assert.assertTrue(results.get(0) instanceof ObTableException);
            Assert.assertEquals(((Map) results.get(1)).get("c2"), "xyz-2");
            Assert.assertTrue(results.get(2) instanceof ObTableException);
            Assert.assertEquals(results.get(3), 1L);
        } catch (Exception ex) {
            Assert.fail("hit exception:" + ex);
        }

        // atomic batch operation
        try {
            batchOps.clear();
            batchOps.setAtomicOperation(true);
            batchOps.insert("abc-1", new String[] { "c2" }, new String[] { "bar-1" });
            batchOps.get("abc-2", new String[] { "c2" });
            batchOps.insert("abc-3", new String[] { "c2" }, new String[] { "bar-3" });
            batchOps.insert(failedKey, new String[] { "c2" }, new String[] { "bar-5" });
            batchOps.execute();
            // no support atomic batch
            // Assert.fail("expect duplicate key exception.");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof ObTableException);
        }
    }

    @Test
    public void testBatchOperation() {
        TableBatchOps batchOps = obTableClient.batch("test_varchar_table");
        try {
            // 测试 isReadOnly: false, isSameType: false, isSamePropertiesNames: false
            {
                batchOps.clear();
                batchOps.insert("abc-1", new String[] { "c1", "c2" }, new String[] { "bar-1",
                        "bar-2" });
                batchOps.get("abc-2", new String[] { "c2" });
                batchOps.insert("abc-3", new String[] { "c2" }, new String[] { "bar-3" });

                Assert.assertFalse(batchOps.getObTableBatchOperation().isReadOnly());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSameType());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSamePropertiesNames());
            }
            // 测试 isReadOnly: true, isSameType: true, isSamePropertiesNames: false
            {
                batchOps.clear();
                batchOps.get("abc-2", new String[] { "c1", "c2", "c3" });
                batchOps.get("abc-3", new String[] { "c1", "c2", "c4" });
                batchOps.get("abc-4", new String[] { "c1", "c2" });

                Assert.assertTrue(batchOps.getObTableBatchOperation().isReadOnly());
                Assert.assertTrue(batchOps.getObTableBatchOperation().isSameType());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSamePropertiesNames());

                batchOps.clear();
                batchOps.get("abc-2", new String[] { "c1", "c2", "c3" });
                batchOps.get("abc-3", new String[] { "c1", "c2", "c4" });
                batchOps.get("abc-4", new String[] { "c1", "c2", "c3" });

                Assert.assertTrue(batchOps.getObTableBatchOperation().isReadOnly());
                Assert.assertTrue(batchOps.getObTableBatchOperation().isSameType());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSamePropertiesNames());
            }
            // 测试 isReadOnly: true, isSameType: true, isSamePropertiesNames: true
            {
                batchOps.clear();
                batchOps.get("abc-2", new String[] { "c1", "c2", "c3" });
                batchOps.get("abc-3", new String[] { "c1", "c2", "c3" });
                batchOps.get("abc-4", new String[] { "c1", "c2", "c3" });

                Assert.assertTrue(batchOps.getObTableBatchOperation().isReadOnly());
                Assert.assertTrue(batchOps.getObTableBatchOperation().isSameType());
                Assert.assertTrue(batchOps.getObTableBatchOperation().isSamePropertiesNames());
            }
            // 测试 isReadOnly: false, isSameType: false, isSamePropertiesNames: true
            {
                batchOps.clear();
                batchOps.get("abc-2", new String[] { "c1a", "c2", "c3" });
                batchOps.get("abc-3", new String[] { "c2", "c3", "C1A" });
                batchOps.get("abc-4", new String[] { "c1A", "c2", "c3" });
                batchOps.insert("abc-4", new String[] { "c3", "C2", "c1a" }, new String[] {
                        "bar-3", "bar-3", "bar-3" });
                batchOps.insert("abc-4", new String[] { "c1A", "c2", "C3" }, new String[] {
                        "bar-2", "bar-2", "bar-2" });

                Assert.assertFalse(batchOps.getObTableBatchOperation().isReadOnly());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSameType());
                Assert.assertTrue(batchOps.getObTableBatchOperation().isSamePropertiesNames());
            }
            // 测试 isReadOnly: false, isSameType: false, isSamePropertiesNames: false
            {
                batchOps.clear();
                batchOps.get("abc-2", new String[] { "c1", "c2", "c3" });
                batchOps.get("abc-3", new String[] { "c1", "c2", "c3" });
                batchOps.get("abc-4", new String[] { "c1", "c2" });
                batchOps.insert("abc-4", new String[] { "c1", "c2", "c3" }, new String[] { "bar-3",
                        "bar-3", "bar-3" });
                batchOps.insert("abc-4", new String[] { "c1", "c2", "c3" }, new String[] { "bar-2",
                        "bar-2", "bar-2" });

                Assert.assertFalse(batchOps.getObTableBatchOperation().isReadOnly());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSameType());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSamePropertiesNames());

                batchOps.clear();
                batchOps.get("abc-2", new String[] { "c1", "c2", "c3" });
                batchOps.get("abc-3", new String[] { "c1", "c4", "c3" });
                batchOps.insert("abc-4", new String[] { "c1", "c2", "c3" }, new String[] { "bar-3",
                        "bar-3", "bar-3" });
                batchOps.insert("abc-4", new String[] { "c2", "c3", "c1" }, new String[] { "bar-2",
                        "bar-2", "bar-2" });

                Assert.assertFalse(batchOps.getObTableBatchOperation().isReadOnly());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSameType());
                Assert.assertFalse(batchOps.getObTableBatchOperation().isSamePropertiesNames());
            }

        } catch (Exception ex) {
            Assert.fail("hit exception:" + ex);
        }

    }

    @Test
    public void testReturnOneRes() {
        Assume.assumeTrue("Skipping returnOneResult when ob version not support",
            ObGlobal.isReturnOneResultSupport());
        TableBatchOps batchOps = obTableClient.batch("test_varchar_table");
        // no atomic ReturnOneRes batch operation
        try {
            batchOps.clear();
            batchOps.setAtomicOperation(false);
            batchOps.setReturnOneResult(true);
            batchOps.insert("abcd-7", new String[] { "c2" }, new String[] { "returnOne-7" });
            batchOps.insert("abcd-8", new String[] { "c2" }, new String[] { "returnOne-8" });
            batchOps.insert("abcd-9", new String[] { "c2" }, new String[] { "returnOne-9" });
            List<Object> results = batchOps.execute();
            Assert.assertEquals(results.size(), 1);
            Assert.assertEquals(results.get(0), 3L);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof ObTableException);
        }

        // atomic ReturnOneRes batch operation
        try {
            batchOps.clear();
            batchOps.setAtomicOperation(true);
            batchOps.setReturnOneResult(true);
            batchOps.insert("abcd-4", new String[] { "c2" }, new String[] { "returnOne-4" });
            batchOps.insert("abcd-5", new String[] { "c2" }, new String[] { "returnOne-5" });
            batchOps.insert("abcd-6", new String[] { "c2" }, new String[] { "returnOne-6" });
            List<Object> results = batchOps.execute();
            Assert.assertEquals(results.size(), 1);
            Assert.assertEquals(results.get(0), 3L);
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof ObTableException);
        }

        // batch delete operation
        try {
            // del
            batchOps.clear();
            batchOps.setReturnOneResult(true);
            batchOps.setAtomicOperation(false);
            for (int i = 0; i <= 9; i += 2) {
                String key = "abc-" + i;
                batchOps.delete(key);
            }
            List<Object> results = batchOps.execute();
            Assert.assertEquals(results.size(), 1);
            Assert.assertEquals(results.get(0), 2L);
            // get
            batchOps.clear();
            batchOps.setAtomicOperation(true);
            for (int i = 0; i <= 9; i++) {
                String key = "abc-" + i;
                batchOps.get(key, new String[] { "c2" });
            }
            batchOps.setReturnOneResult(false);
            List<Object> results_get = batchOps.execute();

            Assert.assertTrue(((Map) results_get.get(0)).isEmpty());
            Assert.assertFalse(((Map) results_get.get(1)).isEmpty());
            Assert.assertTrue(((Map) results_get.get(2)).isEmpty());
            Assert.assertFalse(((Map) results_get.get(3)).isEmpty());
            Assert.assertTrue(((Map) results_get.get(4)).isEmpty());
            Assert.assertTrue(((Map) results_get.get(5)).isEmpty());

        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof ObTableException);
        }

        // default: no ReturnOneRes batch operation
        try {
            batchOps.clear();
            batchOps.setAtomicOperation(false);
            batchOps.setReturnOneResult(false);
            batchOps.insert("abcd-1", new String[] { "c2" }, new String[] { "returnOne-1" });
            batchOps.insert("abcd-2", new String[] { "c2" }, new String[] { "returnOne-2" });
            batchOps.insert("abcd-3", new String[] { "c2" }, new String[] { "returnOne-3" });
            List<Object> results = batchOps.execute();
            Assert.assertEquals(results.size(), 3);
            Assert.assertEquals(results.get(0), 1L);
            Assert.assertEquals(results.get(1), 1L);
            Assert.assertEquals(results.get(2), 1L);
        } catch (Exception ex) {
            Assert.fail("hit exception:" + ex);
        }
    }

    @Test
    public void testReturnOneResPartition() throws Exception {
        Assume.assumeTrue("Skiping returnOneResult when ob version not support",
            ObGlobal.isReturnOneResultSupport());
        BatchOperation batchOperation = obTableClient.batchOperation("test_mutation");
        Object values[][] = { { 1L, "c2_val", "c3_val", 100L }, { 200L, "c2_val", "c3_val", 100L },
                { 401L, "c2_val", "c3_val", 100L }, { 2000L, "c2_val", "c3_val", 100L },
                { 100001L, "c2_val", "c3_val", 100L }, { 10000002L, "c2_val", "c3_val", 100L }, };
        int rowCnt = values.length;
        try {
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                Insert insert = new Insert();
                insert.setRowKey(row(colVal("c1", curRow[0]), colVal("c2", curRow[1])));
                insert.addMutateRow(row(colVal("c3", curRow[2]), colVal("c4", curRow[3])));
                batchOperation.addOperation(insert);
            }
            batchOperation.setReturnOneResult(true);
            BatchOperationResult batchOperationResult = batchOperation.execute();
            Assert.assertEquals(batchOperationResult.size(), 1);
            Assert.assertEquals(batchOperationResult.get(0).getAffectedRows(), 6);
        } catch (Exception ex) {
            Assert.fail("hit exception:" + ex);
        } finally {
            for (int j = 0; j < rowCnt; j++) {
                Object[] curRow = values[j];
                obTableClient.delete("test_mutation", new Object[] { curRow[0], curRow[1] });

            }
        }
    }

    @Test
    public void testBatchGet() throws Exception {
        Assume.assumeTrue("Skipping returnOneResult when ob version not support",
            ObGlobal.isReturnOneResultSupport());
        try {
            {
                // insert
                TableBatchOps batchOps = obTableClient.batch("test_varchar_table");
                batchOps.clear();
                batchOps.insert("batch_get-1", new String[] { "c2" }, new String[] { "bar-1" });
                batchOps.insert("batch_get-2", new String[] { "c2" }, new String[] { "bar-2" });
                batchOps.insert("batch_get-3", new String[] { "c2" }, new String[] { "bar-3" });
                batchOps.insert("batch_get-4", new String[] { "c2" }, new String[] { "bar-4" });
                batchOps.setReturnOneResult(true);
                List<Object> results = batchOps.execute();
                Assert.assertEquals(results.get(0), 4L);
            }
            {
                // get
                TableBatchOps batchOps = obTableClient.batch("test_varchar_table");
                batchOps.clear();
                batchOps.get("batch_get-1", new String[] { "c2" });
                batchOps.get("batch_get-2", new String[] { "c2" });
                batchOps.get("batch_get-3", new String[] { "c2" });
                batchOps.get("batch_get-4", new String[] { "c2" });
                List<Object> results = batchOps.execute();
                Assert.assertEquals(results.size(), 4L);
                Assert.assertEquals(((Map) results.get(0)).get("c2"), "bar-1");
                Assert.assertEquals(((Map) results.get(1)).get("c2"), "bar-2");
                Assert.assertEquals(((Map) results.get(2)).get("c2"), "bar-3");
                Assert.assertEquals(((Map) results.get(3)).get("c2"), "bar-4");
            }
            {
                // get
                TableBatchOps batchOps = obTableClient.batch("test_varchar_table");
                batchOps.clear();
                batchOps.get("batch_get-1", new String[] { "c2" });
                batchOps.get("batch_get-2", new String[] { "c2" });
                batchOps.get("batch_get-1", new String[] { "c2" });
                batchOps.get("batch_get-2", new String[] { "c2" });
                List<Object> results = batchOps.execute();
                Assert.assertEquals(results.size(), 4L);
                Assert.assertEquals(((Map) results.get(0)).get("c2"), "bar-1");
                Assert.assertEquals(((Map) results.get(1)).get("c2"), "bar-2");
                Assert.assertEquals(((Map) results.get(2)).get("c2"), "bar-1");
                Assert.assertEquals(((Map) results.get(3)).get("c2"), "bar-2");
            }
        } catch (Exception ex) {
            Assert.fail("hit exception:" + ex);
        } finally {
            for (int i = 1; i <= 4; i++) {
                String key = "batch_get-" + i;
                obTableClient.delete("test_varchar_table", key);
            }
        }
    }
}

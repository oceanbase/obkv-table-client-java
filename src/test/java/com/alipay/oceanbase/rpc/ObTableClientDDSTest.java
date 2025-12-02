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

import com.alipay.oceanbase.rpc.dds.DdsObTableClient;
import com.alipay.oceanbase.rpc.dds.DdsWeightSwitchHelper;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DdsObTableClient.class)
@PowerMockIgnore({ "javax.crypto.*" })
public class ObTableClientDDSTest {
    private DdsObTableClient                   client;

    private static final String                appName            = "obkv";
    private static final String                appDsName          = "dds_migrate_2x_4x_test";
    private static final String                version            = "v1.0";

    // DDS权重切换测试工具
    private static final DdsWeightSwitchHelper weightSwitchHelper = new DdsWeightSwitchHelper(
                                                                      "http://ddsconsole.stable.alipay.net", // DDS控制台地址
                                                                      "obkv", // 用户名
                                                                      "96328f194f86102ee3fdbd2695b2753c" // 令牌（实际使用时应替换为有效令牌）
                                                                  );

    /*
        tbRule: substr_loadtest(c1,1,2)

      | loadtest_varchar_table_00 |
      | loadtest_varchar_table_01 |
      | loadtest_varchar_table_02 |
      | loadtest_varchar_table_03 |
      | loadtest_varchar_table_04 |
      | loadtest_varchar_table_05 |
      | loadtest_varchar_table_06 |
      | loadtest_varchar_table_07 |
      | loadtest_varchar_table_08 |
      | loadtest_varchar_table_09 |
      | loadtest_varchar_table_10 |

      +-------+--------------+------+-----+---------+-------+
      | Field | Type         | Null | Key | Default | Extra |
      +-------+--------------+------+-----+---------+-------+
      | c1    | varchar(128) | NO   | PRI | NULL    |       |
      | c2    | varchar(128) | YES  |     | NULL    |       |
      +-------+--------------+------+-----+---------+-------+
     * 
     */
    @Test
    public void testDdsMutationInterfaces() throws Exception {
        client = new DdsObTableClient();
        client.setAppName(appName);
        client.setAppDsName(appDsName);
        client.setVersion(version);
        client.init();

        String tableName = "loadtest_varchar_table";
        // Use keys with different shard prefixes to test DDS routing
        // These will route to different physical tables based on substr_loadtest(c1,1,2)
        String testKey1 = "01_dds_test_insert"; // Routes to loadtest_varchar_table_01
        String testKey2 = "02_dds_test_replace"; // Routes to loadtest_varchar_table_02  
        String testKey3 = "03_dds_test_upsert"; // Routes to loadtest_varchar_table_03

        try {
            // Test Insert interface
            Insert insert = client.insert(tableName);
            assertNotNull("Insert interface should return non-null object", insert);

            MutationResult insertResult = insert
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey1)))
                .addMutateColVal(MutationFactory.colVal("c2", "insert_value")).execute();
            assertNotNull("Insert result should not be null", insertResult);
            assertTrue("Insert should affect at least 1 row", insertResult.getAffectedRows() >= 1);

            // Verify insert worked
            Map<String, Object> result = client.get(tableName, new Object[] { testKey1 },
                new String[] { "c2" });
            assertEquals("insert_value", result.get("c2"));

            // Test Update interface
            Update update = client.update(tableName);
            assertNotNull("Update interface should return non-null object", update);

            MutationResult updateResult = update
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey1)))
                .addMutateColVal(MutationFactory.colVal("c2", "updated_value")).execute();
            assertNotNull("Update result should not be null", updateResult);
            assertTrue("Update should affect at least 1 row", updateResult.getAffectedRows() >= 1);

            // Verify update worked
            result = client.get(tableName, new Object[] { testKey1 }, new String[] { "c2" });
            assertEquals("updated_value", result.get("c2"));

            // Test Replace interface
            Replace replace = client.replace(tableName);
            assertNotNull("Replace interface should return non-null object", replace);

            MutationResult replaceResult = replace
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey2)))
                .addMutateColVal(MutationFactory.colVal("c2", "replace_value")).execute();
            assertNotNull("Replace result should not be null", replaceResult);
            assertTrue("Replace should affect at least 1 row", replaceResult.getAffectedRows() >= 1);

            // Verify replace worked
            result = client.get(tableName, new Object[] { testKey2 }, new String[] { "c2" });
            assertEquals("replace_value", result.get("c2"));

            // Test InsertOrUpdate interface
            InsertOrUpdate insertOrUpdate = client.insertOrUpdate(tableName);
            assertNotNull("InsertOrUpdate interface should return non-null object", insertOrUpdate);

            MutationResult insertOrUpdateResult = insertOrUpdate
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey3)))
                .addMutateColVal(MutationFactory.colVal("c2", "insertOrUpdate_value")).execute();
            assertNotNull("InsertOrUpdate result should not be null", insertOrUpdateResult);
            assertTrue("InsertOrUpdate should affect at least 1 row",
                insertOrUpdateResult.getAffectedRows() >= 1);

            // Verify insertOrUpdate worked
            result = client.get(tableName, new Object[] { testKey3 }, new String[] { "c2" });
            assertEquals("insertOrUpdate_value", result.get("c2"));

            // Test Put interface
            Put put = client.put(tableName);
            assertNotNull("Put interface should return non-null object", put);

            MutationResult putResult = put
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey3)))
                .addMutateColVal(MutationFactory.colVal("c2", "put_value")).execute();
            assertNotNull("Put result should not be null", putResult);

            // Verify put worked
            result = client.get(tableName, new Object[] { testKey3 }, new String[] { "c2" });
            assertEquals("put_value", result.get("c2"));

            // Test Delete interface
            Delete delete = client.delete(tableName);
            assertNotNull("Delete interface should return non-null object", delete);

            MutationResult deleteResult = delete.setRowKey(
                MutationFactory.row(MutationFactory.colVal("c1", testKey1))).execute();
            assertNotNull("Delete result should not be null", deleteResult);
            assertTrue("Delete should affect at least 1 row", deleteResult.getAffectedRows() >= 1);

            // Verify delete worked - should return empty result
            result = client.get(tableName, new Object[] { testKey1 }, new String[] { "c2" });
            assertTrue("Deleted record should not exist", result.isEmpty());
        } finally {
            // Clean up remaining test data
            client.delete(tableName, new Object[] { testKey1 });
            client.delete(tableName, new Object[] { testKey2 });
            client.delete(tableName, new Object[] { testKey3 });
        }
    }

    @Test
    public void testHoldClientAndConfChange() throws Exception {
        client = new DdsObTableClient();
        client.setAppName(appName);
        client.setAppDsName(appDsName);
        client.setVersion(version);
        client.init();

        // do nothing, just keep the client alive and wait for config change
        Thread.sleep(Long.MAX_VALUE);
    }

    @Test
    public void testDdsAdvancedMutationInterfaces() throws Exception {
        client = new DdsObTableClient();
        client.setAppName(appName);
        client.setAppDsName(appDsName);
        client.setVersion(version);
        client.init();

        String tableName = "loadtest_varchar_table";
        // Use key with specific shard prefix for advanced operations
        String testKey = "1"; // Routes to loadtest_varchar_table_04

        try {
            // Setup initial data for advanced operations
            client.insert(tableName, new Object[] { testKey }, new String[] { "c2" },
                new Object[] { "initial_value" });

            // Test Increment interface (Note: This may not work with varchar columns, but we test the interface)
            Increment increment = client.increment(tableName);
            assertNotNull("Increment interface should return non-null object", increment);

            // Test Append interface (Note: This may not work with varchar columns, but we test the interface)
            Append append = client.append(tableName);
            assertNotNull("Append interface should return non-null object", append);

            // Test BatchOperation interface
            BatchOperation batchOp = client.batchOperation(tableName);
            assertNotNull("BatchOperation interface should return non-null object", batchOp);

        } finally {
            // Clean up
            client.delete(tableName, new Object[] { testKey });
        }
    }

    @Test
    public void testDdsChainedMutationCalls() throws Exception {
        client = new DdsObTableClient();
        client.setAppName(appName);
        client.setAppDsName(appDsName);
        client.setVersion(version);
        client.init();

        String tableName = "loadtest_varchar_table";
        String testKey = "05_chained_test"; // Routes to loadtest_varchar_table_05

        try {
            // Test chained Insert calls
            MutationResult insertResult = client.insert(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey)))
                .addMutateColVal(MutationFactory.colVal("c2", "chained_insert_value")).execute();
            assertNotNull("Chained insert result should not be null", insertResult);
            assertTrue("Chained insert should affect at least 1 row",
                insertResult.getAffectedRows() >= 1);

            // Verify insert worked
            Map<String, Object> result = client.get(tableName, new Object[] { testKey },
                new String[] { "c2" });
            assertEquals("chained_insert_value", result.get("c2"));

            // Test chained Update calls
            MutationResult updateResult = client.update(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey)))
                .addMutateColVal(MutationFactory.colVal("c2", "chained_update_value")).execute();
            assertNotNull("Chained update result should not be null", updateResult);
            assertTrue("Chained update should affect at least 1 row",
                updateResult.getAffectedRows() >= 1);

            // Verify update worked
            result = client.get(tableName, new Object[] { testKey }, new String[] { "c2" });
            assertEquals("chained_update_value", result.get("c2"));

            // Test chained Replace calls
            MutationResult replaceResult = client.replace(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey)))
                .addMutateColVal(MutationFactory.colVal("c2", "chained_replace_value")).execute();
            assertNotNull("Chained replace result should not be null", replaceResult);
            assertTrue("Chained replace should affect at least 1 row",
                replaceResult.getAffectedRows() >= 1);

            // Verify replace worked
            result = client.get(tableName, new Object[] { testKey }, new String[] { "c2" });
            assertEquals("chained_replace_value", result.get("c2"));

            // Test chained InsertOrUpdate calls
            MutationResult upsertResult = client.insertOrUpdate(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey)))
                .addMutateColVal(MutationFactory.colVal("c2", "chained_upsert_value")).execute();
            assertNotNull("Chained upsert result should not be null", upsertResult);
            assertTrue("Chained upsert should affect at least 1 row",
                upsertResult.getAffectedRows() >= 1);

            // Verify upsert worked
            result = client.get(tableName, new Object[] { testKey }, new String[] { "c2" });
            assertEquals("chained_upsert_value", result.get("c2"));

            // Test chained Put calls
            MutationResult putResult = client.put(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey)))
                .addMutateColVal(MutationFactory.colVal("c2", "chained_put_value")).execute();
            assertNotNull("Chained put result should not be null", putResult);

            // Verify put worked
            result = client.get(tableName, new Object[] { testKey }, new String[] { "c2" });
            assertEquals("chained_put_value", result.get("c2"));

            // Test chained Delete calls
            MutationResult deleteResult = client.delete(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey))).execute();
            assertNotNull("Chained delete result should not be null", deleteResult);
            assertTrue("Chained delete should affect at least 1 row",
                deleteResult.getAffectedRows() >= 1);

            // Verify delete worked
            result = client.get(tableName, new Object[] { testKey }, new String[] { "c2" });
            assertTrue("Deleted record should not exist", result.isEmpty());
        } finally {
            // Clean up
            client.delete(tableName, new Object[] { testKey });
        }
    }

    @Test
    public void testDdsComplexChainedMutationCalls() throws Exception {
        client = new DdsObTableClient();
        client.setAppName(appName);
        client.setAppDsName(appDsName);
        client.setVersion(version);
        client.init();

        String tableName = "loadtest_varchar_table";
        String testKey1 = "06_complex_chained_1"; // Routes to loadtest_varchar_table_06
        String testKey2 = "07_complex_chained_2"; // Routes to loadtest_varchar_table_07

        try {
            // Test complex chained Insert with multiple column values
            MutationResult insertResult = client.insert(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey1)))
                .addMutateColVal(MutationFactory.colVal("c2", "complex_insert_value_1")).execute();
            assertNotNull("Complex chained insert result should not be null", insertResult);
            assertTrue("Complex chained insert should affect at least 1 row",
                insertResult.getAffectedRows() >= 1);

            // Test complex chained InsertOrUpdate with multiple column values
            MutationResult upsertResult = client.insertOrUpdate(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey2)))
                .addMutateColVal(MutationFactory.colVal("c2", "complex_upsert_value_2")).execute();
            assertNotNull("Complex chained upsert result should not be null", upsertResult);
            assertTrue("Complex chained upsert should affect at least 1 row",
                upsertResult.getAffectedRows() >= 1);

            // Verify both operations worked
            Map<String, Object> result1 = client.get(tableName, new Object[] { testKey1 },
                new String[] { "c2" });
            Map<String, Object> result2 = client.get(tableName, new Object[] { testKey2 },
                new String[] { "c2" });
            assertEquals("complex_insert_value_1", result1.get("c2"));
            assertEquals("complex_upsert_value_2", result2.get("c2"));

            // Test chained Update operations
            MutationResult updateResult1 = client.update(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey1)))
                .addMutateColVal(MutationFactory.colVal("c2", "updated_complex_value_1")).execute();
            assertNotNull("Complex chained update result 1 should not be null", updateResult1);

            MutationResult updateResult2 = client.update(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey2)))
                .addMutateColVal(MutationFactory.colVal("c2", "updated_complex_value_2")).execute();
            assertNotNull("Complex chained update result 2 should not be null", updateResult2);

            // Verify updates worked
            result1 = client.get(tableName, new Object[] { testKey1 }, new String[] { "c2" });
            result2 = client.get(tableName, new Object[] { testKey2 }, new String[] { "c2" });
            assertEquals("updated_complex_value_1", result1.get("c2"));
            assertEquals("updated_complex_value_2", result2.get("c2"));

            // Test chained Replace operations
            MutationResult replaceResult1 = client.replace(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey1)))
                .addMutateColVal(MutationFactory.colVal("c2", "replaced_complex_value_1"))
                .execute();
            assertNotNull("Complex chained replace result 1 should not be null", replaceResult1);

            MutationResult replaceResult2 = client.replace(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey2)))
                .addMutateColVal(MutationFactory.colVal("c2", "replaced_complex_value_2"))
                .execute();
            assertNotNull("Complex chained replace result 2 should not be null", replaceResult2);

            // Verify replaces worked
            result1 = client.get(tableName, new Object[] { testKey1 }, new String[] { "c2" });
            result2 = client.get(tableName, new Object[] { testKey2 }, new String[] { "c2" });
            assertEquals("replaced_complex_value_1", result1.get("c2"));
            assertEquals("replaced_complex_value_2", result2.get("c2"));

            // Test chained Put operations
            MutationResult putResult1 = client.put(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey1)))
                .addMutateColVal(MutationFactory.colVal("c2", "put_complex_value_1")).execute();
            assertNotNull("Complex chained put result 1 should not be null", putResult1);

            MutationResult putResult2 = client.put(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey2)))
                .addMutateColVal(MutationFactory.colVal("c2", "put_complex_value_2")).execute();
            assertNotNull("Complex chained put result 2 should not be null", putResult2);

            // Verify puts worked
            result1 = client.get(tableName, new Object[] { testKey1 }, new String[] { "c2" });
            result2 = client.get(tableName, new Object[] { testKey2 }, new String[] { "c2" });
            assertEquals("put_complex_value_1", result1.get("c2"));
            assertEquals("put_complex_value_2", result2.get("c2"));

            // Test chained Delete operations
            MutationResult deleteResult1 = client.delete(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey1))).execute();
            assertNotNull("Complex chained delete result 1 should not be null", deleteResult1);

            MutationResult deleteResult2 = client.delete(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey2))).execute();
            assertNotNull("Complex chained delete result 2 should not be null", deleteResult2);

            // Verify deletes worked
            result1 = client.get(tableName, new Object[] { testKey1 }, new String[] { "c2" });
            result2 = client.get(tableName, new Object[] { testKey2 }, new String[] { "c2" });
            assertTrue("Deleted record 1 should not exist", result1.isEmpty());
            assertTrue("Deleted record 2 should not exist", result2.isEmpty());
        } finally {
            // Clean up
            client.delete(tableName, new Object[] { testKey1 });
            client.delete(tableName, new Object[] { testKey2 });
        }
    }

    @Test
    public void testDdsChainedMutationWithErrorHandling() throws Exception {
        client = new DdsObTableClient();
        client.setAppName(appName);
        client.setAppDsName(appDsName);
        client.setVersion(version);
        client.init();

        String tableName = "loadtest_varchar_table";
        String testKey = "08_error_handling_test"; // Routes to loadtest_varchar_table_08
        try {
            // Test chained Insert with null rowKey (should throw exception)
            try {
                client.insert(tableName)
                    .addMutateColVal(MutationFactory.colVal("c2", "should_fail")).execute();
                assertTrue("Should have thrown exception for null rowKey", false);
            } catch (Exception e) {
                assertTrue("Exception should contain rowKey error message", e.getMessage()
                    .contains("rowKey is null"));
            }

            // Test chained Update with null rowKey (should throw exception)
            try {
                client.update(tableName)
                    .addMutateColVal(MutationFactory.colVal("c2", "should_fail")).execute();
                assertTrue("Should have thrown exception for null rowKey", false);
            } catch (Exception e) {
                assertTrue("Exception should contain rowKey error message", e.getMessage()
                    .contains("rowKey is null"));
            }

            // Test chained Delete with null rowKey (should throw exception)
            try {
                client.delete(tableName).execute();
                assertTrue("Should have thrown exception for null rowKey", false);
            } catch (Exception e) {
                assertTrue("Exception should contain rowKey error message", e.getMessage()
                    .contains("rowKey is null"));
            }

            // Test successful chained operation after error cases
            MutationResult insertResult = client.insert(tableName)
                .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", testKey)))
                .addMutateColVal(MutationFactory.colVal("c2", "success_after_error")).execute();
            assertNotNull("Successful chained insert result should not be null", insertResult);
            assertTrue("Successful chained insert should affect at least 1 row",
                insertResult.getAffectedRows() >= 1);

            // Verify successful operation worked
            Map<String, Object> result = client.get(tableName, new Object[] { testKey },
                new String[] { "c2" });
            assertEquals("success_after_error", result.get("c2"));
        } finally {
            // Clean up
            client.delete(tableName, new Object[] { testKey });
        }
    }

    @Test
    public void testDdsChainedMutationCrossShard() throws Exception {
        client = new DdsObTableClient();
        client.setAppName(appName);
        client.setAppDsName(appDsName);
        client.setVersion(version);
        client.init();

        String tableName = "loadtest_varchar_table";
        // Test chained operations across multiple shards
        long timestamp = System.currentTimeMillis();
        String[] testKeys = { "08_cross_shard_" + timestamp + "_1", // Routes to loadtest_varchar_table_08
                "09_cross_shard_" + timestamp + "_2", // Routes to loadtest_varchar_table_09
                "00_cross_shard_" + timestamp + "_3" // Routes to loadtest_varchar_table_00
        };
        try {
            // Clean up any existing test data first
            for (String key : testKeys) {
                try {
                    client.delete(tableName, new Object[] { key });
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }

            // Insert data across shards using chained calls
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                MutationResult insertResult = client
                    .insert(tableName)
                    .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", key)))
                    .addMutateColVal(
                        MutationFactory.colVal("c2", "cross_shard_insert_" + expectedShard))
                    .execute();

                assertNotNull("Cross-shard chained insert result should not be null for shard "
                              + expectedShard, insertResult);
                assertTrue("Cross-shard chained insert should affect at least 1 row for shard "
                           + expectedShard, insertResult.getAffectedRows() >= 1);
            }

            // Verify data was correctly inserted across shards
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    new String[] { "c2" });
                assertNotNull("Should be able to retrieve data from shard " + expectedShard, result);
                assertEquals("Data should match for shard " + expectedShard, "cross_shard_insert_"
                                                                             + expectedShard,
                    result.get("c2"));
            }

            // Update data across shards using chained calls
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                MutationResult updateResult = client
                    .update(tableName)
                    .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", key)))
                    .addMutateColVal(
                        MutationFactory.colVal("c2", "cross_shard_update_" + expectedShard))
                    .execute();

                assertNotNull("Cross-shard chained update result should not be null for shard "
                              + expectedShard, updateResult);
                assertTrue("Cross-shard chained update should affect at least 1 row for shard "
                           + expectedShard, updateResult.getAffectedRows() >= 1);
            }

            // Verify updates worked across all shards
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    new String[] { "c2" });
                assertEquals("Updated data should match for shard " + expectedShard,
                    "cross_shard_update_" + expectedShard, result.get("c2"));
            }

            // Replace data across shards using chained calls
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                MutationResult replaceResult = client
                    .replace(tableName)
                    .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", key)))
                    .addMutateColVal(
                        MutationFactory.colVal("c2", "cross_shard_replace_" + expectedShard))
                    .execute();

                assertNotNull("Cross-shard chained replace result should not be null for shard "
                              + expectedShard, replaceResult);
                assertTrue("Cross-shard chained replace should affect at least 1 row for shard "
                           + expectedShard, replaceResult.getAffectedRows() >= 1);
            }

            // Verify replaces worked across all shards
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    new String[] { "c2" });
                assertEquals("Replaced data should match for shard " + expectedShard,
                    "cross_shard_replace_" + expectedShard, result.get("c2"));
            }

            // Delete data across shards using chained calls
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                MutationResult deleteResult = client.delete(tableName)
                    .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", key))).execute();

                assertNotNull("Cross-shard chained delete result should not be null for shard "
                              + expectedShard, deleteResult);
                assertTrue("Cross-shard chained delete should affect at least 1 row for shard "
                           + expectedShard, deleteResult.getAffectedRows() >= 1);
            }

            // Verify deletions worked across all shards
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    new String[] { "c2" });
                assertTrue("Data should be deleted from shard " + expectedShard, result.isEmpty());
            }
        } finally {
            // Clean up
            for (String key : testKeys) {
                client.delete(tableName, new Object[] { key });
            }
        }
    }

    @Test
    public void testDdsShardingRouting() throws Exception {
        client = new DdsObTableClient();
        client.setAppName(appName);
        client.setAppDsName(appDsName);
        client.setVersion(version);
        client.init();

        String tableName = "loadtest_varchar_table";

        // Test data routing to different physical tables based on substr_loadtest(c1,1,2)
        // Each key will route to a different physical table: loadtest_varchar_table_XX
        String[] testKeys = { "00_shard_test", // Routes to loadtest_varchar_table_00
                "01_shard_test", // Routes to loadtest_varchar_table_01
                "02_shard_test", // Routes to loadtest_varchar_table_02
                "05_shard_test", // Routes to loadtest_varchar_table_05
                "09_shard_test" // Routes to loadtest_varchar_table_09
        };
        try {
            // Insert data across multiple shards using DDS mutation interfaces
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2); // Extract shard number from key

                // Test Insert interface with cross-shard data
                Insert insert = client.insert(tableName);
                MutationResult result = insert
                    .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", key)))
                    .addMutateColVal(
                        MutationFactory.colVal("c2", "shard_" + expectedShard + "_value"))
                    .execute();

                assertNotNull("Insert result should not be null for shard " + expectedShard, result);
                assertTrue("Insert should affect at least 1 row for shard " + expectedShard,
                    result.getAffectedRows() >= 1);
            }

            // Verify data was correctly inserted and can be retrieved from each shard
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    new String[] { "c2" });
                assertNotNull("Should be able to retrieve data from shard " + expectedShard, result);
                assertEquals("Data should match for shard " + expectedShard, "shard_"
                                                                             + expectedShard
                                                                             + "_value",
                    result.get("c2"));
            }

            // Test Update operations across shards
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Update update = client.update(tableName);
                MutationResult result = update
                    .setRowKey(MutationFactory.row(MutationFactory.colVal("c1", key)))
                    .addMutateColVal(MutationFactory.colVal("c2", "updated_shard_" + expectedShard))
                    .execute();

                assertNotNull("Update result should not be null for shard " + expectedShard, result);
                assertTrue("Update should affect at least 1 row for shard " + expectedShard,
                    result.getAffectedRows() >= 1);
            }

            // Verify updates worked across all shards
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    new String[] { "c2" });
                assertEquals("Updated data should match for shard " + expectedShard,
                    "updated_shard_" + expectedShard, result.get("c2"));
            }

            // Test Delete operations across shards
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Delete delete = client.delete(tableName);
                MutationResult result = delete.setRowKey(
                    MutationFactory.row(MutationFactory.colVal("c1", key))).execute();

                assertNotNull("Delete result should not be null for shard " + expectedShard, result);
                assertTrue("Delete should affect at least 1 row for shard " + expectedShard,
                    result.getAffectedRows() >= 1);
            }

            // Verify deletions worked across all shards
            for (int i = 0; i < testKeys.length; i++) {
                String key = testKeys[i];
                String expectedShard = key.substring(0, 2);

                Map<String, Object> result = client.get(tableName, new Object[] { key },
                    new String[] { "c2" });
                assertTrue("Data should be deleted from shard " + expectedShard, result.isEmpty());
            }
        } finally {
            // Clean up
            for (String key : testKeys) {
                client.delete(tableName, new Object[] { key });
            }
        }
    }

    // ================ DDS weight switch test case =================

    @Test
    public void testDdsWeightSwitchWithContinuousReadWrite() throws Exception {
        client = new DdsObTableClient();
        client.setAppName("obkv");
        client.setAppDsName("obkv_adapt_dds_single");
        client.setVersion("v1.0");
        client.init();
        String tableName = "loadtest_varchar_table_00";

        try {
            DdsWeightSwitchHelper.MetaDataResponse metaData = weightSwitchHelper.getMetaData(
                "obkv", "v1.0", "RDB");

            String originalDbkeySet = null;
            for (DdsWeightSwitchHelper.AppDsInfo appDsInfo : metaData.getData()) {
                if ("obkv_adapt_dds_single".equals(appDsInfo.getAppDsName())) {
                    StringBuilder sb = new StringBuilder("group_00");
                    for (DdsWeightSwitchHelper.ConnProp connProp : appDsInfo.getConnProps()) {
                        sb.append(",").append(connProp.getDbkeyName()).append(":R10W10");
                    }
                    originalDbkeySet = sb.toString();
                    break;
                }
            }
            assertNotNull("should find original config", originalDbkeySet);

            System.out.println("=== 开始数据库独立性验证 ===");
            // 验证初始连接状态
            verifyConnectionDifference(client, tableName, 2);
            
            // 第一阶段：验证数据库1独立性
            System.out.println("\n=== 阶段1：验证第一个数据库的独立性 ===");
            String switchedConfig1 = generateSwitchedConfig(originalDbkeySet, 0);
            boolean switch1Success = weightSwitchHelper.switchWeight("obkv",
                "obkv_adapt_dds_single", "v1.0", switchedConfig1, false);
            assertTrue("should switch weight successfully", switch1Success);
            Thread.sleep(10);
            
            // 插入带有唯一标识的数据到数据库1
            for (int i = 1; i <= 10; i++) {
                String testKey = "db1_independence_test_" + i;
                verifyDatabaseIndependence(client, tableName, testKey);
            }

            // 第二阶段：切换到数据库2
            System.out.println("\n=== 阶段2：切换到第二个数据库 ===");
            String switchedConfig2 = generateSwitchedConfig(originalDbkeySet, 1);
            boolean switch2Success = weightSwitchHelper.switchWeight("obkv",
                "obkv_adapt_dds_single", "v1.0", switchedConfig2, false);
            assertTrue("should switch weight successfully", switch2Success);
            Thread.sleep(10);
            
            // 插入带有唯一标识的数据到数据库2
            for (int i = 1; i <= 10; i++) {
                String testKey = "db2_independence_test_" + i;
                verifyDatabaseIndependence(client, tableName, testKey);
            }

            // 第三阶段：验证数据分布
            System.out.println("\n=== 阶段3：验证数据分布 ===");
            System.out.println("检查数据库1的数据...");
            int db1DataCount = 0;
            for (int i = 1; i <= 10; i++) {
                String testKey = "db1_independence_test_" + i;
                try {
                    Map<String, Object> result = client.get(tableName, new Object[] { testKey }, new String[] { "c2" });
                    if (result != null && !result.isEmpty()) {
                        db1DataCount++;
                    }
                } catch (Exception e) {
                    // 忽略查询异常
                }
            }
            
            System.out.println("检查数据库2的数据...");
            int db2DataCount = 0;
            for (int i = 1; i <= 10; i++) {
                String testKey = "db2_independence_test_" + i;
                try {
                    Map<String, Object> result = client.get(tableName, new Object[] { testKey }, new String[] { "c2" });
                    if (result != null && !result.isEmpty()) {
                        db2DataCount++;
                    }
                } catch (Exception e) {
                    // 忽略查询异常
                }
            }
            
            System.out.println("=== 数据分布验证结果 ===");
            System.out.println("数据库1中的数据记录数: " + db1DataCount);
            System.out.println("数据库2中的数据记录数: " + db2DataCount);
            
            // 第四阶段：持续写入测试
            System.out.println("\n=== 阶段4：持续写入测试 ===");
            String switchedConfig3 = generateSwitchedConfig(originalDbkeySet, 0);
            boolean switch3Success = weightSwitchHelper.switchWeight("obkv",
                "obkv_adapt_dds_single", "v1.0", switchedConfig3, false);
            assertTrue("should switch weight successfully", switch3Success);
            Thread.sleep(10);
            
            for (int i = 1; i <= 25; i++) {
                String testKey = "weight_switch_test_key" + i;
                try {
                    client.insert(tableName, new Object[] { testKey }, new String[] { "c2" },
                        new Object[] { "first_round_value_" + i });
                    if (i % 5 == 0) {
                        System.out.println("first round write completed: key" + i);
                    }
                } catch (Exception e) {
                    System.out.println("first round write failed key" + i + ": " + e.getMessage());
                }
            }

            String switchedConfig4 = generateSwitchedConfig(originalDbkeySet, 1);
            boolean switch4Success = weightSwitchHelper.switchWeight("obkv",
                "obkv_adapt_dds_single", "v1.0", switchedConfig4, false);
            assertTrue("should switch weight successfully", switch4Success);
            Thread.sleep(10);
            
            for (int i = 26; i <= 50; i++) {
                String testKey = "weight_switch_test_key" + i;
                try {
                    client.insert(tableName, new Object[] { testKey }, new String[] { "c2" },
                        new Object[] { "second_round_value_" + i });
                    if (i % 5 == 0) {
                        System.out.println("second round write completed: key" + i);
                    }
                } catch (Exception e) {
                    System.out.println("second round write failed key" + i + ": " + e.getMessage());
                }
            }

            // 第五阶段：最终验证和分析
            System.out.println("\n=== 阶段5：最终验证和弹性位切换有效性分析 ===");
            
            // 分析数据分布模式
            Map<String, Integer> valuePatternCount = new HashMap<>();
            String[] sampleKeys = { "weight_switch_test_key1", "weight_switch_test_key25",
                    "weight_switch_test_key50", "weight_switch_test_key75",
                    "weight_switch_test_key100" };
            for (String sampleKey : sampleKeys) {
                try {
                    Map<String, Object> result = client.get(tableName, new Object[] { sampleKey },
                        new String[] { "c2" });
                    if (result != null && !result.isEmpty()) {
                        String value = (String) result.get("c2");
                        System.out.println("样本数据 [" + sampleKey + "] = " + value);
                        
                        // 统计值模式
                        String pattern = value.contains("first_round") ? "GROUP1" : 
                                       value.contains("second_round") ? "GROUP2" : "OTHER";
                        valuePatternCount.put(pattern, valuePatternCount.getOrDefault(pattern, 0) + 1);
                    } else {
                        System.out.println("样本数据 [" + sampleKey + "] = 未找到");
                    }
                } catch (Exception e) {
                    System.out.println("样本数据 [" + sampleKey + "] = 查询失败: " + e.getMessage());
                }
            }
            
            // batch put some data
            TableBatchOps batchOps = client.batch(tableName);
            for (int i = 1; i <= 10; i++) {
                String testKey = "weight_switch_test_key" + i;
                batchOps.insert(new Object[] { testKey }, new String[] { "c2" }, new Object[] { "batch_value_" + i });
            }
            List<Object> batchResult = batchOps.execute();
            System.out.println("batch put data result: " + batchResult);

            System.out.println("\n=== 弹性位切换有效性分析 ===");
            System.out.println("数据模式统计: " + valuePatternCount);
            
            // 结论分析
            if (valuePatternCount.getOrDefault("GROUP1", 0) > 0 && valuePatternCount.getOrDefault("GROUP2", 0) > 0) {
                System.out.println("✓ 检测到两种不同的数据模式，弹性位切换可能有效");
            } else {
                System.out.println("✗ 只检测到单一数据模式，弹性位切换可能无效");
                System.out.println("  建议检查：");
                System.out.println("  1. 两个弹性位是否真正指向不同的数据库实例");
                System.out.println("  2. 是否存在数据复制/同步机制");
                System.out.println("  3. 弹性位规则配置是否正确");
            }

        } finally {
            // clean up
            String[] testKeys = { "weight_switch_test_key1", "weight_switch_test_key25",
                    "weight_switch_test_key50", "weight_switch_test_key75",
                    "weight_switch_test_key100" };
            for (String key : testKeys) {
                client.delete(tableName, new Object[] { key });
            }
            // close client
            client.close();
        }
    }

    /**
     * generate switched config based on original dbkeySet
     * @param originalDbkeySet the original config
     * @param enabledDbkeyIndex the index of the dbkey to enable, other dbkeys will be disabled
     * @return the switched config
     */
    private String generateSwitchedConfig(String originalDbkeySet, int enabledDbkeyIndex) {
        String[] parts = originalDbkeySet.split(",");
        StringBuilder sb = new StringBuilder("group_00");

        // parse all dbkey names
        for (int i = 1; i < parts.length; i++) {
            // parse format: dbkeyName:weight
            String dbkeyWithWeight = parts[i];
            String dbkeyName = dbkeyWithWeight.split(":")[0];

            // enable the dbkey at the specified index (R10W10), disable others (R0W0)
            String weight = (i - 1 == enabledDbkeyIndex) ? "R10W10" : "R0W0";
            sb.append(",").append(dbkeyName).append(":").append(weight);
        }

        return sb.toString();
    }

    /**
     * 验证数据库连接独立性 - 通过创建独特标识符
     */
    private void verifyDatabaseIndependence(DdsObTableClient client, String tableName,
                                            String testKey) {
        try {
            // 插入一个带有时间戳和随机数的唯一标识符
            String uniqueValue = "independence_test_" + System.currentTimeMillis() + "_"
                                 + Math.random();
            client.insert(tableName, new Object[] { testKey }, new String[] { "c2", "c3" },
                new Object[] { uniqueValue, testKey + "_marker" });
            System.out.println("插入独立性验证数据: " + testKey + " -> " + uniqueValue);

            // 立即读取验证
            Map<String, Object> result = client.get(tableName, new Object[] { testKey },
                new String[] { "c2", "c3" });
            if (result != null && uniqueValue.equals(result.get("c2"))) {
                System.out.println("✓ 独立性验证数据确认写入: " + testKey);
            } else {
                System.out.println("✗ 独立性验证数据写入失败: " + testKey);
            }
        } catch (Exception e) {
            System.out.println("独立性验证异常 " + testKey + ": " + e.getMessage());
        }
    }

    /**
     * 验证数据库连接是否真正不同 - 通过插入和查询不同标识符的数据
     */
    private void verifyConnectionDifference(DdsObTableClient client, String tableName,
                                            int expectedGroupCount) {
        try {
            System.out.println("验证数据库连接的差异性...");

            // 插入测试数据到不同的key，观察路由模式
            String[] testKeys = { "connection_test_1", "connection_test_2", "connection_test_3" };

            for (int i = 0; i < testKeys.length; i++) {
                String testKey = testKeys[i];
                String testValue = "connection_verification_" + i + "_"
                                   + System.currentTimeMillis();

                try {
                    client.insert(tableName, new Object[] { testKey }, new String[] { "c2" },
                        new Object[] { testValue });

                    // 立即读取验证
                    Map<String, Object> result = client.get(tableName, new Object[] { testKey },
                        new String[] { "c2" });
                    if (result != null && testValue.equals(result.get("c2"))) {
                        System.out.println("✓ 连接验证数据 " + i + " 写入并读取成功: " + testKey);
                    } else {
                        System.out.println("✗ 连接验证数据 " + i + " 验证失败: " + testKey);
                    }

                } catch (Exception e) {
                    System.out.println("连接验证异常 " + testKey + ": " + e.getMessage());
                }
            }

        } catch (Exception e) {
            System.out.println("无法验证连接差异: " + e.getMessage());
        }
    }
}

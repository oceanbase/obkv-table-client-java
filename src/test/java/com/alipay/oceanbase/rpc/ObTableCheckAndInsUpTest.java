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

import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Delete;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.compareVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

public class ObTableCheckAndInsUpTest {
    public ObTableClient        client;
    private static long         MINI_SUPP_VERSION = ObGlobal.calcVersion(4, (short) 2, (byte) 1,
                                                      (byte) 2);
    private static final String TABLE_NAME        = "test_mutation";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    private boolean isVersionSupported() {
        if (ObTableClientTestUtil.isOBVersionGreaterEqualThan(MINI_SUPP_VERSION)) {
            return true;
        }
        return false;
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
    public void testBatchWithDiffRows() throws Exception {
        if (!isVersionSupported()) {
            System.out.println("current version is not supported, current version: "
                               + ObGlobal.OB_VERSION);
            return;
        }
        try {
            // 0. prepare data, insert(1, 'c2_v0', 'c3_v0', 100),(2, 'c2_v0', 'c3_v0', 100),(3, 'c2_v0', 'c3_v0', 100),(4, 'c2_v0', 'c3_v0', 100)
            for (long i = 1L; i <= 4L; i++) {
                InsertOrUpdate insertOrUpdate = client.insertOrUpdate(TABLE_NAME);
                insertOrUpdate.setRowKey(row(colVal("c1", i), colVal("c2", "c2_v0")));
                insertOrUpdate.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 100L)));
                MutationResult res = insertOrUpdate.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }

            BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
            // 1. check exists match: insup(1, 'c2_v0', 'c3_v0', 200) if exists c3 >= 'c3_v0';
            InsertOrUpdate insertOrUpdate1 = new InsertOrUpdate();
            insertOrUpdate1.setRowKey(row(colVal("c1", 1L), colVal("c2", "c2_v0")));
            insertOrUpdate1.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 200L)));
            ObTableFilter filter = compareVal(ObCompareOp.GE, "c3", "c3_v0");
            CheckAndInsUp checkAndInsUp1 = new CheckAndInsUp(filter, insertOrUpdate1, true);

            // 2. check exists not match: insup(2, 'c2_v0', 'c3_v0', 200) if exists c3 > 'c3_v0';
            InsertOrUpdate insertOrUpdate2 = new InsertOrUpdate();
            insertOrUpdate2.setRowKey(row(colVal("c1", 2L), colVal("c2", "c2_v0")));
            insertOrUpdate2.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 200L)));
            filter = compareVal(ObCompareOp.GT, "c3", "c3_v0");
            CheckAndInsUp checkAndInsUp2 = new CheckAndInsUp(filter, insertOrUpdate2, true);

            // 3. check no exists match: insup(3, 'c2_v0', 'c3_v0', 200) if not exists c4 > 200
            InsertOrUpdate insertOrUpdate3 = new InsertOrUpdate();
            insertOrUpdate3.setRowKey(row(colVal("c1", 3L), colVal("c2", "c2_v0")));
            insertOrUpdate3.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 200L)));
            filter = compareVal(ObCompareOp.GE, "c4", 200L);
            CheckAndInsUp checkAndInsUp3 = new CheckAndInsUp(filter, insertOrUpdate3, false);

            // 4. check no exists not match: insup(4, 'c2_v0', 'c3_v0', 200) if exists c4 is null
            InsertOrUpdate insertOrUpdate4 = new InsertOrUpdate();
            insertOrUpdate4.setRowKey(row(colVal("c1", 4L), colVal("c2", "c2_v0")));
            insertOrUpdate4.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 200L)));
            filter = compareVal(ObCompareOp.IS_NOT, "c4", null);
            CheckAndInsUp checkAndInsUp4 = new CheckAndInsUp(filter, insertOrUpdate4, false);

            // 5. verify result
            batchOperation.addOperation(checkAndInsUp1, checkAndInsUp2, checkAndInsUp3,
                checkAndInsUp4);
            BatchOperationResult batchOperationResult = batchOperation.execute();
            Assert.assertEquals(4, batchOperationResult.size());
            Assert.assertEquals(1, batchOperationResult.get(0).getAffectedRows());
            Assert.assertEquals(0, batchOperationResult.get(1).getAffectedRows());
            Assert.assertEquals(1, batchOperationResult.get(2).getAffectedRows());
            Assert.assertEquals(0, batchOperationResult.get(3).getAffectedRows());

            Map<String, Object> res = client.get(TABLE_NAME, new Object[] { 1L, "c2_v0" },
                new String[] { "c4" });
            Assert.assertEquals(200L, res.get("c4"));
            res = client.get(TABLE_NAME, new Object[] { 2L, "c2_v0" }, new String[] { "c4" });
            Assert.assertEquals(100L, res.get("c4"));
            res = client.get(TABLE_NAME, new Object[] { 3L, "c2_v0" }, new String[] { "c4" });
            Assert.assertEquals(200L, res.get("c4"));
            res = client.get(TABLE_NAME, new Object[] { 4L, "c2_v0" }, new String[] { "c4" });
            Assert.assertEquals(100L, res.get("c4"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 1L; i <= 4L; i++) {
                Delete delete = client.delete(TABLE_NAME);
                delete.setRowKey(row(colVal("c1", i), colVal("c2", "c2_v0")));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    @Test
    public void testBatchWithSameRows() throws Exception {
        if (!isVersionSupported()) {
            System.out.println("current version is not supported, current version: "
                               + ObGlobal.OB_VERSION);
            return;
        }

        try {
            // 0. prepare data, insert(5, 'c2_v0', 'c3_v0', 100)
            InsertOrUpdate insertOrUpdate = client.insertOrUpdate(TABLE_NAME);
            insertOrUpdate.setRowKey(row(colVal("c1", 5L), colVal("c2", "c2_v0")));
            insertOrUpdate.addMutateRow(row(colVal("c3", "c3_v0".getBytes()), colVal("c4", 100L)));
            MutationResult result = insertOrUpdate.execute();
            Assert.assertEquals(1, result.getAffectedRows());

            // 1. check exists match: insup(5, 'c2_v0', c3_v0, 200) if exists c3 is not null;
            InsertOrUpdate insertOrUpdate1 = new InsertOrUpdate();
            insertOrUpdate1.setRowKey(row(colVal("c1", 5L), colVal("c2", "c2_v0")));
            insertOrUpdate1.addMutateRow(row(colVal("c4", 200L)));
            ObTableFilter filter = compareVal(ObCompareOp.IS_NOT, "c3", null);
            CheckAndInsUp checkAndInsUp1 = new CheckAndInsUp(filter, insertOrUpdate1, true);

            // 2. check exists not match: insup(5, 'c2_v0', 'c3_v1', 200) if exists c4 > 200 ;
            InsertOrUpdate insertOrUpdate2 = new InsertOrUpdate();
            insertOrUpdate2.setRowKey(row(colVal("c1", 5L), colVal("c2", "c2_v0")));
            insertOrUpdate2.addMutateRow(row(colVal("c3", "c3_v1".getBytes()), colVal("c4", 200L)));
            filter = compareVal(ObCompareOp.GT, "c4", 200L);
            CheckAndInsUp checkAndInsUp2 = new CheckAndInsUp(filter, insertOrUpdate2, true);

            // 3. check no exists match: insup(5, 'c2_v0', 'c3_v1', 300) if not exists c4 > 300 ;
            InsertOrUpdate insertOrUpdate3 = new InsertOrUpdate();
            insertOrUpdate3.setRowKey(row(colVal("c1", 5L), colVal("c2", "c2_v0")));
            insertOrUpdate3.addMutateRow(row(colVal("c3", "c3_v1".getBytes()), colVal("c4", 300L)));
            filter = compareVal(ObCompareOp.GT, "c4", 300L);
            CheckAndInsUp checkAndInsUp3 = new CheckAndInsUp(filter, insertOrUpdate3, false);

            // 3. check no exists not match: insup(5, 'c2_v0', 'c3_v1', 400) if not exists c4 >= 300 ;
            InsertOrUpdate insertOrUpdate4 = new InsertOrUpdate();
            insertOrUpdate4.setRowKey(row(colVal("c1", 5L), colVal("c2", "c2_v0")));
            insertOrUpdate4.addMutateRow(row(colVal("c3", "c3_v1".getBytes()), colVal("c4", 400L)));
            filter = compareVal(ObCompareOp.GE, "c4", 300L);
            CheckAndInsUp checkAndInsUp4 = new CheckAndInsUp(filter, insertOrUpdate4, false);

            BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
            batchOperation.addOperation(checkAndInsUp1, checkAndInsUp2, checkAndInsUp3,
                checkAndInsUp4);
            BatchOperationResult batchOperationResult = batchOperation.execute();
            Assert.assertEquals(4, batchOperationResult.size());
            Assert.assertEquals(1, batchOperationResult.get(0).getAffectedRows());
            Assert.assertEquals(0, batchOperationResult.get(1).getAffectedRows());
            Assert.assertEquals(1, batchOperationResult.get(2).getAffectedRows());
            Assert.assertEquals(0, batchOperationResult.get(3).getAffectedRows());

            Map<String, Object> res = client.get(TABLE_NAME, new Object[] { 5L, "c2_v0" }, null);
            Assert.assertEquals("c3_v1", new String((byte[]) res.get("c3"), "UTF-8"));
            Assert.assertEquals(300L, res.get("c4"));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            Delete delete = client.delete(TABLE_NAME);
            delete.setRowKey(row(colVal("c1", 5L), colVal("c2", "c2_v0")));
            MutationResult res = delete.execute();
            Assert.assertEquals(1, res.getAffectedRows());
        }
    }

    @Test
    public void testSingleCheckInsUp() throws Exception {
        if (!isVersionSupported()) {
            System.out.println("current version is not supported, current version: "
                               + ObGlobal.OB_VERSION);
            return;
        }

        try {
            // 0. prepare data, insert(1, 'c2_v0', 'c3_v0', 100),(2, 'c2_v0', 'c3_v0', 100),(3, 'c2_v0', 'c3_v0', 100),(4, 'c2_v0', 'c3_v0', 100)
            for (long i = 1L; i <= 4L; i++) {
                InsertOrUpdate insertOrUpdate = client.insertOrUpdate(TABLE_NAME);
                insertOrUpdate.setRowKey(row(colVal("c1", i), colVal("c2", "c2_v0")));
                insertOrUpdate.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 100L)));
                MutationResult res = insertOrUpdate.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }

            // 1. check exists match: insup(1, 'c2_v0', 'c3_v0', 200) if exists c3 >= 'c3_v0';
            InsertOrUpdate insertOrUpdate1 = new InsertOrUpdate();
            insertOrUpdate1.setRowKey(row(colVal("c1", 1L), colVal("c2", "c2_v0")));
            insertOrUpdate1.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 200L)));
            ObTableFilter filter = compareVal(ObCompareOp.GE, "c3", "c3_v0");
            CheckAndInsUp checkAndInsUp1 = client.checkAndInsUp(TABLE_NAME, filter,
                insertOrUpdate1, true);
            MutationResult result1 = checkAndInsUp1.execute();
            Assert.assertEquals(1, result1.getAffectedRows());
            Map<String, Object> res = client.get(TABLE_NAME, new Object[] { 1L, "c2_v0" },
                new String[] { "c3", "c4" });
            Assert.assertEquals("c3_v0", new String((byte[]) res.get("c3"), "UTF-8"));
            Assert.assertEquals(200L, res.get("c4"));

            // 2. check exists not match: insup(2, 'c2_v0', 'c3_v0', 200) if exists c3 > 'c3_v0';
            InsertOrUpdate insertOrUpdate2 = new InsertOrUpdate();
            insertOrUpdate2.setRowKey(row(colVal("c1", 2L), colVal("c2", "c2_v0")));
            insertOrUpdate2.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 200L)));
            filter = compareVal(ObCompareOp.GT, "c3", "c3_v0");
            CheckAndInsUp checkAndInsUp2 = client.checkAndInsUp(TABLE_NAME, filter,
                insertOrUpdate2, true);
            MutationResult result2 = checkAndInsUp2.execute();
            Assert.assertEquals(0, result2.getAffectedRows());
            res = client.get(TABLE_NAME, new Object[] { 2L, "c2_v0" }, new String[] { "c3", "c4" });
            Assert.assertEquals("c3_v0", new String((byte[]) res.get("c3"), "UTF-8"));
            Assert.assertEquals(100L, res.get("c4"));

            // 3. check no exists match: insup(3, 'c2_v0', 'c3_v0', 200) if not exists c4 > 200
            InsertOrUpdate insertOrUpdate3 = new InsertOrUpdate();
            insertOrUpdate3.setRowKey(row(colVal("c1", 3L), colVal("c2", "c2_v0")));
            insertOrUpdate3.addMutateRow(row(colVal("c3", "c3_v1"), colVal("c4", 200L)));
            filter = compareVal(ObCompareOp.GE, "c4", 200L);
            CheckAndInsUp checkAndInsUp3 = client.checkAndInsUp(TABLE_NAME, filter,
                insertOrUpdate3, false);
            MutationResult result3 = checkAndInsUp3.execute();
            Assert.assertEquals(1, result3.getAffectedRows());
            res = client.get(TABLE_NAME, new Object[] { 3L, "c2_v0" }, new String[] { "c3", "c4" });
            Assert.assertEquals("c3_v1", new String((byte[]) res.get("c3"), "UTF-8"));
            Assert.assertEquals(200L, res.get("c4"));

            // 4. check no exists not match: insup(4, 'c2_v0', 'c3_v0', 200) if exists c4 is null
            InsertOrUpdate insertOrUpdate4 = new InsertOrUpdate();
            insertOrUpdate4.setRowKey(row(colVal("c1", 4L), colVal("c2", "c2_v0")));
            insertOrUpdate4.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 200L)));
            filter = compareVal(ObCompareOp.IS_NOT, "c4", null);
            CheckAndInsUp checkAndInsUp4 = client.checkAndInsUp(TABLE_NAME, filter,
                insertOrUpdate4, false);
            MutationResult result4 = checkAndInsUp4.execute();
            Assert.assertEquals(0, result4.getAffectedRows());
            res = client.get(TABLE_NAME, new Object[] { 3L, "c2_v0" }, new String[] { "c3", "c4" });
            Assert.assertEquals("c3_v1", new String((byte[]) res.get("c3"), "UTF-8"));
            Assert.assertEquals(200L, res.get("c4"));

        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long i = 1L; i <= 4L; i++) {
                Delete delete = client.delete(TABLE_NAME);
                delete.setRowKey(row(colVal("c1", i), colVal("c2", "c2_v0")));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }

    @Test
    public void testBatchCheckInsUpMutPart() throws Exception {
        if (!isVersionSupported()) {
            System.out.println("current version is not supported, current version: "
                    + ObGlobal.OB_VERSION);
            return;
        }
        // insert two record in different partition
        // insert (100, "c2_val", "c3_val", 100)
        InsertOrUpdate insertOrUpdate = client.insertOrUpdate(TABLE_NAME);
        insertOrUpdate.setRowKey(row(colVal("c1", 100L), colVal("c2", "c2_val")));
        insertOrUpdate.addMutateRow(row(colVal("c3", "c3_v0"), colVal("c4", 100L)));
        MutationResult res = insertOrUpdate.execute();
        Assert.assertEquals(1, res.getAffectedRows());
        // insert (400, "c2_val", "c3_val", 400)
        insertOrUpdate = client.insertOrUpdate(TABLE_NAME);
        insertOrUpdate.setRowKey(row(colVal("c1", 400L), colVal("c2", "c2_val")));
        insertOrUpdate.addMutateRow(row(colVal("c3", "c3_val"), colVal("c4", 400L)));
        res = insertOrUpdate.execute();
        Assert.assertEquals(1, res.getAffectedRows());

        try {
            BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
            // 1. check exists not match: insup(100, 'c2_val', 'c3_val', 200) if exists c4 > 100
            InsertOrUpdate insertOrUpdate1 = new InsertOrUpdate();
            insertOrUpdate1.setRowKey(row(colVal("c1", 100L), colVal("c2", "c2_val")));
            insertOrUpdate1.addMutateRow(row(colVal("c3", "c3_val"), colVal("c4", 200L)));
            ObTableFilter filter = compareVal(ObCompareOp.GT, "c4", 100L);
            CheckAndInsUp checkAndInsUp1 = new CheckAndInsUp(filter, insertOrUpdate1, true);
            // 2. check not exists match: insup(400, 'c2_val', 'c3_val', 500) if not exists c4 >= 500
            InsertOrUpdate insertOrUpdate2 = new InsertOrUpdate();
            insertOrUpdate2.setRowKey(row(colVal("c1", 400L), colVal("c2", "c2_val")));
            insertOrUpdate2.addMutateRow(row(colVal("c3", "c3_val"), colVal("c4", 500L)));
            ObTableFilter filter2 = compareVal(ObCompareOp.GE, "c4", 500L);
            CheckAndInsUp checkAndInsUp2 = new CheckAndInsUp(filter2, insertOrUpdate2, false);

            // 3. execute batch
            batchOperation.addOperation(checkAndInsUp1, checkAndInsUp2);
            BatchOperationResult result = batchOperation.execute();
            Assert.assertEquals(2, result.getCorrectCount());
            Assert.assertEquals(0, result.get(0).getAffectedRows());
            Assert.assertEquals(1, result.get(1).getAffectedRows());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }
    }
}

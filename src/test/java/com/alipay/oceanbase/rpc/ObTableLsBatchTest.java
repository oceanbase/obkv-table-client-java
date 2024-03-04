/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2024 OceanBase
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

import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Delete;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;

public class ObTableLsBatchTest {
    public ObTableClient        client;
    private static final String TABLE_NAME        = "test_mutation";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
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
    public void testInsertUp() throws Exception {
        BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
        Object values[][] = {
                {1L, "c2_val", "c3_val", 100L},
                {400L, "c2_val", "c3_val", 100L},
                {401L, "c2_val", "c3_val", 100L},
                {1000L, "c2_val", "c3_val", 100L},
                {1001L, "c2_val", "c3_val", 100L},
                {1002L, "c2_val", "c3_val", 100L},
        };
        int rowCnt = values.length;
        try {
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
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (int j = 1; j < rowCnt; j++) {
                Delete delete = client.delete(TABLE_NAME);
                delete.setRowKey(row(colVal("c1", values[j][0]), colVal("c2", values[j][1])));
                MutationResult res = delete.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        }
    }
}

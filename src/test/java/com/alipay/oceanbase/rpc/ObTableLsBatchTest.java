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
import com.alipay.oceanbase.rpc.mutation.result.OperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import com.alipay.oceanbase.rpc.util.TimeUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.google.common.util.concurrent.ExecutionError;
import com.alipay.oceanbase.rpc.mutation.Row;

import javax.management.Query;

import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;

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

    @Test
    public void testGet() throws Exception {
        // prepare data
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
                InsertOrUpdate insertOrUpdate = client.insertOrUpdate(TABLE_NAME);
                insertOrUpdate.setRowKey(row(colVal("c1", curRow[0]), colVal("c2", curRow[1])));
                insertOrUpdate.addMutateRow(row(colVal("c3", curRow[2]), colVal("c4", curRow[3])));
                MutationResult res = insertOrUpdate.execute();
                Assert.assertEquals(1, res.getAffectedRows());
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {}

        try {
            BatchOperation batchOperation = client.batchOperation(TABLE_NAME);
            for (int i = 0; i < rowCnt; i++) {
                Object[] curRow = values[i];
                TableQuery query = query().setRowKey(row(colVal("c1", curRow[0]), colVal("c2", curRow[1])))
                        .select("c1", "c2", "c3", "c4");
                batchOperation.addOperation(query);
            }

            BatchOperationResult batchOperationResult = batchOperation.execute();
            Assert.assertEquals(rowCnt, batchOperationResult.size());
            for (int j = 0; j < rowCnt; j++) {
                Row row = batchOperationResult.get(j).getOperationRow();
                Assert.assertEquals(values[j][0], row.get("c1"));
                Assert.assertEquals(values[j][1], row.get("c2"));
                Assert.assertEquals(values[j][2], new String((byte[]) row.get("c3"), "UTF-8"));
                Assert.assertEquals(values[j][3], row.get("c4"));
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

    /*
        CREATE TABLE IF NOT EXISTS `test_table_object` (
            `c1` tinyint primary key,
            `c2` smallint not null,
            `c3` int not null,
            `c4` bigint not null,
            `c5` varchar(128) not null,
            `c6` varbinary(128) not null,
            `c7` float not null,
            `c8` double not null,
            `c9` timestamp(6) not null,
            `c10` datetime(6) not null,
            `c11` int default null
        );
     */
    @Test
    public void testGetAllObjType() throws Exception {
        String ALL_OBJ_TYPE_TABLE = "test_table_object";

        // pre-clean data
        client.delete(ALL_OBJ_TYPE_TABLE).addScanRange(ObObj.getMin(), ObObj.getMax()).execute();

        long timeInMillis = System.currentTimeMillis();
        Timestamp c9Val = new Timestamp(timeInMillis);
        Date c10Val = TimeUtils.strToDate("2024-01-30");

        // prepare data
        Object values[][] = {
                {(byte)1, (short)1, (int)1, 1L, "c5_val", "c6_val".getBytes(), 100.0f, 200.0d, c9Val, c10Val, null},
                {(byte)2, (short)2, (int)2, 2L, "c5_val", "c6_val".getBytes(), 100.0f, 200.0d, c9Val, c10Val, null},
                {(byte)3, (short)3, (int)3, 3L, "c5_val", "c6_val".getBytes(), 100.0f, 200.0d, c9Val, c10Val, null},
                {(byte)4, (short)4, (int)4, 4L, "c5_val", "c6_val".getBytes(), 100.0f, 200.0d, c9Val, c10Val, null},
                {(byte)5, (short)5, (int)5, 5L, "c5_val", "c6_val".getBytes(), 100.0f, 200.0d, c9Val, c10Val, null},
                {(byte)6, (short)6, (int)6, 6L, "c5_val", "c6_val".getBytes(), 100.0f, 200.0d, c9Val, c10Val, null}
        };

        int rowCnt = values.length;

        try {
            // pre insert data
            {
                BatchOperation batchOperation = client.batchOperation(ALL_OBJ_TYPE_TABLE);
                for (int i = 0; i < rowCnt; i++) {
                    Object[] curRow = values[i];
                    InsertOrUpdate insertOrUpdate = new InsertOrUpdate();
                    insertOrUpdate.setRowKey(row(colVal("c1", curRow[0])));

                    for (int j = 2; j <= curRow.length; j++) {
                        insertOrUpdate.addMutateRow(row(colVal("c" + j, curRow[j-1])));
                    }
                    batchOperation.addOperation(insertOrUpdate);
                }
                BatchOperationResult res = batchOperation.execute();
                for (int k = 0; k < rowCnt; k++) {
                    Assert.assertEquals(1, res.get(k).getAffectedRows());
                }
            }

            // get data with all columns
            {
                BatchOperation batchOperation = client.batchOperation(ALL_OBJ_TYPE_TABLE);
                for (int i = 0; i < rowCnt; i++) {
                    Object[] curRow = values[i];
                    TableQuery query = query().setRowKey(row(colVal("c1", curRow[0])))
                            .select("c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "c10", "c11");
                    batchOperation.addOperation(query);
                }
                BatchOperationResult res = batchOperation.execute();

                for (int j = 0; j < rowCnt; j++) {
                    Object[] curRow = values[j];
                    Row row = res.get(j).getOperationRow();
                    for (int k = 0; k < curRow.length; k++) {
                        Object getValue = row.get("c" + (k+1));
                        if (getValue instanceof byte[]) {
                            Assert.assertTrue(Arrays.equals((byte[]) curRow[k], (byte[]) getValue));
                        } else {
                            Assert.assertEquals(curRow[k], getValue);
                        }
                    }
                }
            }

            // get data with different columns
            {
                // get columns idx, 1 means get c1
                int columns[][] = {
                        {7, 3, 11, 5, 6, 2, 9, 1, 8, 4, 10},
                        {5, 2, 7, 9, 1, 8, 6, 3, 10, 4},
                        {5, 3, 1, 8, 6, 4, 7, 2, 9},
                        {3, 2, 5, 8, 7, 1, 6, 4},
                        {4, 6, 7, 1, 3, 5, 2},
                        {2, 6, 3, 1, 4, 5},
                };

                BatchOperation batchOperation = client.batchOperation(ALL_OBJ_TYPE_TABLE);
                for (int i = 0; i < rowCnt; i++) {
                    Object[] curRow = values[i];
                    TableQuery query = query().setRowKey(row(colVal("c1", curRow[0])));
                    List<String> selectColumns = new ArrayList<>();
                    for (int j = 0; j < columns[i].length; j++) {
                        selectColumns.add("c" + columns[i][j]);
                    }
                    query.select(selectColumns.toArray(new String[0]));
                    batchOperation.addOperation(query);
                }
                BatchOperationResult res = batchOperation.execute();

                for (int j = 0; j < rowCnt; j++) {
                    int curColumns[] = columns[j];
                    Object[] curRow = values[j];
                    Row row = res.get(j).getOperationRow();
                    for (int k = 0; k < curColumns.length; k++) {
                        Object curValue = curRow[curColumns[k]-1];
                        String curSelectColumn = "c" + curColumns[k];
                        Object getValue = row.get(curSelectColumn);
                        if (getValue instanceof byte[]) {
                            Assert.assertTrue(Arrays.equals((byte[]) curValue, (byte[]) getValue));
                        } else {
                            Assert.assertEquals(curValue, getValue);
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {}
    }

}

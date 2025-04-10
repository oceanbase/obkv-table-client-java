/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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
import com.alipay.oceanbase.rpc.get.Get;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/*
CREATE TABLE IF NOT EXISTS `test_datetime_range` (
    `c1` bigint(20) NOT NULL,
    `c2` datetime(6) NOT NULL,
    `c3` varchar(20) DEFAULT NULL,
    KEY `idx_c2` (`c2`) LOCAL,
    PRIMARY KEY (`c1`, `c2`)
    ) partition by range columns(c2) subpartition by key(c1) subpartition template (
    subpartition `p0`,
    subpartition `p1`,
    subpartition `p2`,
    subpartition `p3`)
    (partition `p0` values less than ('2023-12-01 00:00:00'),
     partition `p1` values less than ('2024-12-10 00:00:00'),
     partition `p2` values less than ('2025-12-20 00:00:00'),
     partition `p3` values less than ('2026-12-30 00:00:00'),
     partition `p4` values less than ('2027-01-01 00:00:00'),
     partition `p5` values less than MAXVALUE);
 */
public class ObTableDatetimeTest {
    ObTableClient        client;
    public static String tableName = "test_datetime_range";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    @Test
    public void testSingle() throws Exception {
        String dateString = "2023-04-05 14:30:00";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(dateString);
        try {
            Row rk = row(colVal("c1", 1L), colVal("c2", date), colVal("c3", 1L));
            client.insertOrUpdate(tableName).setRowKey(rk).addMutateColVal(colVal("c4", "c4_val"))
                .execute();
            Map<String, Object> res = client.get(tableName).setRowKey(rk).select("c4")
                    .execute();
            Assert.assertEquals("c4_val", res.get("c4"));

            client.delete(tableName).setRowKey(rk).execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test
    public void testBatch() throws Exception {
        String dateString1 = "2023-04-05 14:30:00";
        String dateString2 = "2024-04-05 14:30:00";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date1 = sdf.parse(dateString1);
        Date date2 = sdf.parse(dateString2);
        try {
            Row rk1 = row(colVal("c1", 1L), colVal("c2", date1), colVal("c3", 1L));
            Row rk2 = row(colVal("c1", 1L), colVal("c2", date2), colVal("c3", 1L));
            BatchOperation batch = client.batchOperation(tableName);
            InsertOrUpdate insUp1 = client.insertOrUpdate(tableName).setRowKey(rk1)
                .addMutateColVal(colVal("c4", "c4_val"));
            InsertOrUpdate insUp2 = client.insertOrUpdate(tableName).setRowKey(rk2)
                .addMutateColVal(colVal("c4", "c4_val"));
            batch.addOperation(insUp1, insUp2);
            BatchOperationResult res = batch.execute();
            Assert.assertNotNull(res);
            Assert.assertNotEquals(0, res.get(0).getAffectedRows());
            Assert.assertNotEquals(0, res.get(1).getAffectedRows());

            client.delete(tableName).setRowKey(rk1).execute();
            client.delete(tableName).setRowKey(rk2).execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test
    public void testPkQuery() throws Exception {
        String dateString = "2023-04-05 14:30:00";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(dateString);
        try {
            Row rk = row(colVal("c1", 1L), colVal("c2", date), colVal("c3", 1L));
            client.insertOrUpdate(tableName).setRowKey(rk).addMutateColVal(colVal("c4", "c4_val"))
                .execute();

            QueryResultSet res = client.query(tableName).select("c4")
                .setScanRangeColumns("c1", "c2", "c3")
                .addScanRange(new Object[] { 1L, date, 1L }, new Object[] { 1L, date, 1L })
                .execute();
            Assert.assertTrue(res.next());
            Assert.assertEquals("c4_val", res.getRow().get("c4"));

            client.delete(tableName).setRowKey(rk).execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }

    @Test
    public void testIndexQuery() throws Exception {
        String dateString = "2023-04-05 14:30:00";
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = sdf.parse(dateString);
        try {
            Row rk = row(colVal("c1", 1L), colVal("c2", date), colVal("c3", 1L));
            client.insertOrUpdate(tableName).setRowKey(rk).addMutateColVal(colVal("c4", "c4_val"))
                .execute();

            QueryResultSet res = client.query(tableName).indexName("idx_c2").select("c4")
                .setScanRangeColumns("c1", "c2")
                .addScanRange(new Object[] { 1L, date }, new Object[] { 1L, date }).execute();
            Assert.assertTrue(res.next());
            Assert.assertEquals("c4_val", res.getRow().get("c4"));

            client.delete(tableName).setRowKey(rk).execute();
        } catch (Exception e) {
            System.out.println(e.getMessage());
            throw e;
        }
    }
}

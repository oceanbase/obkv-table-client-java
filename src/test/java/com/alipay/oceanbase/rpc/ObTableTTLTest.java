/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.Map;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.compareVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ObTableTTLTest {
    ObTableClient        client;
    public static String tableName    = "test_ttl_timestamp";
    public static String defaultValue = "hello world";
    public static String keyCol       = "c1";
    public static String valueCol     = "c2";
    public static String intCol       = "c3";
    public static String expireCol    = "expired_ts";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { keyCol });
    }

    /**
     CREATE TABLE `test_ttl_timestamp` (
     `c1` bigint NOT NULL,
     `c2` varchar(20) DEFAULT NULL,
     `c3` bigint DEFAULT NULL,
     `expired_ts` timestamp,
     PRIMARY KEY (`c1`)) TTL(expired_ts + INTERVAL 0 SECOND);
     **/
    @Test
    public void testQuery() throws Exception {
        long[] keyIds = { 1L, 2L };
        try {
            // 1. insert records with null expired_ts
            for (long id : keyIds) {
                client.insert(tableName).setRowKey(colVal(keyCol, id))
                    .addMutateColVal(colVal(valueCol, defaultValue))
                    .addMutateColVal(colVal(expireCol, null)).execute();
            }
            // 2. query all inserted records
            QueryResultSet resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[1])
                .execute();
            Assert.assertEquals(resultSet.cacheSize(), keyIds.length);

            // 3. update the expired_ts
            Timestamp curTs = new Timestamp(System.currentTimeMillis());
            curTs.setNanos(0);
            client.update(tableName).setRowKey(colVal(keyCol, keyIds[0]))
                .addMutateColVal(colVal(expireCol, curTs)).execute();

            // 3. re-query all inserted records, the expired record won't be returned
            resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[1]).execute();
            Assert.assertEquals(resultSet.cacheSize(), 1);
            Assert.assertTrue(resultSet.next());
            Row row = resultSet.getResultRow();
            Assert.assertEquals(row.get(keyCol), keyIds[1]);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long id : keyIds) {
                client.delete(tableName).setRowKey(colVal(keyCol, id)).execute();
            }
        }
    }

    @Test
    public void testQueryOffset() throws Exception {
        long[] keyIds = { 1L, 2L, 3L, 4L };
        String tableName = "test_ttl_timestamp_5s";
        try {
            Timestamp curTs = new Timestamp(System.currentTimeMillis());

            // 1. insert records with current expired_ts
            client.insert(tableName).setRowKey(colVal(keyCol, 1L))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, curTs)).execute();

            client.insert(tableName).setRowKey(colVal(keyCol, 2L))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, curTs)).execute();

            client.insert(tableName).setRowKey(colVal(keyCol, 3L))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, null)).execute();

            client.insert(tableName).setRowKey(colVal(keyCol, 4L))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, null)).execute();

            //+----+-------------+------+----------------------------+
            //| c1 | c2          | c3   | expired_ts                 |
            //+----+-------------+------+----------------------------+
            //|  1 | hello world | NULL | xxx                        |
            //|  2 | hello world | NULL | xxx                        |
            //|  3 | hello world | NULL | NULL                       |
            //|  4 | hello world | NULL | NULL                       |
            //+----+-------------+------+----------------------------+
            Thread.sleep(6000);

            // 2. query offset-2
            QueryResultSet resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[3])
                .limit(2, 4).execute();
            Assert.assertEquals(resultSet.cacheSize(), 0);

            // 2. query offset-0 && limit-2 and c1 >= 0
            resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[3]).limit(0, 2)
                .setFilter(compareVal(ObCompareOp.GE, keyCol, 0)).execute();
            Assert.assertEquals(resultSet.cacheSize(), 2);

            // 3. query offset-2 && limit-2 and c1 >= 0
            resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[3]).limit(2, 2)
                .setFilter(compareVal(ObCompareOp.GE, keyCol, 0)).execute();
            Assert.assertEquals(resultSet.cacheSize(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            for (long id : keyIds) {
                client.delete(tableName).setRowKey(colVal(keyCol, id)).execute();
            }
        }
    }

    @Test
    public void test_single_operation() throws Exception {
        long rowKey = 3L;
        try {
            long timeInMillis = System.currentTimeMillis() + 1000;
            Timestamp ts = new Timestamp(timeInMillis);
            ts.setNanos(0);
            client.insert(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, ts)).execute();
            Thread.sleep(1000);

            // 1. get expired record, should return empty result
            Map getRes = client.get(tableName, rowKey, null);
            Assert.assertTrue(getRes.isEmpty());

            // 2. insert new record, should success
            MutationResult mutateRes = client.insert(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, null)).execute();
            assertEquals(1, mutateRes.getAffectedRows());

            // 3. update expired record, affected_rows should be 0
            ts = new Timestamp(System.currentTimeMillis());
            ts.setNanos(0);
            mutateRes = client.update(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(expireCol, ts)).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            mutateRes = client.update(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(expireCol, null)).execute();
            assertEquals(0, mutateRes.getAffectedRows());

            // 4. insertOrUpdate expired record, should insert success
            ts = new Timestamp(System.currentTimeMillis() + 3000);
            ts.setNanos(0);
            mutateRes = client.insertOrUpdate(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(expireCol, ts)).addMutateColVal(colVal(intCol, 50L))
                .execute();
            assertEquals(1, mutateRes.getAffectedRows());
            getRes = client.get(tableName, rowKey, null);
            Assert.assertEquals(null, getRes.get(valueCol));
            Assert.assertEquals(50L, getRes.get(intCol));
            // NOTE: this case failed with a small probability: the interval of insert and get exceed 3000ms
            Assert.assertEquals(ts, getRes.get(expireCol));
            Thread.sleep(3000);

            // 5. increment a expired record, the behavior is same as increment a new record
            mutateRes = client.increment(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(intCol, 100L)).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            getRes = client.get(tableName, rowKey, null);
            Assert.assertEquals(null, getRes.get(expireCol));
            Assert.assertEquals(null, getRes.get(valueCol));
            Assert.assertEquals(100L, getRes.get(intCol));

            // 6. append a expired record, the behavior is same as append a new record
            ts = new Timestamp(System.currentTimeMillis());
            ts.setNanos(0);
            mutateRes = client.update(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, ts)).execute();
            String appendVal = "how are u";
            mutateRes = client.append(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(valueCol, appendVal)).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            getRes = client.get(tableName, rowKey, null);
            Assert.assertEquals(null, getRes.get(intCol));
            Assert.assertEquals(appendVal, getRes.get(valueCol));
            Assert.assertEquals(null, getRes.get(expireCol));

            // 7. replace
            ts = new Timestamp(System.currentTimeMillis());
            ts.setNanos(0);
            mutateRes = client.update(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, ts)).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            mutateRes = client.replace(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(expireCol, null))
                .addMutateColVal(colVal(valueCol, defaultValue)).execute();
            assertEquals(2, mutateRes.getAffectedRows());
            getRes = client.get(tableName, rowKey, null);
            Assert.assertEquals(null, getRes.get(intCol));
            Assert.assertEquals(defaultValue, getRes.get(valueCol));
            Assert.assertEquals(null, getRes.get(expireCol));

            // 8. delete
            ts = new Timestamp(System.currentTimeMillis());
            ts.setNanos(0);
            mutateRes = client.update(tableName).setRowKey(colVal(keyCol, rowKey))
                .addMutateColVal(colVal(expireCol, ts)).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            mutateRes = client.delete(tableName).setRowKey(colVal(keyCol, rowKey)).execute();
            assertEquals(0, mutateRes.getAffectedRows());
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        } finally {
            client.delete(tableName).addScanRange(rowKey, rowKey).execute();
        }
    }

    // 1. query and update
    // 2. query and increment
    // 3. query and append
    // 4. query and delete
    @Test
    public void testQueryAndMutate() throws Exception {
        long[] keyIds = { 5L, 6L };

        try {
            long timeInMillis = System.currentTimeMillis();
            Timestamp curTs = new Timestamp(timeInMillis);
            curTs.setNanos(0);
            // 1. insert two records, one expired, one unexpired
            MutationResult res = client.insert(tableName).setRowKey(colVal(keyCol, keyIds[0]))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, null)).execute();
            Assert.assertEquals(1, res.getAffectedRows());
            res = client.insert(tableName).setRowKey(colVal(keyCol, keyIds[1]))
                .addMutateColVal(colVal(valueCol, defaultValue))
                .addMutateColVal(colVal(expireCol, curTs)).execute();
            Assert.assertEquals(1, res.getAffectedRows());

            // 2. query and update all columns
            res = client.update(tableName).addScanRange(keyIds[0], keyIds[1])
                .addMutateColVal(colVal(valueCol, "hello")).addMutateColVal(colVal(intCol, 100L))
                .execute();
            Assert.assertEquals(1, res.getAffectedRows());

            QueryResultSet resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[1])
                .execute();
            Assert.assertEquals(resultSet.cacheSize(), 1);
            resultSet.next();
            Row row = resultSet.getResultRow();
            Assert.assertEquals(row.get(keyCol), keyIds[0]);
            Assert.assertEquals(row.get(intCol), 100L);
            Assert.assertEquals(row.get(valueCol), "hello");
            Assert.assertEquals(1, res.getAffectedRows());

            // 3. query and increment the int column to 200
            res = client.increment(tableName).addScanRange(keyIds[0], keyIds[1])
                .addMutateColVal(colVal(intCol, 100L)).execute();
            Assert.assertEquals(1, res.getAffectedRows());
            resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[1]).execute();
            Assert.assertEquals(resultSet.cacheSize(), 1);
            resultSet.next();
            row = resultSet.getResultRow();
            Assert.assertEquals(row.get(keyCol), keyIds[0]);
            Assert.assertEquals(row.get(intCol), 200L);

            // 4. query and append the value column to "hello world!"
            res = client.append(tableName).addScanRange(keyIds[0], keyIds[1])
                .addMutateColVal(colVal(valueCol, ", world!")).execute();
            Assert.assertEquals(1, res.getAffectedRows());
            resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[1]).execute();
            Assert.assertEquals(resultSet.cacheSize(), 1);
            resultSet.next();
            row = resultSet.getResultRow();
            Assert.assertEquals(row.get(keyCol), keyIds[0]);
            Assert.assertEquals(row.get(valueCol), "hello, world!");

            // 5. query and delete
            res = client.delete(tableName).addScanRange(keyIds[0], keyIds[1]).execute();
            Assert.assertEquals(1, res.getAffectedRows());

            resultSet = client.query(tableName).addScanRange(keyIds[0], keyIds[1]).execute();
            Assert.assertEquals(resultSet.cacheSize(), 0);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        } finally {
            client.delete(tableName).addScanRange(keyIds[0], keyIds[1]).execute();
        }
    }
}
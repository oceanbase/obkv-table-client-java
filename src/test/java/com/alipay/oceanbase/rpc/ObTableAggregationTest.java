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

import com.alipay.oceanbase.rpc.bolt.ObTableClientTestBase;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTablePartitionConsistentException;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.filter.*;
import com.alipay.oceanbase.rpc.location.model.ObServerAddr;
import com.alipay.oceanbase.rpc.location.model.ServerRoster;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.*;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.stream.async.ObTableQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.table.ObTableClientQueryAsyncImpl;
import com.alipay.oceanbase.rpc.table.ObTableClientQueryImpl;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import com.alipay.oceanbase.rpc.util.ObTableHotkeyThrottleUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.*;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ObTableAggregationTest {
    private ObTableClient client;

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
        obTableClient.addProperty(Property.SERVER_ENABLE_REROUTING.getKey(), "False");
        obTableClient.init();

        this.client = obTableClient;
    }

    @Test
    // Test aggregation
    public void testAggregation() throws Exception {

        /*
         * CREATE TABLE test_aggregation (
         *   `c1` varchar(255),
         *   `c2` int NOT NULL,
         *   `c3` bigint NOT NULL,
         *   `c4` float NOT NULL,
         *   `c5` double NOT NULL,
         *   `c6` tinyint NULL,
         *   `c7` datetime,
         *   PRIMARY KEY(`c1`)
         * );
         * */

        final ObTableClient client = (ObTableClient) this.client;
        client.addRowKeyElement("test_aggregation", new String[] { "c1" });

        SimpleDateFormat sdf = new SimpleDateFormat(" yyyy-MM-dd HH:mm:ss ");
        Date date1 = sdf.parse(" 2001-07-10 19:20:00 ");
        Date date2 = sdf.parse(" 2002-07-10 19:20:00 ");
        Date date3 = sdf.parse(" 2003-07-10 19:20:00 ");

        try {
            client.insert("test_aggregation", "first_row", new String[] { "c2", "c3", "c4", "c5",
                    "c6", "c7" }, new Object[] { 1, 1L, 1.0f, 1.0, (byte) 1, date1 });
            client.insert("test_aggregation", "second_row", new String[] { "c2", "c3", "c4", "c5",
                    "c6", "c7" }, new Object[] { 2, 2L, 2.0f, 2.0, (byte) 2, date2 });
            client.insert("test_aggregation", "third_row", new String[] { "c2", "c3", "c4", "c5",
                    "c6", "c7" }, new Object[] { 3, 3L, 3.0f, 3.0, (byte) 3, date3 });
            ObTableAggregation obtableAggregation = client.aggregate("test_aggregation");

            // test int
            obtableAggregation.max("c2");
            obtableAggregation.min("c2");
            obtableAggregation.count();
            obtableAggregation.sum("c2");
            obtableAggregation.avg("c2");

            // test bigint
            obtableAggregation.max("c3");
            obtableAggregation.min("c3");
            obtableAggregation.count();
            obtableAggregation.sum("c3");
            obtableAggregation.avg("c3");

            // test float
            obtableAggregation.max("c4");
            obtableAggregation.min("c4");
            obtableAggregation.count();
            obtableAggregation.sum("c4");
            obtableAggregation.avg("c4");

            // test double
            obtableAggregation.max("c5");
            obtableAggregation.min("c5");
            obtableAggregation.count();
            obtableAggregation.sum("c5");
            obtableAggregation.avg("c5");

            // test tinyint
            obtableAggregation.max("c6");
            obtableAggregation.min("c6");
            obtableAggregation.count();
            obtableAggregation.sum("c6");
            obtableAggregation.avg("c6");

            // test date
            obtableAggregation.max("c7");
            obtableAggregation.min("c7");

            // execute
            ObTableAggregationResult obtableAggregationResult = obtableAggregation.execute();

            // test int
            Assert.assertEquals(3, obtableAggregationResult.get("max(c2)"));
            Assert.assertEquals(1, obtableAggregationResult.get("min(c2)"));
            Assert.assertEquals(3L, obtableAggregationResult.get("count(*)"));
            Assert.assertEquals(6L, obtableAggregationResult.get("sum(c2)"));
            Assert.assertEquals(2.0, obtableAggregationResult.get("avg(c2)"));

            // test bigint
            Assert.assertEquals(3L, obtableAggregationResult.get("max(c3)"));
            Assert.assertEquals(1L, obtableAggregationResult.get("min(c3)"));
            Assert.assertEquals(3L, obtableAggregationResult.get("count(*)"));
            Assert.assertEquals(6L, obtableAggregationResult.get("sum(c3)"));
            Assert.assertEquals(2.0, obtableAggregationResult.get("avg(c3)"));

            // test float
            Assert.assertEquals(3.0f, obtableAggregationResult.get("max(c4)"));
            Assert.assertEquals(1.0f, obtableAggregationResult.get("min(c4)"));
            Assert.assertEquals(3L, obtableAggregationResult.get("count(*)"));
            Assert.assertEquals(6.0, obtableAggregationResult.get("sum(c4)"));
            Assert.assertEquals(2.0, obtableAggregationResult.get("avg(c4)"));

            // test double
            Assert.assertEquals(3.0, obtableAggregationResult.get("max(c5)"));
            Assert.assertEquals(1.0, obtableAggregationResult.get("min(c5)"));
            Assert.assertEquals(3L, obtableAggregationResult.get("count(*)"));
            Assert.assertEquals(6.0, obtableAggregationResult.get("sum(c5)"));
            Assert.assertEquals(2.0, obtableAggregationResult.get("avg(c5)"));

            // test tinyint
            Assert.assertEquals((byte) 3, obtableAggregationResult.get("max(c6)"));
            Assert.assertEquals((byte) 1, obtableAggregationResult.get("min(c6)"));
            Assert.assertEquals(3L, obtableAggregationResult.get("count(*)"));
            Assert.assertEquals(6L, obtableAggregationResult.get("sum(c6)"));
            Assert.assertEquals(2.0, obtableAggregationResult.get("avg(c6)"));

            //test date
            Assert.assertEquals(date3, obtableAggregationResult.get("max(c7)"));
            Assert.assertEquals(date1, obtableAggregationResult.get("min(c7)"));

        } finally {
            client.delete("test_aggregation", "first_row");
            client.delete("test_aggregation", "second_row");
            client.delete("test_aggregation", "third_row");
        }
    }

    @Test
    // Test aggregation of multiple aggregation
    public void testPartitionAggregation() throws Exception {

        /*
         * CREATE TABLE `test_partition_aggregation` (
         *`c1` bigint NOT NULL,
         *`c2` bigint DEFAULT NULL,
         *PRIMARY KEY (`c1`))partition by range(`c1`)(partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));
         */

        final ObTableClient client = (ObTableClient) this.client;
        client.addRowKeyElement("test_partition_aggregation", new String[] { "c1" });

        // test one partition
        try {
            client.insert("test_partition_aggregation", new Object[] { 50L },
                new String[] { "c2" }, new Object[] { 50L });
            client.insert("test_partition_aggregation", new Object[] { 150L },
                new String[] { "c2" }, new Object[] { 150L });
            client.insert("test_partition_aggregation", new Object[] { 300L },
                new String[] { "c2" }, new Object[] { 300L });

            ObTableAggregation obtableAggregation = client.aggregate("test_partition_aggregation");

            obtableAggregation.addScanRange(new Object[] { 0L }, new Object[] { 150L });

            // test
            obtableAggregation.max("c2");
            obtableAggregation.min("c2");
            obtableAggregation.count();
            obtableAggregation.sum("c2");
            obtableAggregation.avg("c2");

            ObTableException obTableException = null;

            try {
                // execute
                ObTableAggregationResult obtableAggregationResult = obtableAggregation.execute();

                // test
                Assert.assertEquals(150L, obtableAggregationResult.get("max(c2)"));
                Assert.assertEquals(50L, obtableAggregationResult.get("min(c2)"));
                Assert.assertEquals(2L, obtableAggregationResult.get("count(*)"));
                Assert.assertEquals(200L, obtableAggregationResult.get("sum(c2)"));
                Assert.assertEquals(100.0, obtableAggregationResult.get("avg(c2)"));

            } catch (ObTableException e) {
                System.out.println(e.getMessage());
                System.out.println(e.getErrorCode());
                fail();
            }

        } finally {
            client.delete("test_partition_aggregation", 50L);
            client.delete("test_partition_aggregation", 150L);
            client.delete("test_partition_aggregation", 300L);
        }

        // test multiple partitions
        try {
            client.insert("test_partition_aggregation", new Object[] { 50L },
                new String[] { "c2" }, new Object[] { 50L });
            client.insert("test_partition_aggregation", new Object[] { 150L },
                new String[] { "c2" }, new Object[] { 150L });
            client.insert("test_partition_aggregation", new Object[] { 300L },
                new String[] { "c2" }, new Object[] { 300L });

            ObTableAggregation obtableAggregation = client.aggregate("test_partition_aggregation");

            obtableAggregation.addScanRange(new Object[] { 0L }, new Object[] { 300L });

            // test
            obtableAggregation.max("c2");
            obtableAggregation.min("c2");
            obtableAggregation.count();
            obtableAggregation.sum("c2");
            obtableAggregation.avg("c2");

            ObTableException obTableException = null;

            try {
                // execute
                ObTableAggregationResult obtableAggregationResult = obtableAggregation.execute();

                // test
                Assert.assertEquals(150L, obtableAggregationResult.get("max(c2)"));
                Assert.assertEquals(50L, obtableAggregationResult.get("min(c2)"));
                Assert.assertEquals(2L, obtableAggregationResult.get("count(*)"));
                Assert.assertEquals(200L, obtableAggregationResult.get("sum(c2)"));
                Assert.assertEquals(100.0, obtableAggregationResult.get("avg(c2)"));

            } catch (ObTableException e) {
                obTableException = e;
                assertNotNull(obTableException);
                assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode, e.getErrorCode());
            }

        } finally {
            client.delete("test_partition_aggregation", 50L);
            client.delete("test_partition_aggregation", 150L);
            client.delete("test_partition_aggregation", 300L);
        }
    }

    @Test
    // Test aggregation with filter
    public void testAggregationWithFilter() throws Exception {

        /*
         * CREATE TABLE `test_partition_aggregation` (
         *`c1` bigint NOT NULL,
         *`c2` bigint DEFAULT NULL,
         *PRIMARY KEY (`c1`))partition by range(`c1`)(partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));
         */

        final ObTableClient client = (ObTableClient) this.client;
        client.addRowKeyElement("test_partition_aggregation", new String[] { "c1" });

        // test without filter
        try {
            client.insert("test_partition_aggregation", new Object[] { 50L },
                new String[] { "c2" }, new Object[] { 50L });
            client.insert("test_partition_aggregation", new Object[] { 100L },
                new String[] { "c2" }, new Object[] { 100L });
            client.insert("test_partition_aggregation", new Object[] { 120L },
                new String[] { "c2" }, new Object[] { 300L });
            client.insert("test_partition_aggregation", new Object[] { 130L },
                new String[] { "c2" }, new Object[] { 300L });

            // without filter
            ObTableAggregation obtableAggregationWithoutFilter = client
                .aggregate("test_partition_aggregation");

            // test
            obtableAggregationWithoutFilter.max("c2");
            obtableAggregationWithoutFilter.min("c2");
            obtableAggregationWithoutFilter.count();
            obtableAggregationWithoutFilter.sum("c2");
            obtableAggregationWithoutFilter.avg("c2");

            // execute
            ObTableAggregationResult obtableAggregationResultWithoutFilter = obtableAggregationWithoutFilter
                .execute();

            // test without filter
            Assert.assertEquals(300L, obtableAggregationResultWithoutFilter.get("max(c2)"));
            Assert.assertEquals(50L, obtableAggregationResultWithoutFilter.get("min(c2)"));
            Assert.assertEquals(4L, obtableAggregationResultWithoutFilter.get("count(*)"));
            Assert.assertEquals(750L, obtableAggregationResultWithoutFilter.get("sum(c2)"));
            Assert.assertEquals(187.5, obtableAggregationResultWithoutFilter.get("avg(c2)"));

            // with filter
            ObTableAggregation obtableAggregationWithFilter = client
                .aggregate("test_partition_aggregation");

            // test
            obtableAggregationWithFilter.max("c2");
            obtableAggregationWithFilter.min("c2");
            obtableAggregationWithFilter.count();
            obtableAggregationWithFilter.sum("c2");
            obtableAggregationWithFilter.avg("c2");

            // filter
            ObTableValueFilter filter = new ObTableValueFilter(ObCompareOp.GT, "c1", 90L);

            // add filter
            obtableAggregationWithFilter.setFilter(filter);

            // execute
            ObTableAggregationResult obtableAggregationResultWithFilter = obtableAggregationWithFilter
                .execute();

            // test with filter
            double delta = 1e-6;
            Assert.assertEquals(300L, obtableAggregationResultWithFilter.get("max(c2)"));
            Assert.assertEquals(100L, obtableAggregationResultWithFilter.get("min(c2)"));
            Assert.assertEquals(3L, obtableAggregationResultWithFilter.get("count(*)"));
            Assert.assertEquals(700L, obtableAggregationResultWithFilter.get("sum(c2)"));
            Assert.assertEquals(233.33333333,
                (double) obtableAggregationResultWithFilter.get("avg(c2)"), delta);

            // with filter
            ObTableAggregation obtableAggregationWithFilterAndLimit = client
                .aggregate("test_partition_aggregation");

            // test
            obtableAggregationWithFilterAndLimit.max("c2");
            obtableAggregationWithFilterAndLimit.min("c2");
            obtableAggregationWithFilterAndLimit.count();
            obtableAggregationWithFilterAndLimit.sum("c2");
            obtableAggregationWithFilterAndLimit.avg("c2");

            // add filter
            obtableAggregationWithFilterAndLimit.setFilter(filter);

            // add limit
            obtableAggregationWithFilterAndLimit.limit(0, 2);

            // execute
            ObTableAggregationResult obtableAggregationResultWithFilterAndLimit = obtableAggregationWithFilterAndLimit
                .execute();

            // test with filter and limit
            Assert.assertEquals(300L, obtableAggregationResultWithFilterAndLimit.get("max(c2)"));
            Assert.assertEquals(100L, obtableAggregationResultWithFilterAndLimit.get("min(c2)"));
            Assert.assertEquals(2L, obtableAggregationResultWithFilterAndLimit.get("count(*)"));
            Assert.assertEquals(400L, obtableAggregationResultWithFilterAndLimit.get("sum(c2)"));
            Assert.assertEquals(200.0, obtableAggregationResultWithFilterAndLimit.get("avg(c2)"));

        } finally {
            client.delete("test_partition_aggregation", 50L);
            client.delete("test_partition_aggregation", 100L);
            client.delete("test_partition_aggregation", 120L);
            client.delete("test_partition_aggregation", 130L);
        }
    }

    @Test
    // Test aggregation with empty table
    public void testAggregationWithEmptyRow() throws Exception {
        /*
         * CREATE TABLE `test_partition_aggregation` (
         *`c1` bigint NOT NULL,
         *`c2` bigint DEFAULT NULL,
         *PRIMARY KEY (`c1`))partition by range(`c1`)(partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));
         */

        final ObTableClient client = (ObTableClient) this.client;
        client.addRowKeyElement("test_partition_aggregation", new String[] { "c1" });
        // aggregate without insert
        ObTableAggregation obtableAggregation = client.aggregate("test_partition_aggregation");

        // test
        obtableAggregation.max("c2");
        obtableAggregation.min("c2");

        // execute
        ObTableAggregationResult obtableAggregationResult = obtableAggregation.execute();

        // test null
        Assert.assertNull(obtableAggregationResult.get("max(c2)"));
        Assert.assertNull(obtableAggregationResult.get("min(c2)"));
    }

    @Test
    // Test aggregation exist null
    public void testAggregationExistNull() throws Exception {

        /*
         * CREATE TABLE `test_partition_aggregation` (
         *`c1` bigint NOT NULL,
         *`c2` bigint DEFAULT NULL,
         *PRIMARY KEY (`c1`))partition by range(`c1`)(partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));
         */

        final ObTableClient client = (ObTableClient) this.client;
        client.addRowKeyElement("test_partition_aggregation", new String[] { "c1" });

        // test with null
        try {
            client.insert("test_partition_aggregation", new Object[] { 50L },
                new String[] { "c2" }, new Object[] { null });
            client.insert("test_partition_aggregation", new Object[] { 100L },
                new String[] { "c2" }, new Object[] { null });

            // with null
            ObTableAggregation obtableAggregationWithoutFilter = client
                .aggregate("test_partition_aggregation");

            // test
            obtableAggregationWithoutFilter.max("c1");
            obtableAggregationWithoutFilter.min("c1");
            obtableAggregationWithoutFilter.max("c2");
            obtableAggregationWithoutFilter.min("c2");

            // execute
            ObTableAggregationResult obtableAggregationResultWithoutFilter = obtableAggregationWithoutFilter
                .execute();

            // test with null
            Assert.assertEquals(100L, obtableAggregationResultWithoutFilter.get("max(c1)"));
            Assert.assertEquals(50L, obtableAggregationResultWithoutFilter.get("min(c1)"));

            Assert.assertEquals(null, obtableAggregationResultWithoutFilter.get("max(c2)"));
            Assert.assertEquals(null, obtableAggregationResultWithoutFilter.get("min(c2)"));

        } finally {
            client.delete("test_partition_aggregation", 50L);
            client.delete("test_partition_aggregation", 100L);
        }
    }

    @Test
    // Test aggregation with illegal column
    public void testAggregationWithIllegalColumn() throws Exception {

        /*
         * CREATE TABLE `test_partition_aggregation` (
         *`c1` bigint NOT NULL,
         *`c2` bigint DEFAULT NULL,
         *PRIMARY KEY (`c1`))partition by range(`c1`)(partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));
         */

        final ObTableClient client = (ObTableClient) this.client;
        client.addRowKeyElement("test_partition_aggregation", new String[] { "c1" });

        // test with null
        try {
            client.insert("test_partition_aggregation", new Object[] { 50L },
                new String[] { "c2" }, new Object[] { null });
            client.insert("test_partition_aggregation", new Object[] { 100L },
                new String[] { "c2" }, new Object[] { null });

            // with
            ObTableAggregation obtableAggregationWithIllegal = client
                .aggregate("test_partition_aggregation");

            // test illegal column
            obtableAggregationWithIllegal.max("c3");

            try {
                obtableAggregationWithIllegal.execute();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof ObTableUnexpectedException);
                Assert.assertEquals(ResultCodes.OB_ERR_UNEXPECTED.errorCode,
                    ((ObTableUnexpectedException) e).getErrorCode());
            }

        } finally {
            client.delete("test_partition_aggregation", 50L);
            client.delete("test_partition_aggregation", 100L);
        }
    }

    /*
    CREATE TABLE test_aggregation (
    `c1` varchar(255),
    `c2` int NOT NULL,
    `c3` bigint NOT NULL,
    `c4` float NOT NULL,
    `c5` double NOT NULL,
    `c6` tinyint NULL,
    `c7` datetime,
    PRIMARY KEY(`c1`)
    );
     */

    @Test
    // Test aggregation with big int , should report error out of range
    public void testAggregationWithBigint() throws Exception {
        final String TABLE_NAME = "test_aggregation";
        try {
            final ObTableClient client = (ObTableClient) this.client;
            client.addRowKeyElement(TABLE_NAME, new String[] { "c1" });
            SimpleDateFormat sdf = new SimpleDateFormat(" yyyy-MM-dd HH:mm:ss ");
            Date date1 = sdf.parse(" 1000-12-10 19:20:00 ");
            Date date2 = sdf.parse(" 1010-07-10 19:20:00 ");
            Date date3 = sdf.parse(" 1100-02-10 19:20:00 ");

            client.insert(TABLE_NAME, "first_row", new String[] { "c2", "c3", "c4", "c5", "c6",
                    "c7" }, new Object[] { 1, 9223372036854775807L, 1.0f, 1.0, (byte) 1, date1 });
            client.insert(TABLE_NAME, "second_row", new String[] { "c2", "c3", "c4", "c5", "c6",
                    "c7" }, new Object[] { 2, 9223372036854775807L, 2.0f, 2.0, (byte) 2, date2 });
            client.insert(TABLE_NAME, "third_row", new String[] { "c2", "c3", "c4", "c5", "c6",
                    "c7" }, new Object[] { 3, 9223372036854775807L, 3.0f, 3.0, (byte) 3, date3 });
            ObTableAggregation obtableAggregation = client.aggregate(TABLE_NAME);

            // test
            obtableAggregation.sum("c3");

            ObTableAggregationResult obtableAggregationResult = obtableAggregation.execute();

            Assert.assertEquals(10L, obtableAggregationResult.get("sum(c3)"));

        } catch (Exception e) {
            Assert.assertTrue(((ObTableException) e).getMessage().contains(
                "[OB_DATA_OUT_OF_RANGE][Out of range value for column 'sum(c3)' at row 0]"));
        } finally {
        }
    }

    @Test
    // Test aggregation with empty table
    public void testAggregationEmptyVal() throws Exception {

        /*
         * CREATE TABLE `test_partition_aggregation` (
         *`c1` bigint NOT NULL,
         *`c2` bigint DEFAULT NULL,
         *PRIMARY KEY (`c1`))partition by range(`c1`)(partition p0 values less than(200), partition p1 values less than(500), partition p2 values less than(900));
         */

        final ObTableClient client = (ObTableClient) this.client;
        client.addRowKeyElement("test_partition_aggregation", new String[] { "c1" });

        try {
            // with filter
            ObTableAggregation obtableAggregationWithFilter = client
                .aggregate("test_partition_aggregation");

            // test
            obtableAggregationWithFilter.max("c2");
            obtableAggregationWithFilter.min("c2");
            obtableAggregationWithFilter.count();
            obtableAggregationWithFilter.sum("c2");
            obtableAggregationWithFilter.avg("c2");

            // filter
            ObTableValueFilter filter = new ObTableValueFilter(ObCompareOp.GT, "c1", 90L);

            // add filter
            obtableAggregationWithFilter.setFilter(filter);

            // execute
            ObTableAggregationResult obtableAggregationResultWithFilter = obtableAggregationWithFilter
                .execute();

            // empty table generate null row
            Assert.assertEquals(5, obtableAggregationResultWithFilter.getRow().size());
            Assert.assertEquals(null, obtableAggregationResultWithFilter.get("max(c2)"));
            Assert.assertEquals(null, obtableAggregationResultWithFilter.get("min(c2)"));
            Assert.assertEquals(null, obtableAggregationResultWithFilter.get("count()"));
            Assert.assertEquals(null, obtableAggregationResultWithFilter.get("sum(c2)"));
            Assert.assertEquals(null, obtableAggregationResultWithFilter.get("avg(c2)"));

        } finally {
        }
    }

}

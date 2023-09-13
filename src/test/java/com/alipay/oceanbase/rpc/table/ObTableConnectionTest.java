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

package com.alipay.oceanbase.rpc.table;

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.bolt.ObTableClientTestBase;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ObTableConnectionTest extends ObTableClientTestBase {
    ObTableClient           obTableClient;
    public static final int TEST_CONNECTION_POOL_SIZE = 3;
    public static final int TEST_NETTY_HIGH_WATERMARK = 128 * 1024;
    public static final int TEST_NETTY_LOW_WATERMARK  = 64 * 1024;
    public static final int TEST_NETTY_WAIT_INTERVAL  = 2;

    @Before
    public void setup() throws Exception {
        obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(),
            Integer.toString(TEST_CONNECTION_POOL_SIZE));
        obTableClient.init();

        client = obTableClient;
        syncRefreshMetaHelper(obTableClient);
    }

    @BeforeClass
    public static void testVersion() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        if (ObGlobal.obVsnMajor() <= 0) {
            // ob version is invalid
            Assert.assertTrue(false);
        } else if (ObGlobal.obVsnMajor() != 3) {
            // todo: only support in 3.x currently
            Assert.assertTrue(false);
        }
    }

    @Test
    public void testVarcharConcurrent() throws Exception {
        obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(),
            Integer.toString(TEST_CONNECTION_POOL_SIZE));
        obTableClient.init();
        syncRefreshMetaHelper(obTableClient);

        test_varchar_helper_thread(obTableClient, "T101", 100);
        test_varchar_helper_thread(obTableClient, "T102", 100);
        test_varchar_helper_thread(obTableClient, "T103", 100);
    }

    @Test
    public void testConnectionPoolSize() throws Exception {
        if (obTableClient.isOdpMode()) {
            assertEquals(TEST_CONNECTION_POOL_SIZE, obTableClient.getOdpTable()
                .getObTableConnectionPoolSize());
        } else {
            ObPair<Long, ObTableParam> obPair = obTableClient.getTable("test_varchar_table",
                new String[] { "abc" }, false, false);
            int poolSize = obPair.getRight().getObTable().getObTableConnectionPoolSize();
            assertEquals(TEST_CONNECTION_POOL_SIZE, poolSize);
        }
    }

    @Test
    public void testWatermarkSetting() throws Exception {
        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();

        obTableClient.addProperty(Property.NETTY_BUFFER_LOW_WATERMARK.getKey(),
            Integer.toString(TEST_NETTY_LOW_WATERMARK));
        obTableClient.addProperty(Property.NETTY_BUFFER_HIGH_WATERMARK.getKey(),
            Integer.toString(TEST_NETTY_HIGH_WATERMARK));
        obTableClient.addProperty(Property.NETTY_BLOCKING_WAIT_INTERVAL.getKey(),
            Integer.toString(TEST_NETTY_WAIT_INTERVAL));
        obTableClient.init();

        if (obTableClient.isOdpMode()) {
            assertEquals(TEST_NETTY_LOW_WATERMARK, obTableClient.getOdpTable()
                .getNettyBufferLowWatermark());
            assertEquals(TEST_NETTY_HIGH_WATERMARK, obTableClient.getOdpTable()
                .getNettyBufferHighWatermark());
            assertEquals(TEST_NETTY_WAIT_INTERVAL, obTableClient.getOdpTable()
                .getNettyBlockingWaitInterval());
        } else {
            ObPair<Long, ObTableParam> obPair = obTableClient.getTable("test_varchar_table",
                new String[] { "abc" }, false, false);
            int lowWatermark = obPair.getRight().getObTable().getNettyBufferLowWatermark();
            int highWatermark = obPair.getRight().getObTable().getNettyBufferHighWatermark();
            int waitInterval = obPair.getRight().getObTable().getNettyBlockingWaitInterval();

            assertEquals(TEST_NETTY_LOW_WATERMARK, lowWatermark);
            assertEquals(TEST_NETTY_HIGH_WATERMARK, highWatermark);
            assertEquals(TEST_NETTY_WAIT_INTERVAL, waitInterval);
        }
    }

    @Test
    public void testDefaultWatermark() throws Exception {
        if (obTableClient.isOdpMode()) {
            // do nothing
        } else {
            ObPair<Long, ObTableParam> obPair = obTableClient.getTable("test_varchar_table",
                new String[] { "abc" }, false, false);
            int lowWatermark = obPair.getRight().getObTable().getNettyBufferLowWatermark();
            int highWatermark = obPair.getRight().getObTable().getNettyBufferHighWatermark();
            int waitInterval = obPair.getRight().getObTable().getNettyBlockingWaitInterval();

            assertEquals(Property.NETTY_BUFFER_LOW_WATERMARK.getDefaultInt(), lowWatermark);
            assertEquals(Property.NETTY_BUFFER_HIGH_WATERMARK.getDefaultInt(), highWatermark);
            assertEquals(Property.NETTY_BLOCKING_WAIT_INTERVAL.getDefaultInt(), waitInterval);
        }
    }
}

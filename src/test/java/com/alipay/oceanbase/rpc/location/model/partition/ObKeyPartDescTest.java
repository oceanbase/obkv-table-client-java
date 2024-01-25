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

package com.alipay.oceanbase.rpc.location.model.partition;

import com.alipay.oceanbase.rpc.location.LocationUtil;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnExpressParser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.rpc.location.model.partition.ObPartFuncType.KEY_V3;

public class ObKeyPartDescTest {

    private ObKeyPartDesc keyBinary;

    private ObKeyPartDesc keyUtf8_CI;

    private ObKeyPartDesc keyUtf8;

    @Before
    public void setUp() {
        keyBinary = new ObKeyPartDesc();
        keyBinary.setPartFuncType(KEY_V3);
        keyBinary.setPartExpr("k_prefix");
        keyBinary.setPartNum(16);
        keyBinary.setPartSpace(0);
        Map<String, Long> partNameIdMap = LocationUtil.buildDefaultPartNameIdMap(keyBinary
            .getPartNum());
        keyBinary.setPartNameIdMap(partNameIdMap);
        ObColumn column = new ObGeneratedColumn("k_prefix",//
            0,//
            ObObjType.valueOf(22),//
            ObCollationType.valueOf(63),
            new ObGeneratedColumnExpressParser("substr(K, 1, 4)").parse());

        List<ObColumn> partColumns = new ArrayList<ObColumn>();
        partColumns.add(column);
        keyBinary.setPartColumns(partColumns);
        keyBinary.setRowKeyElement(TableEntry.HBASE_ROW_KEY_ELEMENT);
        keyBinary.prepare();

        keyUtf8_CI = new ObKeyPartDesc();
        keyUtf8_CI.setPartFuncType(KEY_V3);
        keyUtf8_CI.setPartExpr("k_prefix");
        keyUtf8_CI.setPartNum(16);
        keyUtf8_CI.setPartSpace(0);
        partNameIdMap = LocationUtil.buildDefaultPartNameIdMap(keyUtf8_CI.getPartNum());
        keyUtf8_CI.setPartNameIdMap(partNameIdMap);
        column = new ObGeneratedColumn("k_prefix",//
            0,//
            ObObjType.valueOf(22),//
            ObCollationType.valueOf(45),
            new ObGeneratedColumnExpressParser("substr(K, 1, 4)").parse());

        partColumns = new ArrayList<ObColumn>();
        partColumns.add(column);
        keyUtf8_CI.setPartColumns(partColumns);
        keyUtf8_CI.setRowKeyElement(TableEntry.HBASE_ROW_KEY_ELEMENT);
        keyUtf8_CI.prepare();

        keyUtf8 = new ObKeyPartDesc();
        keyUtf8.setPartFuncType(KEY_V3);
        keyUtf8.setPartExpr("k_prefix");
        keyUtf8.setPartNum(16);
        keyUtf8.setPartSpace(0);
        partNameIdMap = LocationUtil.buildDefaultPartNameIdMap(keyUtf8.getPartNum());
        keyUtf8.setPartNameIdMap(partNameIdMap);
        column = new ObGeneratedColumn("k_prefix",//
            0,//
            ObObjType.valueOf(22),//
            ObCollationType.valueOf(46),
            new ObGeneratedColumnExpressParser("substr(K, 1, 4)").parse());

        partColumns = new ArrayList<ObColumn>();
        partColumns.add(column);
        keyUtf8.setPartColumns(partColumns);
        keyUtf8.setRowKeyElement(TableEntry.HBASE_ROW_KEY_ELEMENT);
        keyUtf8.prepare();
    }

    @Test
    public void testGetPartId() {
        // key binary

        long partId = keyBinary.getPartId("partition_1", "column_1", System.currentTimeMillis());
        Assert.assertEquals(11, partId);

        Assert.assertEquals(
            keyBinary.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyBinary.getPartId("partition_2", "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            keyBinary.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyBinary.getPartId("partition_1".getBytes(), "column_1", System.currentTimeMillis()));

        Assert.assertEquals(keyBinary.getPartId("test_1", "column_1", System.currentTimeMillis()),
            keyBinary.getPartId("test_2", "column_1", System.currentTimeMillis()));

        Assert.assertEquals(keyBinary.getPartId("test_1", "column_1", System.currentTimeMillis()),
            keyBinary.getPartId("test_2".getBytes(), "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            keyBinary.getPartId("test_1".getBytes(), "column_1", System.currentTimeMillis()),
            keyBinary.getPartId("test_2".getBytes(), "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            keyUtf8_CI.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyUtf8_CI.getPartId("Partition_1".getBytes(), "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            keyUtf8_CI.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyUtf8_CI.getPartId("Partition_2".getBytes(), "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            keyUtf8.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyUtf8.getPartId("partition_2", "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            keyUtf8.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyUtf8.getPartId("partition_2".getBytes(), "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            keyUtf8.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyUtf8.getPartId("partition_2", "column_1", System.currentTimeMillis()));

        Assert.assertNotEquals(
            keyUtf8.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyUtf8.getPartId("Partition_1", "column_1", System.currentTimeMillis()));

        Assert.assertNotEquals(
            keyUtf8_CI.getPartId("PARTITION_1", "column_1", System.currentTimeMillis()),
            keyUtf8.getPartId("partition_1".getBytes(), "column_1", System.currentTimeMillis()));

        Assert.assertNotEquals(
            keyUtf8_CI.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            keyBinary.getPartId("PARTITION_1".getBytes(), "column_1", System.currentTimeMillis()));
    }

    @Test
    public void testGetPartIds() {
        long timestamp = System.currentTimeMillis();
        Object[] startKey1 = new Object[] { "partition_1", "column_1", timestamp };
        Object[] endKey1 = new Object[] { "partition_2", "column_1", timestamp };

        Object[] startKey2 = new Object[] { "partition_1".getBytes(), "column_1", timestamp };
        Object[] endKey2 = new Object[] { "partition_2".getBytes(), "column_1", timestamp };

        Object[] startKey3 = new Object[] { "test_1".getBytes(), "column_1", timestamp };
        Object[] endKey3 = new Object[] { "test_2".getBytes(), "column_1", timestamp };

        Object[] startKey4 = new Object[] { "PARTITION_1", "column_1", timestamp };
        Object[] endKey4 = new Object[] { "PARTITION_2", "column_1", timestamp };

        Object[] startKey5 = new Object[] { "PARTITION_1".getBytes(), "column_1", timestamp };
        Object[] endKey5 = new Object[] { "PARTITION_2".getBytes(), "column_1", timestamp };

        Object[] startKey6 = new Object[] { "TEST_1".getBytes(), "column_1", timestamp };
        Object[] endKey6 = new Object[] { "TEST_2".getBytes(), "column_1", timestamp };

        Assert.assertEquals(keyBinary.getPartIds(startKey1, true, endKey1, true),
            keyBinary.getPartIds(startKey2, true, endKey2, true));

        Assert.assertEquals(keyBinary.getPartIds(startKey1, true, endKey2, true),
            keyBinary.getPartIds(startKey2, true, endKey1, true));

        Assert.assertEquals(keyBinary.getPartIds(startKey1, false, endKey2, false),
            keyBinary.getPartIds(startKey2, false, endKey1, false));

        try {
            List<Long> ans = keyBinary.getPartIds(startKey1, false, endKey3, false);
            Assert.assertEquals(16, ans.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        try {
            List<Long> ans = keyBinary.getPartIds(startKey3, false, endKey1, true);
            Assert.assertEquals(16, ans.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        try {
            List<Long> ans = keyBinary.getPartIds(startKey1, false, endKey4, true);
            Assert.assertEquals(16, ans.size());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.assertTrue(false);
        }

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey1, true, endKey1, true),
            keyUtf8_CI.getPartIds(startKey4, true, endKey4, true));

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey2, true, endKey2, true),
            keyUtf8_CI.getPartIds(startKey5, true, endKey5, true));

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey3, false, endKey3, false),
            keyUtf8_CI.getPartIds(startKey6, false, endKey6, false));

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey1, true, endKey1, true),
            keyUtf8_CI.getPartIds(startKey2, true, endKey2, true));

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey1, true, endKey2, true),
            keyUtf8_CI.getPartIds(startKey2, true, endKey1, true));

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey1, false, endKey2, false),
            keyUtf8_CI.getPartIds(startKey2, false, endKey1, false));

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey1, true, endKey1, true),
            keyUtf8_CI.getPartIds(startKey4, true, endKey4, true));

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey1, true, endKey2, true),
            keyUtf8_CI.getPartIds(startKey5, true, endKey5, true));

        Assert.assertEquals(keyUtf8_CI.getPartIds(startKey1, false, endKey2, false),
            keyUtf8_CI.getPartIds(startKey5, false, endKey4, false));

        Assert.assertEquals(keyUtf8.getPartIds(startKey1, true, endKey1, true),
            keyUtf8.getPartIds(startKey2, true, endKey2, true));

        Assert.assertEquals(keyUtf8.getPartIds(startKey1, true, endKey2, true),
            keyUtf8.getPartIds(startKey2, true, endKey1, true));

        Assert.assertEquals(keyUtf8.getPartIds(startKey1, false, endKey2, false),
            keyUtf8.getPartIds(startKey2, false, endKey1, false));

        Assert.assertEquals(keyUtf8.getPartIds(startKey1, true, endKey1, true),
            keyUtf8.getPartIds(startKey2, true, endKey2, true));

        Assert.assertEquals(keyUtf8.getPartIds(startKey1, true, endKey2, true),
            keyUtf8.getPartIds(startKey2, true, endKey1, true));

        Assert.assertEquals(keyUtf8.getPartIds(startKey1, false, endKey2, false),
            keyUtf8.getPartIds(startKey2, false, endKey1, false));

        Assert.assertNotEquals(keyUtf8.getPartIds(startKey1, true, endKey1, true),
            keyUtf8.getPartIds(startKey4, true, endKey4, true));

        Assert.assertNotEquals(keyUtf8.getPartIds(startKey2, true, endKey2, true),
            keyUtf8.getPartIds(startKey5, true, endKey5, true));

        Assert.assertNotEquals(keyUtf8.getPartIds(startKey3, false, endKey3, false),
            keyUtf8.getPartIds(startKey6, false, endKey6, false));

    }

    @Test
    public void testGetRandomId() {
        Assert.assertTrue(keyBinary.getRandomPartId() >= 0
                          && keyBinary.getRandomPartId() <= keyBinary.getPartNum());
    }
}

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
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObSimpleColumn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alipay.oceanbase.rpc.location.model.partition.ObPartFuncType.RANGE_COLUMNS;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObSimpleColumn.*;

public class ObRangePartDescTest {
    private ObRangePartDesc rangeBinary;
    private ObRangePartDesc rangeUtf8_CI;
    private ObRangePartDesc rangeUtf8;

    @Before
    public void setUp() {
        rangeBinary = new ObRangePartDesc();
        rangeBinary.setPartFuncType(RANGE_COLUMNS);
        rangeBinary.setPartExpr("K");
        List<ObComparableKV<ObPartitionKey, Long>> bounds = new ArrayList<ObComparableKV<ObPartitionKey, Long>>();
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(
            Collections.singletonList(DEFAULT_BINARY), ObPartitionKey.MAX_PARTITION_ELEMENT), 2L));
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(
            Collections.singletonList(DEFAULT_BINARY),
            ObObjType.ObVarcharType.parseToComparable("h", ObCollationType.CS_TYPE_BINARY)), 0L));
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(
            Collections.singletonList(DEFAULT_BINARY),
            ObObjType.ObVarcharType.parseToComparable("w", ObCollationType.CS_TYPE_BINARY)), 1L));
        Collections.sort(bounds);
        rangeBinary.setBounds(bounds);
        List<ObObjType> types = new ArrayList<ObObjType>();
        types.add(ObObjType.ObVarcharType);
        rangeBinary.setOrderedCompareColumnTypes(types);
        rangeBinary.setPartNameIdMap(LocationUtil.buildDefaultPartNameIdMap(2));

        ObColumn column = new ObSimpleColumn("k",//
            0,//
            ObObjType.valueOf(22),//
            ObCollationType.valueOf(63));

        List<ObColumn> partColumns = new ArrayList<ObColumn>();
        partColumns.add(column);
        rangeBinary.setPartColumns(partColumns);
        rangeBinary.setOrderedCompareColumns(partColumns);
        rangeBinary.setRowKeyElement(TableEntry.HBASE_ROW_KEY_ELEMENT);
        rangeBinary.prepare();

        rangeUtf8_CI = new ObRangePartDesc();
        rangeUtf8_CI.setPartFuncType(RANGE_COLUMNS);
        rangeUtf8_CI.setPartExpr("K");
        bounds = new ArrayList<ObComparableKV<ObPartitionKey, Long>>();
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(
            Collections.singletonList(DEFAULT_UTF8MB4_GENERAL_CI),
            ObPartitionKey.MAX_PARTITION_ELEMENT), 2L));
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(Collections
            .singletonList(DEFAULT_UTF8MB4_GENERAL_CI), ObObjType.ObVarcharType.parseToComparable(
            "h", ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI)), 0L));
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(Collections
            .singletonList(DEFAULT_UTF8MB4_GENERAL_CI), ObObjType.ObVarcharType.parseToComparable(
            "w", ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI)), 1L));
        Collections.sort(bounds);
        rangeUtf8_CI.setBounds(bounds);
        types = new ArrayList<ObObjType>();
        types.add(ObObjType.ObVarcharType);
        rangeUtf8_CI.setOrderedCompareColumnTypes(types);
        rangeUtf8_CI.setPartNameIdMap(LocationUtil.buildDefaultPartNameIdMap(2));

        column = new ObSimpleColumn("k",//
            0,//
            ObObjType.valueOf(22),//
            ObCollationType.valueOf(45));

        partColumns = new ArrayList<ObColumn>();
        partColumns.add(column);
        rangeUtf8_CI.setPartColumns(partColumns);
        rangeUtf8_CI.setOrderedCompareColumns(partColumns);
        rangeUtf8_CI.setRowKeyElement(TableEntry.HBASE_ROW_KEY_ELEMENT);
        rangeUtf8_CI.prepare();

        rangeUtf8 = new ObRangePartDesc();
        rangeUtf8.setPartFuncType(RANGE_COLUMNS);
        rangeUtf8.setPartExpr("K");
        bounds = new ArrayList<ObComparableKV<ObPartitionKey, Long>>();
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(
            Collections.singletonList(DEFAULT_UTF8MB4), ObPartitionKey.MAX_PARTITION_ELEMENT), 2L));
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(
            Collections.singletonList(DEFAULT_UTF8MB4),
            ObObjType.ObVarcharType.parseToComparable("h", ObCollationType.CS_TYPE_UTF8MB4_BIN)),
            0L));
        bounds.add(new ObComparableKV<ObPartitionKey, Long>(ObPartitionKey.getInstance(
            Collections.singletonList(DEFAULT_UTF8MB4),
            ObObjType.ObVarcharType.parseToComparable("w", ObCollationType.CS_TYPE_UTF8MB4_BIN)),
            1L));
        Collections.sort(bounds);
        rangeUtf8.setBounds(bounds);
        types = new ArrayList<ObObjType>();
        types.add(ObObjType.ObVarcharType);
        rangeUtf8.setOrderedCompareColumnTypes(types);
        rangeUtf8.setPartNameIdMap(LocationUtil.buildDefaultPartNameIdMap(2));

        column = new ObSimpleColumn("k",//
            0,//
            ObObjType.valueOf(22),//
            ObCollationType.valueOf(46));

        partColumns = new ArrayList<ObColumn>();
        partColumns.add(column);
        rangeUtf8.setPartColumns(partColumns);
        rangeUtf8.setOrderedCompareColumns(partColumns);
        rangeUtf8.setRowKeyElement(TableEntry.HBASE_ROW_KEY_ELEMENT);
        rangeUtf8.prepare();
    }

    @Test
    public void testGetPartId() {
        long partId = rangeBinary.getPartId("partition_1", "column_1", System.currentTimeMillis());
        Assert.assertEquals(1, partId);

        partId = rangeBinary.getPartId("a", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeBinary.getPartId("x", "column_1", System.currentTimeMillis());
        Assert.assertEquals(2, partId);

        Assert.assertEquals(
            rangeBinary.getPartId("partition_1", "column_1", System.currentTimeMillis()),
            rangeBinary.getPartId("partition_2", "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            rangeBinary.getPartId("test_1", "column_1", System.currentTimeMillis()),
            rangeBinary.getPartId("test_2", "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            rangeBinary.getPartId("test_1", "column_1", System.currentTimeMillis()),
            rangeBinary.getPartId("test_2".getBytes(), "column_1", System.currentTimeMillis()));

        Assert.assertEquals(
            rangeBinary.getPartId("test_1".getBytes(), "column_1", System.currentTimeMillis()),
            rangeBinary.getPartId("test_2".getBytes(), "column_1", System.currentTimeMillis()));

        partId = rangeBinary.getPartId("A", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeBinary.getPartId("P", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeBinary.getPartId("X", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeUtf8_CI.getPartId("a", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeUtf8_CI.getPartId("partition_1", "column_1", System.currentTimeMillis());
        Assert.assertEquals(1, partId);

        partId = rangeUtf8_CI.getPartId("x", "column_1", System.currentTimeMillis());
        Assert.assertEquals(2, partId);

        partId = rangeUtf8_CI.getPartId("A", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeUtf8_CI.getPartId("P", "column_1", System.currentTimeMillis());
        Assert.assertEquals(1, partId);

        partId = rangeUtf8_CI.getPartId("X", "column_1", System.currentTimeMillis());
        Assert.assertEquals(2, partId);

        partId = rangeUtf8.getPartId("a", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeUtf8.getPartId("p", "column_1", System.currentTimeMillis());
        Assert.assertEquals(1, partId);

        partId = rangeUtf8.getPartId("x", "column_1", System.currentTimeMillis());
        Assert.assertEquals(2, partId);

        partId = rangeUtf8.getPartId("A", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeUtf8.getPartId("P", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        partId = rangeUtf8.getPartId("X", "column_1", System.currentTimeMillis());
        Assert.assertEquals(0, partId);

        ArrayList<Object[]> rowKeys = new ArrayList<Object[]>();
        rowKeys.add(new Object[] { "P", "column_1", System.currentTimeMillis() });
        partId = rangeUtf8.getPartId(rowKeys, true);
        Assert.assertEquals(0, partId);
        Assert.assertTrue(rangeUtf8.toString().contains("partExpr"));
    }

    @Test
    public void testGetPartIds() {
        long timestamp = System.currentTimeMillis();
        Object[] startKey1 = new Object[] { "partition_1", "column_1", timestamp };
        Object[] endKey1 = new Object[] { "partition_2", "column_1", timestamp };

        Object[] startKey2 = new Object[] { "partition_1".getBytes(), "column_1", timestamp };
        Object[] endKey2 = new Object[] { "partition_2".getBytes(), "column_1", timestamp };

        Object[] startKey3 = new Object[] { "yes_1".getBytes(), "column_1", timestamp };
        Object[] endKey3 = new Object[] { "yes_2".getBytes(), "column_1", timestamp };

        Object[] startKey4 = new Object[] { "PARTITION_1", "column_1", timestamp };
        Object[] endKey4 = new Object[] { "PARTITION_2", "column_1", timestamp };

        Object[] startKey5 = new Object[] { "PARTITION_1".getBytes(), "column_1", timestamp };
        Object[] endKey5 = new Object[] { "PARTITION_2".getBytes(), "column_1", timestamp };

        Object[] startKey6 = new Object[] { "YES_1".getBytes(), "column_1", timestamp };
        Object[] endKey6 = new Object[] { "YES_2".getBytes(), "column_1", timestamp };

        Assert.assertEquals(rangeBinary.getPartIds(startKey1, true, endKey1, true),
            rangeBinary.getPartIds(startKey2, true, endKey2, true));

        Assert.assertEquals(rangeBinary.getPartIds(startKey1, true, endKey2, true),
            rangeBinary.getPartIds(startKey2, true, endKey1, true));

        Assert.assertEquals(rangeBinary.getPartIds(startKey1, false, endKey2, false),
            rangeBinary.getPartIds(startKey2, false, endKey1, false));

        Assert.assertNotEquals(rangeBinary.getPartIds(startKey1, true, endKey1, true),
            rangeBinary.getPartIds(startKey4, true, endKey4, true));

        Assert.assertNotEquals(rangeBinary.getPartIds(startKey2, true, endKey2, true),
            rangeBinary.getPartIds(startKey5, true, endKey5, true));

        Assert.assertNotEquals(rangeBinary.getPartIds(startKey3, false, endKey3, false),
            rangeBinary.getPartIds(startKey6, false, endKey6, false));

        Assert.assertNotEquals(rangeBinary.getPartIds(startKey1, true, endKey1, true),
            rangeBinary.getPartIds(startKey5, true, endKey5, true));

        Assert.assertNotEquals(rangeBinary.getPartIds(startKey1, true, endKey2, true),
            rangeBinary.getPartIds(startKey5, true, endKey4, true));

        Assert.assertNotEquals(rangeBinary.getPartIds(startKey1, false, endKey2, false),
            rangeBinary.getPartIds(startKey5, false, endKey5, false));

        List<Long> partIds = new ArrayList<Long>();
        partIds.add(1L);
        partIds.add(2L);
        Assert.assertEquals(partIds, rangeBinary.getPartIds(startKey1, false, endKey3, false));

        partIds = new ArrayList<Long>();
        partIds.add(0L);
        Assert.assertEquals(partIds, rangeBinary.getPartIds(startKey4, false, endKey4, false));

        Assert.assertEquals(partIds, rangeBinary.getPartIds(startKey6, false, endKey4, true));

        Assert.assertEquals(0, rangeBinary.getPartIds(startKey3, false, endKey1, true).size());

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey1, true, endKey1, true),
            rangeUtf8_CI.getPartIds(startKey2, true, endKey2, true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey1, true, endKey2, true),
            rangeUtf8_CI.getPartIds(startKey2, true, endKey1, true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey1, false, endKey2, false),
            rangeUtf8_CI.getPartIds(startKey2, false, endKey1, false));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey1, true, endKey1, true),
            rangeUtf8_CI.getPartIds(startKey4, true, endKey4, true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey2, true, endKey2, true),
            rangeUtf8_CI.getPartIds(startKey5, true, endKey5, true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey3, false, endKey3, false),
            rangeUtf8_CI.getPartIds(startKey6, false, endKey6, false));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey1, true, endKey1, true),
            rangeUtf8_CI.getPartIds(startKey5, true, endKey5, true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey1, true, endKey2, true),
            rangeUtf8_CI.getPartIds(startKey5, true, endKey4, true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(startKey1, false, endKey2, false),
            rangeUtf8_CI.getPartIds(startKey5, false, endKey4, false));

        partIds = new ArrayList<Long>();
        partIds.add(1L);
        partIds.add(2L);
        Assert.assertEquals(partIds, rangeUtf8_CI.getPartIds(startKey1, false, endKey3, false));
        Assert.assertEquals(partIds, rangeUtf8_CI.getPartIds(startKey4, false, endKey6, false));
        Assert.assertEquals(0, rangeUtf8_CI.getPartIds(startKey3, false, endKey1, true).size());

        Assert.assertEquals(rangeUtf8.getPartIds(startKey1, true, endKey1, true),
            rangeUtf8.getPartIds(startKey2, true, endKey2, true));

        Assert.assertEquals(rangeUtf8.getPartIds(startKey1, true, endKey2, true),
            rangeUtf8.getPartIds(startKey2, true, endKey1, true));

        Assert.assertEquals(rangeUtf8.getPartIds(startKey1, false, endKey2, false),
            rangeUtf8.getPartIds(startKey2, false, endKey1, false));

        Assert.assertNotEquals(rangeUtf8.getPartIds(startKey1, true, endKey1, true),
            rangeUtf8.getPartIds(startKey4, true, endKey4, true));

        Assert.assertNotEquals(rangeUtf8.getPartIds(startKey2, true, endKey2, true),
            rangeUtf8.getPartIds(startKey5, true, endKey5, true));

        Assert.assertNotEquals(rangeUtf8.getPartIds(startKey3, false, endKey3, false),
            rangeUtf8.getPartIds(startKey6, false, endKey6, false));

        Assert.assertNotEquals(rangeUtf8.getPartIds(startKey1, true, endKey1, true),
            rangeUtf8.getPartIds(startKey5, true, endKey5, true));

        Assert.assertNotEquals(rangeUtf8.getPartIds(startKey1, true, endKey2, true),
            rangeUtf8.getPartIds(startKey5, true, endKey4, true));

        Assert.assertNotEquals(rangeUtf8.getPartIds(startKey1, false, endKey2, false),
            rangeUtf8.getPartIds(startKey5, false, endKey5, false));

        partIds = new ArrayList<Long>();
        partIds.add(1L);
        partIds.add(2L);
        Assert.assertEquals(partIds, rangeUtf8.getPartIds(startKey1, false, endKey3, false));

        partIds = new ArrayList<Long>();
        partIds.add(0L);
        Assert.assertEquals(partIds, rangeUtf8.getPartIds(startKey4, false, endKey4, false));

        Assert.assertEquals(partIds, rangeUtf8.getPartIds(startKey6, false, endKey4, true));

        Assert.assertEquals(0, rangeUtf8.getPartIds(startKey3, false, endKey1, true).size());

    }

    @Test
    public void testGetRandomId() {
        Assert.assertTrue(rangeBinary.getRandomPartId() >= 0 && rangeBinary.getRandomPartId() <= 2);
    }
}

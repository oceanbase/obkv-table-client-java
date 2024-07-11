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
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObSimpleColumn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
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
        Map<String, Object> partition_1 = new HashMap<String, Object>() {{
            put("K", "partition_1");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> partition_2 = new HashMap<String, Object>() {{
            put("K", "partition_2");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        long partId = rangeBinary.getPartId(new Row(partition_1));
        Assert.assertEquals(1, partId);

        Map<String, Object> test_a = new HashMap<String, Object>() {{
            put("K", "a");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        partId = rangeBinary.getPartId(new Row(test_a));
        Assert.assertEquals(0, partId);

        Map<String, Object> test_x = new HashMap<String, Object>() {{
            put("K", "x");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        partId = rangeBinary.getPartId(new Row(test_x));
        Assert.assertEquals(2, partId);

        Assert.assertEquals(
                rangeBinary.getPartId(new Row(partition_1)),
                rangeBinary.getPartId(new Row(partition_2)));

        Map<String, Object> test_1 = new HashMap<String, Object>() {{
            put("K", "test_1");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> test_1_bytes = new HashMap<String, Object>() {{
            put("K", "test_1".getBytes());
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> test_2 = new HashMap<String, Object>() {{
            put("K", "test_2");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> test_2_bytes = new HashMap<String, Object>() {{
            put("K", "test_2".getBytes());
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};

        Assert.assertEquals(
            rangeBinary.getPartId(new Row(test_1)),
            rangeBinary.getPartId(new Row(test_2)));

        Assert.assertEquals(
            rangeBinary.getPartId(new Row(test_1)),
            rangeBinary.getPartId(new Row(test_2_bytes)));

        Assert.assertEquals(
            rangeBinary.getPartId(new Row(test_1_bytes)),
            rangeBinary.getPartId(new Row(test_2_bytes)));

        Map<String, Object> test_A = new HashMap<String, Object>() {{
            put("K", "A");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> test_P = new HashMap<String, Object>() {{
            put("K", "P");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> test_X = new HashMap<String, Object>() {{
            put("K", "X");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> test_p = new HashMap<String, Object>() {{
            put("K", "p");
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};

        partId = rangeBinary.getPartId(new Row(test_A));
        Assert.assertEquals(0, partId);

        partId = rangeBinary.getPartId(new Row(test_P));
        Assert.assertEquals(0, partId);

        partId = rangeBinary.getPartId(new Row(test_X));
        Assert.assertEquals(0, partId);

        partId = rangeUtf8_CI.getPartId(new Row(test_a));
        Assert.assertEquals(0, partId);

        partId = rangeUtf8_CI.getPartId(new Row(partition_1));
        Assert.assertEquals(1, partId);

        partId = rangeUtf8_CI.getPartId(new Row(test_x));
        Assert.assertEquals(2, partId);

        partId = rangeUtf8_CI.getPartId(new Row(test_A));
        Assert.assertEquals(0, partId);

        partId = rangeUtf8_CI.getPartId(new Row(test_P));
        Assert.assertEquals(1, partId);

        partId = rangeUtf8_CI.getPartId(new Row(test_X));
        Assert.assertEquals(2, partId);

        partId = rangeUtf8.getPartId(new Row(test_a));
        Assert.assertEquals(0, partId);

        partId = rangeUtf8.getPartId(new Row(test_p));
        Assert.assertEquals(1, partId);

        partId = rangeUtf8.getPartId(new Row(test_x));
        Assert.assertEquals(2, partId);

        partId = rangeUtf8.getPartId(new Row(test_A));
        Assert.assertEquals(0, partId);

        partId = rangeUtf8.getPartId(new Row(test_P));
        Assert.assertEquals(0, partId);

        partId = rangeUtf8.getPartId(new Row(test_X));
        Assert.assertEquals(0, partId);

        ArrayList<Object> rowKeys = new ArrayList<Object>();
        rowKeys.add(new Row(test_P));
        partId = rangeUtf8.getPartId(rowKeys, true);
        Assert.assertEquals(0, partId);
        Assert.assertTrue(rangeUtf8.toString().contains("partExpr"));
    }

    @Test
    public void testGetPartIds() {
        long timestamp = System.currentTimeMillis();
        Map<String, Object> startKey1 = new HashMap<String, Object>() {{
            put("K", "partition_1");
            put("Q", "column_1");
            put("T", timestamp);
        }};
        Map<String, Object> endKey1 = new HashMap<String, Object>() {{
            put("K", "partition_1");
            put("Q", "column_1");
            put("T", timestamp);
        }};

        Map<String, Object> startKey2 = new HashMap<String, Object>() {{
            put("K", "partition_1".getBytes());
            put("Q", "column_1");
            put("T", timestamp);
        }};
        Map<String, Object> endKey2 = new HashMap<String, Object>() {{
            put("K", "partition_1".getBytes());
            put("Q", "column_1");
            put("T", timestamp);
        }};

        Map<String, Object> startKey3 = new HashMap<String, Object>() {{
            put("K", "yes_1".getBytes());
            put("Q", "column_1");
            put("T", timestamp);
        }};
        Map<String, Object> endKey3 = new HashMap<String, Object>() {{
            put("K", "yes_2".getBytes());
            put("Q", "column_1");
            put("T", timestamp);
        }};

        Map<String, Object> startKey4 = new HashMap<String, Object>() {{
            put("K", "PARTITION_1");
            put("Q", "column_1");
            put("T", timestamp);
        }};
        Map<String, Object> endKey4 = new HashMap<String, Object>() {{
            put("K", "PARTITION_2");
            put("Q", "column_1");
            put("T", timestamp);
        }};

        Map<String, Object> startKey5 = new HashMap<String, Object>() {{
            put("K", "PARTITION_1".getBytes());
            put("Q", "column_1");
            put("T", timestamp);
        }};
        Map<String, Object> endKey5 = new HashMap<String, Object>() {{
            put("K", "PARTITION_2".getBytes());
            put("Q", "column_1");
            put("T", timestamp);
        }};

        Map<String, Object> startKey6 = new HashMap<String, Object>() {{
            put("K", "YES_1".getBytes());
            put("Q", "column_1");
            put("T", timestamp);
        }};
        Map<String, Object> endKey6 = new HashMap<String, Object>() {{
            put("K", "YES_2".getBytes());
            put("Q", "column_1");
            put("T", timestamp);
        }};


        Assert.assertEquals(rangeBinary.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeBinary.getPartIds(new Row(startKey2), true, new Row(endKey2), true));

        Assert.assertEquals(rangeBinary.getPartIds(new Row(startKey1), true, new Row(endKey2), true),
            rangeBinary.getPartIds(new Row(startKey2), true, new Row(endKey1), true));

        Assert.assertEquals(rangeBinary.getPartIds(new Row(startKey1), false, new Row(endKey2), false),
            rangeBinary.getPartIds(new Row(startKey2), false, new Row(endKey1), false));

        Assert.assertNotEquals(rangeBinary.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeBinary.getPartIds(new Row(startKey4), true, new Row(endKey4), true));

        Assert.assertNotEquals(rangeBinary.getPartIds(new Row(startKey2), true, new Row(endKey2), true),
            rangeBinary.getPartIds(new Row(startKey5), true, new Row(endKey5), true));

        Assert.assertNotEquals(rangeBinary.getPartIds(new Row(startKey3), false, new Row(endKey3), false),
            rangeBinary.getPartIds(new Row(startKey6), false, new Row(endKey6), false));

        Assert.assertNotEquals(rangeBinary.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeBinary.getPartIds(new Row(startKey5), true, new Row(endKey5), true));

        Assert.assertNotEquals(rangeBinary.getPartIds(new Row(startKey1), true, new Row(endKey2), true),
            rangeBinary.getPartIds(new Row(startKey5), true, new Row(endKey4), true));

        Assert.assertNotEquals(rangeBinary.getPartIds(new Row(startKey1), false, new Row(endKey2), false),
            rangeBinary.getPartIds(new Row(startKey5), false, new Row(endKey5), false));

        List<Long> partIds = new ArrayList<Long>();
        partIds.add(1L);
        partIds.add(2L);
        Assert.assertEquals(partIds, rangeBinary.getPartIds(new Row(startKey1), false, new Row(endKey3), false));

        partIds = new ArrayList<Long>();
        partIds.add(0L);
        Assert.assertEquals(partIds, rangeBinary.getPartIds(new Row(startKey4), false, new Row(endKey4), false));

        Assert.assertEquals(partIds, rangeBinary.getPartIds(new Row(startKey6), false, new Row(endKey4), true));

        Assert.assertEquals(0, rangeBinary.getPartIds(new Row(startKey3), false, new Row(endKey1), true).size());

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeUtf8_CI.getPartIds(new Row(startKey2), true, new Row(endKey2), true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey1), true, new Row(endKey2), true),
            rangeUtf8_CI.getPartIds(new Row(startKey2), true, new Row(endKey1), true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey1), false, new Row(endKey2), false),
            rangeUtf8_CI.getPartIds(new Row(startKey2), false, new Row(endKey1), false));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeUtf8_CI.getPartIds(new Row(startKey4), true, new Row(endKey4), true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey2), true, new Row(endKey2), true),
            rangeUtf8_CI.getPartIds(new Row(startKey5), true, new Row(endKey5), true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey3), false, new Row(endKey3), false),
            rangeUtf8_CI.getPartIds(new Row(startKey6), false, new Row(endKey6), false));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeUtf8_CI.getPartIds(new Row(startKey5), true, new Row(endKey5), true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey1), true, new Row(endKey2), true),
            rangeUtf8_CI.getPartIds(new Row(startKey5), true, new Row(endKey4), true));

        Assert.assertEquals(rangeUtf8_CI.getPartIds(new Row(startKey1), false, new Row(endKey2), false),
            rangeUtf8_CI.getPartIds(new Row(startKey5), false, new Row(endKey4), false));

        partIds = new ArrayList<Long>();
        partIds.add(1L);
        partIds.add(2L);
        Assert.assertEquals(partIds, rangeUtf8_CI.getPartIds(new Row(startKey1), false, new Row(endKey3), false));
        Assert.assertEquals(partIds, rangeUtf8_CI.getPartIds(new Row(startKey4), false, new Row(endKey6), false));
        Assert.assertEquals(0, rangeUtf8_CI.getPartIds(new Row(startKey3), false, new Row(endKey1), true).size());

        Assert.assertEquals(rangeUtf8.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeUtf8.getPartIds(new Row(startKey2), true, new Row(endKey2), true));

        Assert.assertEquals(rangeUtf8.getPartIds(new Row(startKey1), true, new Row(endKey2), true),
            rangeUtf8.getPartIds(new Row(startKey2), true, new Row(endKey1), true));

        Assert.assertEquals(rangeUtf8.getPartIds(new Row(startKey1), false, new Row(endKey2), false),
            rangeUtf8.getPartIds(new Row(startKey2), false, new Row(endKey1), false));

        Assert.assertNotEquals(rangeUtf8.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeUtf8.getPartIds(new Row(startKey4), true, new Row(endKey4), true));

        Assert.assertNotEquals(rangeUtf8.getPartIds(new Row(startKey2), true, new Row(endKey2), true),
            rangeUtf8.getPartIds(new Row(startKey5), true, new Row(endKey5), true));

        Assert.assertNotEquals(rangeUtf8.getPartIds(new Row(startKey3), false, new Row(endKey3), false),
            rangeUtf8.getPartIds(new Row(startKey6), false, new Row(endKey6), false));

        Assert.assertNotEquals(rangeUtf8.getPartIds(new Row(startKey1), true, new Row(endKey1), true),
            rangeUtf8.getPartIds(new Row(startKey5), true, new Row(endKey5), true));

        Assert.assertNotEquals(rangeUtf8.getPartIds(new Row(startKey1), true, new Row(endKey2), true),
            rangeUtf8.getPartIds(new Row(startKey5), true, new Row(endKey4), true));

        Assert.assertNotEquals(rangeUtf8.getPartIds(new Row(startKey1), false, new Row(endKey2), false),
            rangeUtf8.getPartIds(new Row(startKey5), false, new Row(endKey5), false));

        partIds = new ArrayList<Long>();
        partIds.add(1L);
        partIds.add(2L);
        Assert.assertEquals(partIds, rangeUtf8.getPartIds(new Row(startKey1), false, new Row(endKey3), false));

        partIds = new ArrayList<Long>();
        partIds.add(0L);
        Assert.assertEquals(partIds, rangeUtf8.getPartIds(new Row(startKey4), false, new Row(endKey4), false));

        Assert.assertEquals(partIds, rangeUtf8.getPartIds(new Row(startKey6), false, new Row(endKey4), true));

        Assert.assertEquals(0, rangeUtf8.getPartIds(new Row(startKey3), false, new Row(endKey1), true).size());

    }

    @Test
    public void testGetRandomId() {
        Assert.assertTrue(rangeBinary.getRandomPartId() >= 0 && rangeBinary.getRandomPartId() <= 2);
    }
}

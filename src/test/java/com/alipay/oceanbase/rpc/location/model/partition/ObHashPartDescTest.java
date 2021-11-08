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
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.rpc.location.model.partition.ObPartFuncType.HASH_V2;
import static java.lang.Math.abs;

public class ObHashPartDescTest {
    private ObHashPartDesc obHashPartDesc;

    @Before
    public void setUp() {
        obHashPartDesc = new ObHashPartDesc();
        obHashPartDesc.setPartFuncType(HASH_V2);
        obHashPartDesc.setPartExpr("k");
        obHashPartDesc.setPartNum(16);
        obHashPartDesc.setPartSpace(0);
        Map<String, Long> partNameIdMap = LocationUtil.buildDefaultPartNameIdMap(obHashPartDesc
            .getPartNum());
        obHashPartDesc.setPartNameIdMap(partNameIdMap);
        ObColumn column = new ObSimpleColumn("k",//
            0,//
            ObObjType.valueOf(4),//
            ObCollationType.valueOf(63));

        List<ObColumn> partColumns = new ArrayList<ObColumn>();
        partColumns.add(column);
        obHashPartDesc.setPartColumns(partColumns);
        obHashPartDesc.setRowKeyElement(TableEntry.HBASE_ROW_KEY_ELEMENT);
        obHashPartDesc.prepare();
    }

    @Test
    public void testGetPartId() {

        Assert.assertEquals(0,
            (long) obHashPartDesc.getPartId(0, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(1,
            (long) obHashPartDesc.getPartId(1, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(2,
            (long) obHashPartDesc.getPartId(2, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(3,
            (long) obHashPartDesc.getPartId(3, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(4,
            (long) obHashPartDesc.getPartId(4, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(5,
            (long) obHashPartDesc.getPartId(5, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(6,
            (long) obHashPartDesc.getPartId(6, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(7,
            (long) obHashPartDesc.getPartId(7, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(8,
            (long) obHashPartDesc.getPartId(8, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(9,
            (long) obHashPartDesc.getPartId(9, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(10,
            (long) obHashPartDesc.getPartId(10, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(11,
            (long) obHashPartDesc.getPartId(11, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(12,
            (long) obHashPartDesc.getPartId(12, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(13,
            (long) obHashPartDesc.getPartId(13, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(14,
            (long) obHashPartDesc.getPartId(14, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(15,
            (long) obHashPartDesc.getPartId(15, "column_1", System.currentTimeMillis()));

        Assert.assertEquals(0,
            (long) obHashPartDesc.getPartId(-0, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(1,
            (long) obHashPartDesc.getPartId(-1, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(2,
            (long) obHashPartDesc.getPartId(-2, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(3,
            (long) obHashPartDesc.getPartId(-3, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(4,
            (long) obHashPartDesc.getPartId(-4, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(5,
            (long) obHashPartDesc.getPartId(-5, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(6,
            (long) obHashPartDesc.getPartId(-6, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(7,
            (long) obHashPartDesc.getPartId(-7, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(8,
            (long) obHashPartDesc.getPartId(-8, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(9,
            (long) obHashPartDesc.getPartId(-9, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(10,
            (long) obHashPartDesc.getPartId(-10, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(11,
            (long) obHashPartDesc.getPartId(-11, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(12,
            (long) obHashPartDesc.getPartId(-12, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(13,
            (long) obHashPartDesc.getPartId(-13, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(14,
            (long) obHashPartDesc.getPartId(-14, "column_1", System.currentTimeMillis()));
        Assert.assertEquals(15,
            (long) obHashPartDesc.getPartId(-15, "column_1", System.currentTimeMillis()));

        Assert.assertEquals(obHashPartDesc.getPartId(1, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("1", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(2, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("2", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(3, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("3", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(4, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("4", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(5, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("5", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(6, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("6", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(7, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("7", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(8, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("8", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(9, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("9", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(10, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("10", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(11, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("11", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(12, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("12", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(13, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("13", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(14, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("14", "column_1", System.currentTimeMillis()));
        Assert.assertEquals(obHashPartDesc.getPartId(15, "column_1", System.currentTimeMillis()),
            obHashPartDesc.getPartId("15", "column_1", System.currentTimeMillis()));

    }

    @Test
    public void testGetPartIds() {
        Object[] rowKey_0 = new Object[] { 0, "column_1", System.currentTimeMillis() };

        Object[] rowKey_8 = new Object[] { 8, "column_1", System.currentTimeMillis() };

        Object[] rowKey_15 = new Object[] { 15, "column_1", System.currentTimeMillis() };

        Object[] rowKey_16 = new Object[] { 16, "column_1", System.currentTimeMillis() };

        Object[] rowKey_30 = new Object[] { 30, "column_1", System.currentTimeMillis() };

        Object[] rowKey_8f = new Object[] { -8, "column_1", System.currentTimeMillis() };

        Object[] rowKey_15f = new Object[] { -15, "column_1", System.currentTimeMillis() };

        Object[] rowKey_30f = new Object[] { -30, "column_1", System.currentTimeMillis() };

        Assert.assertEquals(buildPartIds(0, 0),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_0, true));
        Assert.assertEquals(buildEmptyPartIds(),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_0, false));
        Assert.assertEquals(buildEmptyPartIds(),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_0, true));
        Assert.assertEquals(buildEmptyPartIds(),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_0, false));

        Assert.assertEquals(buildEmptyPartIds(),
            obHashPartDesc.getPartIds(rowKey_8, true, rowKey_0, true));
        Assert.assertEquals(buildEmptyPartIds(),
            obHashPartDesc.getPartIds(rowKey_8, false, rowKey_0, true));
        Assert.assertEquals(buildEmptyPartIds(),
            obHashPartDesc.getPartIds(rowKey_8, true, rowKey_0, false));
        Assert.assertEquals(buildEmptyPartIds(),
            obHashPartDesc.getPartIds(rowKey_8, false, rowKey_0, false));

        Assert.assertEquals(buildPartIds(0, 8),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_8, true));
        Assert.assertEquals(buildPartIds(1, 8),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_8, true));
        Assert.assertEquals(buildPartIds(0, 7),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_8, false));
        Assert.assertEquals(buildPartIds(1, 7),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_8, false));

        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_15, true));
        Assert.assertEquals(buildPartIds(0, 14),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_15, false));
        Assert.assertEquals(buildPartIds(1, 15),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_15, true));
        Assert.assertEquals(buildPartIds(1, 14),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_15, false));

        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_16, true));
        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_16, false));
        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_16, true));
        Assert.assertEquals(buildPartIds(1, 15),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_16, false));

        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_30, true));
        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_0, true, rowKey_30, false));
        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_30, true));
        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_0, false, rowKey_30, false));

        Assert.assertEquals(buildPartIds(-8, 0),
            obHashPartDesc.getPartIds(rowKey_8f, true, rowKey_0, true));
        Assert.assertEquals(buildPartIds(-8, -1),
            obHashPartDesc.getPartIds(rowKey_8f, true, rowKey_0, false));
        Assert.assertEquals(buildPartIds(-7, 0),
            obHashPartDesc.getPartIds(rowKey_8f, false, rowKey_0, true));
        Assert.assertEquals(buildPartIds(-7, -1),
            obHashPartDesc.getPartIds(rowKey_8f, false, rowKey_0, false));

        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_15f, true, rowKey_0, true));
        Assert.assertEquals(buildPartIds(-15, -1),
            obHashPartDesc.getPartIds(rowKey_15f, true, rowKey_0, false));
        Assert.assertEquals(buildPartIds(-14, 0),
            obHashPartDesc.getPartIds(rowKey_15f, false, rowKey_0, true));
        Assert.assertEquals(buildPartIds(-14, -1),
            obHashPartDesc.getPartIds(rowKey_15f, false, rowKey_0, false));

        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_30f, true, rowKey_0, true));
        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_30f, true, rowKey_0, false));
        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_30f, false, rowKey_0, true));
        Assert.assertEquals(buildPartIds(0, 15),
            obHashPartDesc.getPartIds(rowKey_30f, false, rowKey_0, false));
    }

    private List<Long> buildPartIds(long start, long end) {
        List<Long> ids = new ArrayList<Long>();
        for (long i = start; i <= end; i++) {
            ids.add(abs(i));
        }
        return ids;
    }

    private List<Long> buildEmptyPartIds() {
        return new ArrayList<Long>();
    }

    @Test
    public void testGetRandomId() {
        Assert.assertTrue(obHashPartDesc.getRandomPartId() >= 0
                          && obHashPartDesc.getRandomPartId() <= obHashPartDesc.getPartNum());
    }
}

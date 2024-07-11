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
import com.alipay.oceanbase.rpc.mutation.*;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
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
        // set values
        Map<String, Object> values0 = new HashMap<String, Object>() {{
            put("K", 0);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values1 = new HashMap<String, Object>() {{
            put("K", 1);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values2 = new HashMap<String, Object>() {{
            put("K", 2);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values3 = new HashMap<String, Object>() {{
            put("K", 3);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values4 = new HashMap<String, Object>() {{
            put("K", 4);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values5 = new HashMap<String, Object>() {{
            put("K", 5);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values6 = new HashMap<String, Object>() {{
            put("K", 6);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values7 = new HashMap<String, Object>() {{
            put("K", 7);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};Map<String, Object> values8 = new HashMap<String, Object>() {{
            put("K", 8);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};Map<String, Object> values9 = new HashMap<String, Object>() {{
            put("K", 9);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values10 = new HashMap<String, Object>() {{
            put("K", 10);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values11 = new HashMap<String, Object>() {{
            put("K", 11);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values12 = new HashMap<String, Object>() {{
            put("K", 12);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values13 = new HashMap<String, Object>() {{
            put("K", 13);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values14 = new HashMap<String, Object>() {{
            put("K", 14);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values15 = new HashMap<String, Object>() {{
            put("K", 15);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};

        Map<String, Object> values_0 = new HashMap<String, Object>() {{
            put("K", -0);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_1 = new HashMap<String, Object>() {{
            put("K", -1);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_2 = new HashMap<String, Object>() {{
            put("K", -2);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_3 = new HashMap<String, Object>() {{
            put("K", -3);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_4 = new HashMap<String, Object>() {{
            put("K", -4);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_5 = new HashMap<String, Object>() {{
            put("K", -5);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_6 = new HashMap<String, Object>() {{
            put("K", -6);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_7 = new HashMap<String, Object>() {{
            put("K", -7);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_8 = new HashMap<String, Object>() {{
            put("K", -8);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_9 = new HashMap<String, Object>() {{
            put("K", -9);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_10 = new HashMap<String, Object>() {{
            put("K", -10);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_11 = new HashMap<String, Object>() {{
            put("K", -11);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_12 = new HashMap<String, Object>() {{
            put("K", -12);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_13 = new HashMap<String, Object>() {{
            put("K", -13);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_14 = new HashMap<String, Object>() {{
            put("K", -14);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_15 = new HashMap<String, Object>() {{
            put("K", -15);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};

        Map<String, Object> values0_e = new HashMap<String, Object>() {{
            put("K", 0);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values0_l = new HashMap<String, Object>() {{
            put("K", 0);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values1_e = new HashMap<String, Object>() {{
            put("K", 1);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values1_l = new HashMap<String, Object>() {{
            put("K", 1);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values2_e = new HashMap<String, Object>() {{
            put("K", 2);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values2_l = new HashMap<String, Object>() {{
            put("K", 2);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values3_e = new HashMap<String, Object>() {{
            put("K", 3);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values3_l = new HashMap<String, Object>() {{
            put("K", 3);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values4_e = new HashMap<String, Object>() {{
            put("K", 4);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values4_l = new HashMap<String, Object>() {{
            put("K", 4);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values5_e = new HashMap<String, Object>() {{
            put("K", 5);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values5_l = new HashMap<String, Object>() {{
            put("K", 5);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values6_e = new HashMap<String, Object>() {{
            put("K", 6);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values6_l = new HashMap<String, Object>() {{
            put("K", 6);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values7_e = new HashMap<String, Object>() {{
            put("K", 7);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values7_l = new HashMap<String, Object>() {{
                put("K", 7);
                put("Q", "column_1");
                put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values8_e = new HashMap<String, Object>() {{
            put("K", 8);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values8_l = new HashMap<String, Object>() {{
            put("K", 8);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values9_e = new HashMap<String, Object>() {{
            put("K", 9);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values9_l = new HashMap<String, Object>() {{
            put("K", 9);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values10_e = new HashMap<String, Object>() {{
            put("K", 10);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values10_l = new HashMap<String, Object>() {{
            put("K", 10);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};

        // test getPartId interface
        Assert.assertEquals(0,
                (long) obHashPartDesc.getPartId(new Row(values0)));
        Assert.assertEquals(1,
                (long) obHashPartDesc.getPartId(new Row(values1)));
        Assert.assertEquals(2,
                (long) obHashPartDesc.getPartId(new Row(values2)));
        Assert.assertEquals(3,
                (long) obHashPartDesc.getPartId(new Row(values3)));
        Assert.assertEquals(4,
                (long) obHashPartDesc.getPartId(new Row(values4)));
        Assert.assertEquals(5,
                (long) obHashPartDesc.getPartId(new Row(values5)));
        Assert.assertEquals(6,
                (long) obHashPartDesc.getPartId(new Row(values6)));
        Assert.assertEquals(7,
                (long) obHashPartDesc.getPartId(new Row(values7)));
        Assert.assertEquals(8,
                (long) obHashPartDesc.getPartId(new Row(values8)));
        Assert.assertEquals(9,
                (long) obHashPartDesc.getPartId(new Row(values9)));
        Assert.assertEquals(10,
                (long) obHashPartDesc.getPartId(new Row(values10)));
        Assert.assertEquals(11,
                (long) obHashPartDesc.getPartId(new Row(values11)));
        Assert.assertEquals(12,
                (long) obHashPartDesc.getPartId(new Row(values12)));
        Assert.assertEquals(13,
                (long) obHashPartDesc.getPartId(new Row(values13)));
        Assert.assertEquals(14,
                (long) obHashPartDesc.getPartId(new Row(values14)));
        Assert.assertEquals(15,
                (long) obHashPartDesc.getPartId(new Row(values15)));

        Assert.assertEquals(0,
                (long) obHashPartDesc.getPartId(new Row(values_0)));
        Assert.assertEquals(1,
                (long) obHashPartDesc.getPartId(new Row(values_1)));
        Assert.assertEquals(2,
                (long) obHashPartDesc.getPartId(new Row(values_2)));
        Assert.assertEquals(3,
                (long) obHashPartDesc.getPartId(new Row(values_3)));
        Assert.assertEquals(4,
                (long) obHashPartDesc.getPartId(new Row(values_4)));
        Assert.assertEquals(5,
                (long) obHashPartDesc.getPartId(new Row(values_5)));
        Assert.assertEquals(6,
                (long) obHashPartDesc.getPartId(new Row(values_6)));
        Assert.assertEquals(7,
                (long) obHashPartDesc.getPartId(new Row(values_7)));
        Assert.assertEquals(8,
                (long) obHashPartDesc.getPartId(new Row(values_8)));
        Assert.assertEquals(9,
                (long) obHashPartDesc.getPartId(new Row(values_9)));
        Assert.assertEquals(10,
                (long) obHashPartDesc.getPartId(new Row(values_10)));
        Assert.assertEquals(11,
                (long) obHashPartDesc.getPartId(new Row(values_11)));
        Assert.assertEquals(12,
                (long) obHashPartDesc.getPartId(new Row(values_12)));
        Assert.assertEquals(13,
                (long) obHashPartDesc.getPartId(new Row(values_13)));
        Assert.assertEquals(14,
                (long) obHashPartDesc.getPartId(new Row(values_14)));
        Assert.assertEquals(15,
                (long) obHashPartDesc.getPartId(new Row(values_15)));

        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values0_e)),
                obHashPartDesc.getPartId(new Row(values0_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values1_e)),
                obHashPartDesc.getPartId(new Row(values1_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values2_e)),
                obHashPartDesc.getPartId(new Row(values2_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values3_e)),
                obHashPartDesc.getPartId(new Row(values3_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values4_e)),
                obHashPartDesc.getPartId(new Row(values4_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values5_e)),
                obHashPartDesc.getPartId(new Row(values5_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values6_e)),
                obHashPartDesc.getPartId(new Row(values6_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values7_e)),
                obHashPartDesc.getPartId(new Row(values7_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values8_e)),
                obHashPartDesc.getPartId(new Row(values8_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values9_e)),
                obHashPartDesc.getPartId(new Row(values9_l)));
        Assert.assertEquals(obHashPartDesc.getPartId(new Row(values10_e)),
                obHashPartDesc.getPartId(new Row(values10_l)));
    }

    @Test
    public void testGetPartIds() {
        Map<String, Object> values0 = new HashMap<String, Object>() {{
            put("K", 0);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values8 = new HashMap<String, Object>() {{
            put("K", 8);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values15 = new HashMap<String, Object>() {{
            put("K", 15);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values16 = new HashMap<String, Object>() {{
            put("K", 16);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values30 = new HashMap<String, Object>() {{
            put("K", 30);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_8f = new HashMap<String, Object>() {{
            put("K", -8);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_15f = new HashMap<String, Object>() {{
            put("K", -15);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};
        Map<String, Object> values_30f = new HashMap<String, Object>() {{
            put("K", -30);
            put("Q", "column_1");
            put("T", System.currentTimeMillis());
        }};

        Assert.assertEquals(buildPartIds(0, 0),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values0), true));
        Assert.assertEquals(buildEmptyPartIds(),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values0), false));
        Assert.assertEquals(buildEmptyPartIds(),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values0), true));
        Assert.assertEquals(buildEmptyPartIds(),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values0), false));

        Assert.assertEquals(buildEmptyPartIds(),
                obHashPartDesc.getPartIds(new Row(values8), true, new Row(values0), true));
        Assert.assertEquals(buildEmptyPartIds(),
                obHashPartDesc.getPartIds(new Row(values8), false, new Row(values0), true));
        Assert.assertEquals(buildEmptyPartIds(),
                obHashPartDesc.getPartIds(new Row(values8), true, new Row(values0), false));
        Assert.assertEquals(buildEmptyPartIds(),
                obHashPartDesc.getPartIds(new Row(values8), false, new Row(values0), false));

        Assert.assertEquals(buildPartIds(0, 8),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values8), true));
        Assert.assertEquals(buildPartIds(1, 8),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values8), true));
        Assert.assertEquals(buildPartIds(0, 7),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values8), false));
        Assert.assertEquals(buildPartIds(1, 7),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values8), false));

        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values15), true));
        Assert.assertEquals(buildPartIds(0, 14),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values15), false));
        Assert.assertEquals(buildPartIds(1, 15),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values15), true));
        Assert.assertEquals(buildPartIds(1, 14),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values15), false));

        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values16), true));
        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values16), false));
        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values16), true));
        Assert.assertEquals(buildPartIds(1, 15),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values16), false));

        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values30), true));
        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values0), true, new Row(values30), false));
        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values30), true));
        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values0), false, new Row(values30), false));

        Assert.assertEquals(buildPartIds(-8, 0),
                obHashPartDesc.getPartIds(new Row(values_8f), true, new Row(values0), true));
        Assert.assertEquals(buildPartIds(-8, -1),
                obHashPartDesc.getPartIds(new Row(values_8f), true, new Row(values0), false));
        Assert.assertEquals(buildPartIds(-7, 0),
                obHashPartDesc.getPartIds(new Row(values_8f), false, new Row(values0), true));
        Assert.assertEquals(buildPartIds(-7, -1),
                obHashPartDesc.getPartIds(new Row(values_8f), false, new Row(values0), false));

        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values_15f), true, new Row(values0), true));
        Assert.assertEquals(buildPartIds(-15, -1),
                obHashPartDesc.getPartIds(new Row(values_15f), true, new Row(values0), false));
        Assert.assertEquals(buildPartIds(-14, 0),
                obHashPartDesc.getPartIds(new Row(values_15f), false, new Row(values0), true));
        Assert.assertEquals(buildPartIds(-14, -1),
                obHashPartDesc.getPartIds(new Row(values_15f), false, new Row(values0), false));

        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values_30f), true, new Row(values0), true));
        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values_30f), true, new Row(values0), false));
        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values_30f), false, new Row(values0), true));
        Assert.assertEquals(buildPartIds(0, 15),
                obHashPartDesc.getPartIds(new Row(values_30f), false, new Row(values0), false));
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

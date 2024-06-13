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

package com.alipay.oceanbase.rpc.protocol.payload.impl.column;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType.ObVarcharType;

public class ObSimpleColumnTest {
    @Test
    public void testEntity() {
        ObSimpleColumn column = new ObSimpleColumn("A", 0, ObVarcharType,
            CS_TYPE_UTF8MB4_GENERAL_CI);
        Assert.assertEquals("A", column.getColumnName());
        List<String> refColumnNames = new ArrayList<String>();
        refColumnNames.add("A");
        //        Assert.assertEquals(refColumnNames, column.getRefColumnNames());
        Assert.assertEquals(0, column.getIndex());
        Assert.assertEquals(ObVarcharType, column.getObObjType());
        Assert.assertEquals(CS_TYPE_UTF8MB4_GENERAL_CI, column.getObCollationType());
    }
}

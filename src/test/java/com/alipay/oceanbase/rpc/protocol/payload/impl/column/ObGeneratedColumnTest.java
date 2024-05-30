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
import static org.junit.Assert.fail;

public class ObGeneratedColumnTest {
    @Test
    public void testEntity() {
//        ObGeneratedColumnSubStrFunc subStr = new ObGeneratedColumnSubStrFunc();
//        subStr.setParameters(new ArrayList<Object>() {
//            {
//                add("K");
//                add(1);
//                add(4);
//            }
//        });
//        ObGeneratedColumn column = new ObGeneratedColumn("k_prefix", 0, ObVarcharType,
//            CS_TYPE_UTF8MB4_GENERAL_CI, subStr);
//        Assert.assertEquals("k_prefix", column.getColumnName());
//        List<String> refColumnNames = new ArrayList<String>();
//        refColumnNames.add("K");
//        //        Assert.assertEquals(refColumnNames, column.getRefColumnNames());
//        Assert.assertEquals(0, column.getIndex());
//        Assert.assertEquals(ObVarcharType, column.getObObjType());
//        Assert.assertEquals(CS_TYPE_UTF8MB4_GENERAL_CI, column.getObCollationType());
//
//        try {
//            column.evalValue();
//            fail();
//        } catch (IllegalArgumentException e) {
//
//        }

    }
}

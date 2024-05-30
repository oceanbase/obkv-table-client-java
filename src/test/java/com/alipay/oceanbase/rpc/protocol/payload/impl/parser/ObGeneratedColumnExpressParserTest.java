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

package com.alipay.oceanbase.rpc.protocol.payload.impl.parser;

import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnSimpleFunc;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnSubStrFunc;
import org.junit.Assert;
import org.junit.Test;

public class ObGeneratedColumnExpressParserTest {
    @Test
    public void testSubStr_1() {
//        ObGeneratedColumnExpressParser parser = new ObGeneratedColumnExpressParser(
//            "substr(K,   1,4)");
//        ObGeneratedColumnSimpleFunc func = parser.parse();
//        Assert.assertTrue(func instanceof ObGeneratedColumnSubStrFunc);
//        ObGeneratedColumnSubStrFunc subStr = (ObGeneratedColumnSubStrFunc) func;
//
//        //        Assert.assertEquals("K", subStr.getRefColumnNames().get(0));
//        Assert.assertEquals(1, subStr.getPos());
//        Assert.assertEquals(4, subStr.getLen());
//
//        parser = new ObGeneratedColumnExpressParser("substr(K,   -1,2)");
//        func = parser.parse();
//        Assert.assertTrue(func instanceof ObGeneratedColumnSubStrFunc);
//        subStr = (ObGeneratedColumnSubStrFunc) func;
//
//        //        Assert.assertEquals("K", subStr.getRefColumnNames().get(0));
//        Assert.assertEquals(-1, subStr.getPos());
//        Assert.assertEquals(2, subStr.getLen());
//
//        parser = new ObGeneratedColumnExpressParser("substr(A,   -3)");
//        func = parser.parse();
//        Assert.assertTrue(func instanceof ObGeneratedColumnSubStrFunc);
//        subStr = (ObGeneratedColumnSubStrFunc) func;
//
//        //        Assert.assertEquals("A", subStr.getRefColumnNames().get(0));
//        Assert.assertEquals(-3, subStr.getPos());
//        Assert.assertEquals(Integer.MIN_VALUE, subStr.getLen());
//
//        parser = new ObGeneratedColumnExpressParser("substr(A,   -3,2)");
//        func = parser.parse();
//        Assert.assertTrue(func instanceof ObGeneratedColumnSubStrFunc);
//        subStr = (ObGeneratedColumnSubStrFunc) func;
//
//        //        Assert.assertEquals("A", subStr.getRefColumnNames().get(0));
//        Assert.assertEquals(-3, subStr.getPos());
//        Assert.assertEquals(2, subStr.getLen());

    }
}

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

import static com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI;
import static org.junit.Assert.fail;

public class ObGeneratedColumnSubStrFuncFuncTest {
    @Test
    public void testEval() {

        ObGeneratedColumnSubStrFunc subStr = new ObGeneratedColumnSubStrFunc();
        subStr.setParameters(new ArrayList<Object>() {
            {
                add("K");
                add(1);
                add(4);
            }
        });

        Assert.assertEquals("1", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1"));
        Assert.assertEquals("11", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "11"));
        Assert.assertEquals("111", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "111"));
        Assert.assertEquals("1111", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1111"));
        Assert.assertEquals("1111", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "11111"));

        subStr = new ObGeneratedColumnSubStrFunc();
        subStr.setParameters(new ArrayList<Object>() {
            {
                add("K");
                add(-1);
                add(2);
            }
        });

        Assert.assertEquals("K", subStr.getRefColumnNames().get(0));
        Assert.assertEquals(-1, subStr.getPos());
        Assert.assertEquals(2, subStr.getLen());

        Assert.assertEquals("1", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1"));
        Assert.assertEquals("2", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "12"));
        Assert.assertEquals("4", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1234"));
        Assert.assertEquals("5", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "12345"));
        Assert.assertEquals("6", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "123456"));

        subStr = new ObGeneratedColumnSubStrFunc();
        subStr.setParameters(new ArrayList<Object>() {
            {
                add("A");
                add(-3);
            }
        });

        Assert.assertEquals("A", subStr.getRefColumnNames().get(0));
        Assert.assertEquals(-3, subStr.getPos());
        Assert.assertEquals(Integer.MIN_VALUE, subStr.getLen());

        try {
            subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1");
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1");
            fail();
        } catch (IllegalArgumentException e) {

        }
        Assert.assertEquals("234", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1234"));
        Assert.assertEquals("345", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "12345"));
        Assert.assertEquals("456", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "123456"));

        subStr = new ObGeneratedColumnSubStrFunc();
        subStr.setParameters(new ArrayList<Object>() {
            {
                add("A");
                add(-3);
                add(2);
            }
        });
        Assert.assertEquals("A", subStr.getRefColumnNames().get(0));
        Assert.assertEquals(-3, subStr.getPos());
        Assert.assertEquals(2, subStr.getLen());

        try {
            subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1");
            fail();
        } catch (IllegalArgumentException e) {

        }

        try {
            subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1");
            fail();
        } catch (IllegalArgumentException e) {

        }
        Assert.assertEquals("23", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "1234"));
        Assert.assertEquals("34", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "12345"));
        Assert.assertEquals("45", subStr.evalValue(CS_TYPE_UTF8MB4_GENERAL_CI, "123456"));
    }
}

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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class ObGeneratedColumnNegateFuncTest {
    @Test
    public void testEval() {
        ObGeneratedColumnSimpleFunc func = new ObGeneratedColumnNegateFunc("K");
        Assert.assertEquals(-1L, func.evalValue(ObCollationType.CS_TYPE_INVALID, 1L));
        Assert.assertEquals(-11L, func.evalValue(ObCollationType.CS_TYPE_INVALID, 11L));
        Assert.assertEquals(-111,
                func.evalValue(ObCollationType.CS_TYPE_INVALID, 111));
        Assert.assertEquals(1111L,
                func.evalValue(ObCollationType.CS_TYPE_INVALID, -1111L));
        Assert.assertEquals(11111L,
                func.evalValue(ObCollationType.CS_TYPE_INVALID, -11111L));
        try {
            func.evalValue(ObCollationType.CS_TYPE_INVALID, Integer.MIN_VALUE);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("The currently provided parameter is the " +
                    "minimum value of the Integer type, and its negation will cause an overflow."));
        }
        List<String> refColumns = new ArrayList<String>();
        refColumns.add("K");
        Assert.assertEquals(refColumns, func.getRefColumnNames());
    }
}

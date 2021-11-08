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

package com.alipay.oceanbase.rpc.util;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class ObVStringTest {
    @Test
    public void testEntity() {
        ObVString obVString = new ObVString("test");

        Assert.assertEquals("test", obVString.getStringVal());
        Assert.assertEquals(6, obVString.getEncodeNeedBytes());

        Assert.assertEquals(6, Serialization.getNeedBytes("test"));

        Assert.assertTrue(Arrays.equals(ObObjType.ObVarcharType.encode(obVString),
            obVString.getEncodeBytes()));

        Assert.assertEquals(6, ObObjType.ObVarcharType.getEncodedSize(obVString));

        ObObj obObj = new ObObj();
        obObj.setValue("test");

        Assert.assertTrue(obObj.getValue() instanceof ObVString);

    }
}

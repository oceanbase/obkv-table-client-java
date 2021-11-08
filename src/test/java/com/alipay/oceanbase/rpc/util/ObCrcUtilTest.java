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

import com.alipay.remoting.util.CrcUtil;
import org.junit.Assert;
import org.junit.Test;

public class ObCrcUtilTest {
    @Test
    public void testObCrc32() {
        String v1 = "abc";
        Assert.assertEquals(1445960909, ObPureCrc32C.calculate(v1.getBytes()));
    }

    @Test
    public void testCrc64() {
        CRC64 crc64 = new CRC64();
        crc64.update("hello world".getBytes());
        Assert.assertEquals(1319870418925634090L, crc64.getValue());
        crc64.update(23);
        Assert.assertEquals(6547284443804659788L, crc64.getValue());
        crc64.reset();
        crc64.update("hello world".getBytes(), 2, 6);
        Assert.assertEquals(-2846105552371110207L, crc64.getValue());
        crc64.update((byte) 23);
        Assert.assertEquals(-5755183111318406602L, crc64.getValue());
    }

    @Test
    public void testCrc32() {
        Assert.assertEquals(222957957, CrcUtil.crc32("hello world".getBytes()));
    }
}

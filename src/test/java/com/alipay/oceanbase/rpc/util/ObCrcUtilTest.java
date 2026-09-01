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

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class ObCrcUtilTest {
    @Test
    public void testObCrc32() {
        String v1 = "abc";
        Assert.assertEquals(1445960909, ObPureCrc32C.calculate(v1.getBytes()));
    }

    @Test
    public void testOceanBaseCrc32cKnownVectors() {
        Assert.assertEquals(0L, ObPureCrc32C.calculate(new byte[0]));
        Assert.assertEquals(0x6345d352L,
            ObPureCrc32C.calculate("hello world".getBytes(StandardCharsets.UTF_8)));
        Assert.assertEquals(0x58e3fa20L,
            ObPureCrc32C.calculate("123456789".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    public void testSlicingBy8MatchesScalar() {
        Random random = new Random(20260807L);
        int[] lengths = new int[] { 0, 1, 7, 8, 15, 16, 17, 31, 32, 63, 64, 65, 127, 128, 129, 255,
                256, 257, 1023, 1024, 1025, 65535, 1048576 };
        for (int length : lengths) {
            byte[] bytes = new byte[length + 11];
            random.nextBytes(bytes);
            assertSlicingBy8EqualsScalar(bytes, 5, length);
        }

        for (int round = 0; round < 1000; round++) {
            int length = random.nextInt(32768);
            int prefix = random.nextInt(16);
            byte[] bytes = new byte[prefix + length + random.nextInt(16)];
            random.nextBytes(bytes);
            assertSlicingBy8EqualsScalar(bytes, prefix, length);
        }
    }

    private static void assertSlicingBy8EqualsScalar(byte[] bytes, int offset, int length) {
        long scalar = ObPureCrc32C.calculateScalar(bytes, offset, length);
        Assert.assertEquals(scalar, ObPureCrc32C.calculateSlicingBy8(bytes, offset, length));
        Assert.assertEquals(scalar, ObPureCrc32C.calculate(bytes, offset, length));
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

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

import com.alipay.oceanbase.rpc.location.model.partition.ObPartFuncType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.util.hash.MurmurHash;
import com.alipay.oceanbase.rpc.util.hash.ObHashSortUtf8mb4;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Timestamp;

public class ObHashUtilTest {
    @Test
    public void testHash() {
        try {
            String s1 = new String("蚂蚁金服".getBytes("UTF-8"), "UTF-8");
            Assert.assertEquals(5857140318165389032L, ObHashUtils.varcharHash(s1,
                ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI, 0, ObPartFuncType.KEY_V3));
        } catch (UnsupportedEncodingException e) {
            Assert.fail("Don't support utf8 encoding.");
        }
        Assert.assertEquals(-6632533813747801170L, ObHashUtils.varcharHash("partitionKey",
            ObCollationType.CS_TYPE_BINARY, 0, ObPartFuncType.KEY_V3));

        Assert.assertEquals(3972054043075874492L, ObHashUtils.longHash(145, 0));

        Assert.assertEquals(5483367784116059036L, ObHashUtils.varcharHash("abcdef",
            ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI, 0, ObPartFuncType.KEY_V3));

        Assert.assertEquals(141008085297877540L, ObHashUtils.varcharHash(
            "Test Partition Key 0 c1 column c1 c1 c1", ObCollationType.CS_TYPE_UTF8MB4_BIN, 0,
            ObPartFuncType.KEY_V3));

        Assert.assertEquals(-8699342424260018756L, ObHashUtils.varcharHash(new byte[] { 0x01, 0x01,
                0x02, 0x7a, 0x71, 0x2b, 0x5f, 0x30 }, ObCollationType.CS_TYPE_UTF8MB4_BIN, 47,
            ObPartFuncType.KEY_V3));

        Assert.assertEquals(2452379837092619935L, ObHashUtils.longHash(754623846937L, 53));

        Assert.assertEquals(-7033280955909782425L,
            ObHashUtils.dateHash(Date.valueOf("2019-11-12"), 47));

        Assert.assertEquals(3717832596662983799L,
            ObHashUtils.timeStampHash(Timestamp.valueOf("2019-11-12 15:34:28.986"), 47));
    }

    @Test
    public void testMurmurHash() {
        String hello = "HelloWorld";
        Assert.assertEquals(1204826224913109481L, MurmurHash.hash64(hello));
        Assert.assertEquals(-5023989056864726753L, MurmurHash.hash64(hello.substring(1)));
        Assert.assertEquals(-3875639216988124983L, MurmurHash.hash64(hello.substring(2)));
        Assert.assertEquals(2538767550008373905L, MurmurHash.hash64(hello.substring(3)));
        Assert.assertEquals(4901870543103673187L, MurmurHash.hash64(hello.substring(4)));
        Assert.assertEquals(-4158945282897732778L, MurmurHash.hash64(hello.substring(5)));
        Assert.assertEquals(-5214760330201079850L, MurmurHash.hash64(hello.substring(6)));
        Assert.assertEquals(-4180608572660262002L, MurmurHash.hash64(hello.substring(7)));
        Assert.assertEquals(7444398649062471539L, MurmurHash.hash64(hello.substring(8)));
        Assert.assertEquals(1204826224913109481L,
            MurmurHash.hash64(hello.getBytes(), hello.getBytes().length));
        Assert.assertEquals(-6333124373453295532L, MurmurHash.hash64(hello, 1, 8));

        Assert.assertEquals(654891875, MurmurHash.hash32(hello));
        Assert.assertEquals(-1697315902, MurmurHash.hash32(hello.substring(1)));
        Assert.assertEquals(-1465155702, MurmurHash.hash32(hello.substring(2)));
        Assert.assertEquals(1141768675, MurmurHash.hash32(hello.substring(3)));
        Assert.assertEquals(-611248479, MurmurHash.hash32(hello.substring(4)));
        Assert
            .assertEquals(654891875, MurmurHash.hash32(hello.getBytes(), hello.getBytes().length));
        Assert.assertEquals(173123068, MurmurHash.hash32(hello, 1, 8));
    }

    @Test
    public void testObHashSortUtf8mb4() {
        long seed = 0xc6a4a7935bd1e995L;
        long hashCode = 57;
        long res = ObHashSortUtf8mb4.obHashSortUtf8Mb4(new byte[] { (byte) 0xd3, (byte) 0x84 }, 2,
            seed, hashCode, false);
        Assert.assertEquals(7005345108782627091L, res);
        res = ObHashSortUtf8mb4.obHashSortUtf8Mb4(new byte[] { (byte) 0xe6, (byte) 0x84,
                (byte) 0x57 }, 3, seed, hashCode, false);
        Assert.assertEquals(-4132994306676758123L, res);
        res = ObHashSortUtf8mb4.obHashSortUtf8Mb4(new byte[] { (byte) 0xf6, (byte) 0x84,
                (byte) 0x57, (byte) 0x6a, (byte) 0x43 }, 4, seed, hashCode, false);
        Assert.assertEquals(-4132994306676758123L, res);
    }

}

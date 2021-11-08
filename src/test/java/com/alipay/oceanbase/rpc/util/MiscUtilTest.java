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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Timestamp;

public class MiscUtilTest {
    @Test
    public void testRandomUtil() {
        try {
            RandomUtil.getRandomNum(8, 4);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        try {
            RandomUtil.getRandomNum(0, 0);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        Assert.assertTrue(RandomUtil.getRandomNum(4, 8) >= 4);
        Assert.assertTrue(RandomUtil.getRandomNum(4, 8) < 8);
    }

    @Test
    public void testTimeUtils() {
        TimeUtils.getCurrentTimeMs();
        TimeUtils.getCurrentTimeUs();
        TimeUtils.getNanoTimeNs();
        TimeUtils.getNanoTimeUs();
        TimeUtils.formatTimeMsToDate(TimeUtils.getCurrentTimeMs());
        TimeUtils.formatTimeUsToDate(TimeUtils.getCurrentTimeUs());
        TimeUtils.sleep(1);
        Timestamp ts = TimeUtils.strToTimestamp("20200314");
        Assert.assertNotNull(ts);
        ts = TimeUtils.strToTimestamp("20200314122345");
        Assert.assertNotNull(ts);
        String s = TimeUtils.getTime("yyyy-MM-dd HH:mm:ss");
        Assert.assertNotNull(s);
        Date d = TimeUtils.strToDate("2020-03-14");
        Assert.assertNotNull(d);
        d = TimeUtils.strToDate("2020-03-14 12:23:45");
        Assert.assertNotNull(d);
        d = TimeUtils.strToDate("2020-03-14 12:23:45.138370");
        Assert.assertNotNull(d);
        d = TimeUtils.strToDate("2020-03-14Z");
        Assert.assertNull(d);
        d = TimeUtils.strToDate("2020-x3-14");
        Assert.assertNull(d);
    }

    @Test
    public void testBase64() {
        try {
            byte[] sourceData = "HelloWorld".getBytes();
            byte[] empty = new byte[] {};
            String codedStr = Base64.encode(sourceData);
            byte[] bytes = Base64.decode(codedStr);
            Assert.assertEquals(10, bytes.length);

            bytes = Base64.decode(codedStr.toCharArray(), 0, codedStr.length());
            Assert.assertEquals(10, bytes.length);

            Base64.decode(codedStr, System.out);
            Base64.decode(codedStr.toCharArray(), 0, codedStr.length(), System.out);

            codedStr = Base64.encode("HelloWorld".getBytes(), 1, 6);
            bytes = Base64.decode(codedStr);
            Assert.assertTrue(bytes.length > 0);

            Base64.encode(sourceData, 0, sourceData.length, System.out);
            Base64.encode(sourceData, 0, sourceData.length - 2, System.out);
            Base64.encode(sourceData, 0, sourceData.length, new StringWriter());
            Base64.encode(sourceData, 0, sourceData.length - 2, new StringWriter());

            Base64.encode(empty);
            Base64.encode(empty, 0, empty.length, System.out);
            Base64.encode(empty, 0, empty.length, new StringWriter());
            System.out.println();
        } catch (Exception e) {
            Assert.fail("Base64 catch exception " + e.getMessage());
        }
    }

    @Test
    public void testNamedThreadFactory() {
        NamedThreadFactory threadFactory = new NamedThreadFactory();
        threadFactory.newThread(new Runnable() {
            @Override
            public void run() {
            }
        });
    }

    @Test
    public void testSerialization() {
        try {
            String str = new String("蚂蚁金服".getBytes("UTF-8"), "UTF-8");
            byte[] bytes = Serialization.strToBytes(str);
            String ss = Serialization.bytesToStr(bytes);
            Assert.assertEquals(str, ss);
            Serialization.encodeObString(str);

            long l = 8342516951305938535L;
            int i = 173123068;
            short s = 2078;
            byte b = 126;
            float f = 1.2f;
            double d = 1.234948;
            double delta = 0.000001;

            Assert.assertEquals(l, Serialization.decodeI64(Serialization.encodeI64(l)));
            Assert.assertEquals(i, Serialization.decodeI32(Serialization.encodeI32(i)));
            Assert.assertEquals(s, Serialization.decodeI16(Serialization.encodeI16(s)));
            Assert.assertEquals(b, Serialization.decodeI8(Serialization.encodeI8(b)));
            Assert.assertEquals(l, Serialization.decodeVi64(Serialization.encodeVi64(l)));
            Assert.assertEquals(i, Serialization.decodeVi32(Serialization.encodeVi32(i)));
            ByteBuf byteBuf = Unpooled.buffer(10);
            byteBuf.writeBytes(Serialization.encodeFloat(f));
            Assert.assertEquals(f, Serialization.decodeFloat(byteBuf), delta);
            byteBuf = Unpooled.buffer(10);
            byteBuf.writeBytes(Serialization.encodeDouble(d));
            Assert.assertEquals(d, Serialization.decodeDouble(byteBuf), delta);
            byteBuf = Unpooled.buffer(10);
            byteBuf.writeBytes(Serialization.encodeI64(l));
            Assert.assertEquals(l, Serialization.decodeI64(byteBuf));
            byteBuf = Unpooled.buffer(10);
            byteBuf.writeBytes(Serialization.encodeI32(i));
            Assert.assertEquals(i, Serialization.decodeI32(byteBuf));

            Assert.assertEquals(26, Serialization.getObStringSerializeSize(24));
            Assert.assertEquals(6, Serialization.getObStringSerializeSize(str));
            long offset = 1;
            for (int ix = 1; ix < 10; ix++) {
                Assert.assertEquals(ix, Serialization.encodeLengthVi64(offset));
                offset = offset << 7;
            }
        } catch (UnsupportedEncodingException e) {
            Assert.fail("UnsupportedEncodingException");
        }

    }

}

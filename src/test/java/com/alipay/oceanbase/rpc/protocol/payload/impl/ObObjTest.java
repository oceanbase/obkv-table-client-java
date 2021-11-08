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

package com.alipay.oceanbase.rpc.protocol.payload.impl;

import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType.CS_TYPE_BINARY;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ObObjTest {

    @Test
    public void test_min_max() {
        assertEquals(-2L, ObObj.getMax().getValue());
        assertEquals(-3L, ObObj.getMin().getValue());

        ObObj temp = new ObObj();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(ObObj.getMax().encode());
        temp.decode(buf);
        assertEquals(-2L, temp.getValue());

        temp = new ObObj();
        buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(ObObj.getMin().encode());
        temp.decode(buf);
        assertEquals(-3L, temp.getValue());
    }

    @Test
    public void test_text_type() {
        byte[] test = "test".getBytes();
        ObBytesString testBytes = new ObBytesString("test");
        byte[] bytes = Serialization.encodeBytes(test);

        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        buf.writeBytes(new byte[] { 0 });
        Object res = ObObjType.ObTextType.decode(buf, CS_TYPE_BINARY);
        assertTrue(res instanceof byte[]);

        buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        buf.writeBytes(new byte[] { 0 });
        res = ObObjType.ObTextType.decode(buf, ObCollationType.CS_TYPE_UTF8MB4_BIN);
        assertTrue(res instanceof String);

        Object comparable = ObObjType.ObTextType
            .parseToComparable(test, CS_TYPE_UTF8MB4_GENERAL_CI);
        assertEquals("test", comparable);

        comparable = ObObjType.ObTextType.parseToComparable(test, CS_TYPE_BINARY);
        assertEquals(testBytes, comparable);

        buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        buf.writeBytes(new byte[] { 0 });
        res = ObObjType.ObTinyTextType.decode(buf, CS_TYPE_BINARY);
        assertTrue(res instanceof byte[]);

        buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        buf.writeBytes(new byte[] { 0 });
        res = ObObjType.ObTinyTextType.decode(buf, ObCollationType.CS_TYPE_UTF8MB4_BIN);
        assertTrue(res instanceof String);

        comparable = ObObjType.ObTinyTextType.parseToComparable(test, CS_TYPE_UTF8MB4_GENERAL_CI);
        assertEquals("test", comparable);

        comparable = ObObjType.ObTextType.parseToComparable(test, CS_TYPE_BINARY);
        assertEquals(testBytes, comparable);

        buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        buf.writeBytes(new byte[] { 0 });
        res = ObObjType.ObMediumTextType.decode(buf, CS_TYPE_BINARY);
        assertTrue(res instanceof byte[]);

        buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        buf.writeBytes(new byte[] { 0 });
        res = ObObjType.ObMediumTextType.decode(buf, ObCollationType.CS_TYPE_UTF8MB4_BIN);
        assertTrue(res instanceof String);

        comparable = ObObjType.ObMediumTextType.parseToComparable(test, CS_TYPE_UTF8MB4_GENERAL_CI);
        assertEquals("test", comparable);

        comparable = ObObjType.ObTextType.parseToComparable(test, CS_TYPE_BINARY);
        assertEquals(testBytes, comparable);

        buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        buf.writeBytes(new byte[] { 0 });
        res = ObObjType.ObLongTextType.decode(buf, CS_TYPE_BINARY);
        assertTrue(res instanceof byte[]);

        buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        buf.writeBytes(new byte[] { 0 });
        res = ObObjType.ObLongTextType.decode(buf, ObCollationType.CS_TYPE_UTF8MB4_BIN);
        assertTrue(res instanceof String);

        comparable = ObObjType.ObLongTextType.parseToComparable(test, CS_TYPE_UTF8MB4_GENERAL_CI);
        assertEquals("test", comparable);

        comparable = ObObjType.ObLongTextType.parseToComparable(test, CS_TYPE_BINARY);
        assertEquals(testBytes, comparable);
    }

}

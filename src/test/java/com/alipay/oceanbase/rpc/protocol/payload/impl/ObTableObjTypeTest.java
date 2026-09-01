/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2026 OceanBase
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

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class ObTableObjTypeTest {
    @Test
    public void resolvesEveryDeclaredProtocolType() {
        for (ObTableObjType type : ObTableObjType.values()) {
            assertSame(type, ObTableObjType.valueOf(type.getValue() & 0xFF));
        }
    }

    @Test
    public void preservesUnknownAndSparseIdBehavior() {
        int[] unknown = { Integer.MIN_VALUE, -1, 13, 14, 15, 16, 27, 127, 128, 255, 256,
                Integer.MAX_VALUE };
        for (int value : unknown) {
            assertNull("value=" + value, ObTableObjType.valueOf(value));
        }
    }

    @SuppressWarnings("unchecked")
    @Test
    public void resolvesIdsOutsideFastLookupArrayThroughFallbackMap() throws Exception {
        Method registerLookup = ObTableObjType.class.getDeclaredMethod("registerLookup", int.class,
            ObTableObjType.class);
        registerLookup.setAccessible(true);
        Field overflowLookup = ObTableObjType.class.getDeclaredField("OVERFLOW_VALUE_LOOKUP");
        overflowLookup.setAccessible(true);
        Map<Integer, ObTableObjType> overflow = (Map<Integer, ObTableObjType>) overflowLookup
            .get(null);

        int[] overflowIds = { 128, 255, 256 };
        for (int overflowId : overflowIds) {
            try {
                registerLookup.invoke(null, overflowId, ObTableObjType.ObTableInvalidType);
                assertSame(ObTableObjType.ObTableInvalidType, ObTableObjType.valueOf(overflowId));
                if (overflowId <= 0xFF) {
                    ByteBuf encodedType = Unpooled.wrappedBuffer(new byte[] { (byte) overflowId });
                    assertSame(ObTableObjType.ObTableInvalidType,
                        ObTableSerialUtil.decodeTableObjType(encodedType));
                }
            } finally {
                overflow.remove(overflowId);
            }
            assertNull(ObTableObjType.valueOf(overflowId));
        }
    }
}

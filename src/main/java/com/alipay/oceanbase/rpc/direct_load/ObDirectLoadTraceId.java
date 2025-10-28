/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.direct_load;

import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ObDirectLoadTraceId {

    private final long uniqueId;
    private final long sequence;

    public ObDirectLoadTraceId(long uniqueId, long sequence) {
        this.uniqueId = uniqueId;
        this.sequence = sequence;
    }

    public long getUniqueId() {
        return uniqueId;
    }

    public long getSequence() {
        return sequence;
    }

    public String toString() {
        return String.format("Y%X-%016X", uniqueId, sequence);
    }

    public byte[] encode() {
        int needBytes = (int) getEncodedSize();
        ObByteBuf buf = new ObByteBuf(needBytes);
        encode(buf);
        return buf.bytes;
    }

    public void encode(ObByteBuf buf) {
        Serialization.encodeVi64(buf, uniqueId);
        Serialization.encodeVi64(buf, sequence);
    }

    public static ObDirectLoadTraceId decode(ByteBuf buf) {
        long uniqueId = Serialization.decodeVi64(buf);
        long sequence = Serialization.decodeVi64(buf);
        return new ObDirectLoadTraceId(uniqueId, sequence);
    }

    public static ObDirectLoadTraceId decode(byte[] bytes) {
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        return decode(buf);
    }

    public int getEncodedSize() {
        int len = 0;
        len += Serialization.getNeedBytes(uniqueId);
        len += Serialization.getNeedBytes(sequence);
        return len;
    }

    public static final ObDirectLoadTraceId DEFAULT_TRACE_ID;

    static {
        DEFAULT_TRACE_ID = new ObDirectLoadTraceId(0, 0);
    }

}

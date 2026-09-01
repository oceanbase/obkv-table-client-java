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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableObjType;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

/** Encodes the fixed HBase Put Q/T/V/(TTL) cell protocol without temporary ObObj objects. */
public final class ObHbasePutCellCodec {
    private static final long CELL_VERSION = 1;

    private ObHbasePutCellCodec() {
    }

    public static long getCellPayloadSize(int qualifierLength, long timestamp, int valueLength,
                                          boolean hasTtl, long ttl) {
        long contentSize = getCellContentSize(qualifierLength, timestamp, valueLength, hasTtl, ttl);
        return getCellPayloadSize(contentSize);
    }

    public static void encodeCell(ObByteBuf buf, byte[] qualifier, int qualifierOffset,
                                  int qualifierLength, long timestamp, byte[] value,
                                  int valueOffset, int valueLength, boolean hasTtl, long ttl) {
        checkSlice(qualifier, qualifierOffset, qualifierLength, "qualifier");
        checkSlice(value, valueOffset, valueLength, "value");
        long contentSize = getCellContentSize(qualifierLength, timestamp, valueLength, hasTtl, ttl);
        encodeCell(buf, qualifier, qualifierOffset, qualifierLength, timestamp, value, valueOffset,
            valueLength, hasTtl, ttl, contentSize);
    }

    static void encodeCell(ObByteBuf buf, byte[] qualifier, int qualifierOffset,
                           int qualifierLength, long timestamp, byte[] value, int valueOffset,
                           int valueLength, boolean hasTtl, long ttl, long contentSize) {
        Serialization.encodeObUniVersionHeader(buf, CELL_VERSION, contentSize);
        Serialization.encodeVi64(buf, hasTtl ? 4 : 3);
        encodeVarchar(buf, qualifier, qualifierOffset, qualifierLength);
        encodeInt64(buf, timestamp);
        encodeVarchar(buf, value, valueOffset, valueLength);
        if (hasTtl) {
            encodeInt64(buf, ttl);
        }
    }

    static long getCellContentSize(int qualifierLength, long timestamp, int valueLength,
                                   boolean hasTtl, long ttl) {
        checkLength(qualifierLength, "qualifier");
        checkLength(valueLength, "value");
        long size = Serialization.getNeedBytes(hasTtl ? 4 : 3);
        size += getVarcharEncodedSize(qualifierLength);
        size += ObTableObjType.DEFAULT_TABLE_OBJ_TYPE_SIZE + Serialization.getNeedBytes(timestamp);
        size += getVarcharEncodedSize(valueLength);
        if (hasTtl) {
            size += ObTableObjType.DEFAULT_TABLE_OBJ_TYPE_SIZE + Serialization.getNeedBytes(ttl);
        }
        return size;
    }

    static long getCellPayloadSize(long contentSize) {
        return Serialization.getObUniVersionHeaderLength(CELL_VERSION, contentSize) + contentSize;
    }

    private static long getVarcharEncodedSize(int length) {
        return ObTableObjType.DEFAULT_TABLE_OBJ_TYPE_SIZE + Serialization.getNeedBytes(length)
               + (long) length + 1;
    }

    private static void encodeVarchar(ObByteBuf buf, byte[] bytes, int offset, int length) {
        Serialization.encodeI8(buf, ObTableObjType.ObTableVarcharType.getValue());
        Serialization.encodeVi32(buf, length);
        if (length > 0) {
            buf.writeBytes(bytes, offset, length);
        }
        buf.writeByte((byte) 0);
    }

    private static void encodeInt64(ObByteBuf buf, long value) {
        Serialization.encodeI8(buf, ObTableObjType.ObTableInt64Type.getValue());
        Serialization.encodeVi64(buf, value);
    }

    private static void checkSlice(byte[] bytes, int offset, int length, String name) {
        if (bytes == null) {
            throw new IllegalArgumentException(name + " bytes is null");
        }
        if (offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IllegalArgumentException("invalid " + name + " slice: offset=" + offset
                                               + ", length=" + length + ", capacity="
                                               + bytes.length);
        }
    }

    private static void checkLength(int length, String name) {
        if (length < 0) {
            throw new IllegalArgumentException(name + " length is negative: " + length);
        }
    }
}

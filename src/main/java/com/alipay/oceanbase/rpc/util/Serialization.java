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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import io.netty.buffer.ByteBuf;

import java.io.UnsupportedEncodingException;

public class Serialization {

    private final static long   OB_MAX_V1B = (1L << 7) - 1;
    private final static long   OB_MAX_V2B = (1L << 14) - 1;
    private final static long   OB_MAX_V3B = (1L << 21) - 1;
    private final static long   OB_MAX_V4B = (1L << 28) - 1;
    private final static long   OB_MAX_V5B = (1L << 35) - 1;
    private final static long   OB_MAX_V6B = (1L << 42) - 1;
    private final static long   OB_MAX_V7B = (1L << 49) - 1;
    private final static long   OB_MAX_V8B = (1L << 56) - 1;
    private final static long   OB_MAX_V9B = (1L << 63) - 1;             // Java 中没有 Unsigned Integer

    private static final long[] OB_MAX     = { OB_MAX_V1B, OB_MAX_V2B, OB_MAX_V3B, OB_MAX_V4B,
            OB_MAX_V5B, OB_MAX_V6B, OB_MAX_V7B, OB_MAX_V8B, OB_MAX_V9B };

    /**
     * bytes length + bytes + 1
     *
     * @param val
     * @return
     */
    public static byte[] encodeObString(String val) {
        byte[] strbytes = new byte[0];
        try {
            strbytes = val.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new ObTableException(e);
        }
        byte[] length = encodeI64(strbytes.length);

        byte[] bytes = new byte[length.length + strbytes.length + 1];
        System.arraycopy(length, 0, bytes, 0, length.length);
        System.arraycopy(strbytes, 0, bytes, length.length, strbytes.length);
        bytes[bytes.length - 1] = (byte) 0x00;

        return bytes;
    }

    /**
     * Str to bytes.
     */
    public static byte[] strToBytes(String str) throws IllegalArgumentException {
        try {
            return str.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("can not encode str to bytes with utf-8");
        }
    }

    /**
     * Bytes to str.
     */
    public static String bytesToStr(byte[] bytes) throws IllegalArgumentException {
        try {
            return new String(bytes, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("can not decode bytes to str with utf-8");
        }
    }

    /**
     * Get ob string serialize size.
     */
    public static long getObStringSerializeSize(long val) {
        return encodeLengthVi64(val) + val + 1;
    }

    /**
     * Get ob string serialize size.
     */
    public static long getObStringSerializeSize(String val) {
        return getObStringSerializeSize(val.length());
    }

    /**
     * Encode length vi64.
     */
    public static long encodeLengthVi64(long len) {
        int needBytes;
        if (len < OB_MAX_V1B) {
            needBytes = 1;
        } else if (len < OB_MAX_V2B) {
            needBytes = 2;
        } else if (len < OB_MAX_V3B) {
            needBytes = 3;
        } else if (len < OB_MAX_V4B) {
            needBytes = 4;
        } else if (len < OB_MAX_V5B) {
            needBytes = 5;
        } else if (len < OB_MAX_V6B) {
            needBytes = 6;
        } else if (len < OB_MAX_V7B) {
            needBytes = 7;
        } else if (len < OB_MAX_V8B) {
            needBytes = 8;
        } else if (len < OB_MAX_V9B) {
            needBytes = 9;
        } else {
            needBytes = 10;
        }
        return needBytes;
    }

    /**
     * Encode i8.
     */
    public static byte[] encodeI8(byte val) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) ((val) & 0xff);
        return bytes;
    }

    /**
     * Encode i8.
     */
    public static byte[] encodeI8(short val) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) ((val) & 0xff);
        return bytes;
    }

    /**
     * Encode i16.
     */
    public static byte[] encodeI16(short val) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((val >> 8) & 0xff);
        bytes[1] = (byte) ((val) & 0xff);
        return bytes;
    }

    /**
     * Encode i32.
     */
    public static byte[] encodeI32(int val) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((val >> 24) & 0xff);
        bytes[1] = (byte) ((val >> 16) & 0xff);
        bytes[2] = (byte) ((val >> 8) & 0xff);
        bytes[3] = (byte) ((val) & 0xff);
        return bytes;
    }

    /**
     * Encode i64.
     */
    public static byte[] encodeI64(long val) {
        byte[] bytes = new byte[8];
        bytes[0] = (byte) ((val >> 56) & 0xff);
        bytes[1] = (byte) ((val >> 48) & 0xff);
        bytes[2] = (byte) ((val >> 40) & 0xff);
        bytes[3] = (byte) ((val >> 32) & 0xff);
        bytes[4] = (byte) ((val >> 24) & 0xff);
        bytes[5] = (byte) ((val >> 16) & 0xff);
        bytes[6] = (byte) ((val >> 8) & 0xff);
        bytes[7] = (byte) ((val) & 0xff);
        return bytes;
    }

    /**
     * Decode i8.
     */
    public static byte decodeI8(ByteBuf val) {
        return (byte) (val.readByte() & 0xff);
    }

    /**
     * Decode u i8.
     */
    public static short decodeUI8(ByteBuf val) {
        return (short) (val.readUnsignedByte() & 0xffff);
    }

    /**
     * Decode i8.
     */
    public static byte decodeI8(byte val) {
        return (byte) (val & 0xff);
    }

    /**
     * Decode i8.
     */
    public static byte decodeI8(byte[] val) {
        return (byte) (val[0] & 0xff);
    }

    /**
     * Decode i16.
     */
    public static short decodeI16(ByteBuf val) {
        short ret = (short) ((val.readByte() & 0xff) << 8);
        ret |= (val.readByte() & 0xff);
        return ret;
    }

    /**
     * Decode i16.
     */
    public static short decodeI16(byte[] val) {
        short ret = (short) ((val[0] & 0xff) << 8);
        ret |= (val[1] & 0xff);
        return ret;
    }

    /**
     * Decode i32.
     */
    public static int decodeI32(byte[] val) {
        int ret = (val[0] & 0xff) << 24;
        ret |= (val[1] & 0xff) << 16;
        ret |= (val[2] & 0xff) << 8;
        ret |= (val[3] & 0xff);
        return ret;
    }

    /**
     * Decode i32.
     */
    public static int decodeI32(ByteBuf val) {
        int ret = (val.readByte() & 0xff) << 24;
        ret |= (val.readByte() & 0xff) << 16;
        ret |= (val.readByte() & 0xff) << 8;
        ret |= (val.readByte() & 0xff);
        return ret;
    }

    /**
     * Decode i64.
     */
    public static long decodeI64(byte[] val) {
        long ret = ((long) (val[0] & 0xff)) << 56;
        ret |= ((long) (val[1] & 0xff)) << 48;
        ret |= ((long) (val[2] & 0xff)) << 40;
        ret |= ((long) (val[3] & 0xff)) << 32;
        ret |= ((long) (val[4] & 0xff)) << 24;
        ret |= ((long) (val[5] & 0xff)) << 16;
        ret |= ((long) (val[6] & 0xff)) << 8;
        ret |= ((long) (val[7] & 0xff));
        return ret;
    }

    /**
     * Decode i64.
     */
    public static long decodeI64(ByteBuf val) {
        long ret = ((long) (val.readByte() & 0xff)) << 56;
        ret |= ((long) (val.readByte() & 0xff)) << 48;
        ret |= ((long) (val.readByte() & 0xff)) << 40;
        ret |= ((long) (val.readByte() & 0xff)) << 32;
        ret |= ((long) (val.readByte() & 0xff)) << 24;
        ret |= ((long) (val.readByte() & 0xff)) << 16;
        ret |= ((long) (val.readByte() & 0xff)) << 8;
        ret |= ((long) (val.readByte() & 0xff));
        return ret;
    }

    /**
     * Decode double.
     */
    public static double decodeDouble(ByteBuf buffer) {
        return Double.longBitsToDouble(Serialization.decodeVi64(buffer));
    }

    /**
     * Encode double.
     */
    public static byte[] encodeDouble(double d) {
        return Serialization.encodeVi64(Double.doubleToRawLongBits(d));
    }

    /**
     * Decode float.
     */
    public static float decodeFloat(ByteBuf value) {
        return Float.intBitsToFloat(decodeVi32(value));
    }

    /**
     * Encode float.
     */
    public static byte[] encodeFloat(float f) {
        return encodeVi32(Float.floatToRawIntBits(f));
    }

    /**
     * Get need bytes.
     */
    public static int getNeedBytes(long l) {
        if (l < 0)
            return 10;
        int needBytes = 0;
        for (long max : OB_MAX) {
            needBytes++;
            if (l <= max)
                break;
        }
        return needBytes;
    }

    /**
     * Get need bytes.
     */
    public static int getNeedBytes(int l) {
        if (l < 0)
            return 5;
        int needBytes = 0;
        for (long max : OB_MAX) {
            needBytes++;
            if (l <= max)
                break;
        }
        return needBytes;
    }

    /**
     * Get need bytes.
     */
    public static int getNeedBytes(ObBytesString str) {
        if (str == null) {
            str = new ObBytesString(new byte[0]);
        }
        return getNeedBytes(str.length()) + str.length() + 1;
    }

    /**
     * Get need bytes.
     */
    public static int getNeedBytes(double val) {
        return getNeedBytes(Double.doubleToRawLongBits(val));
    }

    /**
     * Get need bytes.
     */
    public static int getNeedBytes(float val) {
        return getNeedBytes(Float.floatToRawIntBits(val));
    }

    /**
     * Get need bytes.
     */
    public static int getNeedBytes(byte[] bytes) {
        if (bytes == null) {
            bytes = new byte[0];
        }
        return getNeedBytes(bytes.length) + bytes.length;
    }

    /**
     * Get need bytes.
     */
    public static int getNeedBytes(String str) {
        if (str == null)
            str = "";
        try {
            int len = str.getBytes("UTF-8").length;
            return getNeedBytes(len) + len + 1;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Encode vi32.
     */
    public static byte[] encodeVi32(int i) {
        byte[] ret = new byte[getNeedBytes(i)];
        int index = 0;
        while (i < 0 || i > OB_MAX_V1B) {
            ret[index++] = (byte) (i | 0x80);
            i >>>= 7;
        }
        ret[index] = (byte) (i & 0x7f);
        return ret;
    }

    /**
     * Decode vi32.
     */
    public static int decodeVi32(byte[] value) {
        int ret = 0;
        int shift = 0;
        for (byte b : value) {
            ret |= (b & 0x7f) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return ret;
    }

    /**
     * Decode vi32.
     */
    public static int decodeVi32(ByteBuf buffer) {
        int ret = 0;
        int shift = 0;
        while (true) {
            byte b = buffer.readByte();
            ret |= (b & 0x7f) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return ret;
    }

    /**
     * Decode vi64.
     */
    public static long decodeVi64(ByteBuf buffer) {
        long ret = 0;
        int shift = 0;
        while (true) {
            byte b = buffer.readByte();
            ret |= (b & 0x7fl) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return ret;
    }

    /**
     * Encode vi64.
     */
    public static byte[] encodeVi64(long l) {
        byte[] ret = new byte[getNeedBytes(l)];
        int index = 0;
        while (l < 0 || l > OB_MAX_V1B) {
            ret[index++] = (byte) (l | 0x80);
            l >>>= 7;
        }
        ret[index] = (byte) (l & 0x7f);
        return ret;
    }

    /**
     * Decode vi64.
     */
    public static long decodeVi64(byte[] value) {
        long ret = 0;
        int shift = 0;
        for (byte b : value) {
            ret |= (b & 0x7fl) << shift;
            shift += 7;
            if ((b & 0x80) == 0) {
                break;
            }
        }
        return ret;
    }

    /**
     * Encode v string.
     */
    public static byte[] encodeVString(String str) {
        return encodeVString(str, "UTF-8");
    }

    /**
     * Encode bytes string.
     */
    public static byte[] encodeBytesString(ObBytesString str) {
        if (str == null) {
            str = new ObBytesString(new byte[0]);
        }
        byte[] data = str.bytes;
        int dataLen = data.length;
        int strLen = getNeedBytes(dataLen);
        byte[] ret = new byte[strLen + dataLen + 1];
        int index = 0;
        for (byte b : encodeVi32(dataLen)) {
            ret[index++] = b;
        }
        for (byte b : data) {
            ret[index++] = b;
        }
        ret[index] = 0;
        return ret;
    }

    /**
     * Encode bytes.
     */
    public static byte[] encodeBytes(byte[] bytes) {
        if (bytes == null) {
            bytes = new byte[0];
        }
        byte[] data = bytes;
        int dataLen = data.length;
        int strLen = getNeedBytes(dataLen);
        byte[] ret = new byte[strLen + dataLen];
        int index = 0;
        for (byte b : encodeVi32(dataLen)) {
            ret[index++] = b;
        }
        for (byte b : data) {
            ret[index++] = b;
        }
        return ret;
    }

    /**
     * Encode v string.
     */
    public static byte[] encodeVString(String str, String charset) {
        if (str == null)
            str = "";
        try {
            byte[] data = str.getBytes(charset);
            int dataLen = data.length;
            int strLen = getNeedBytes(dataLen);
            byte[] ret = new byte[strLen + dataLen + 1];
            int index = 0;
            for (byte b : encodeVi32(dataLen)) {
                ret[index++] = b;
            }
            for (byte b : data) {
                ret[index++] = b;
            }
            ret[index] = 0;
            return ret;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Decode bytes string.
     */
    public static ObBytesString decodeBytesString(ByteBuf buf) {
        int dataLen = decodeVi32(buf);
        byte[] content = new byte[dataLen];
        buf.readBytes(content);
        buf.readByte();// skip the end byte '0'
        return new ObBytesString(content);
    }

    /**
     * Decode bytes.
     */
    public static byte[] decodeBytes(ByteBuf buf) {
        int dataLen = decodeVi32(buf);
        byte[] content = new byte[dataLen];
        buf.readBytes(content);
        return content;
    }

    /**
     * Decode v string.
     */
    public static String decodeVString(ByteBuf buf) {
        return decodeVString(buf, "UTF-8");
    }

    /**
     * Decode v string.
     */
    public static String decodeVString(ByteBuf buffer, String charset) {
        int dataLen = decodeVi32(buffer);
        byte[] content = new byte[dataLen];
        buffer.readBytes(content);
        buffer.readByte();// skip the end byte '0'
        return decodeVString(content, charset);
    }

    /**
     * Decode v string.
     */
    public static String decodeVString(byte[] content) {
        return decodeVString(content, "UTF-8");
    }

    /**
     * Decode v string.
     */
    public static String decodeVString(byte[] content, String charset) {
        try {
            return new String(content, charset);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("can not decode to " + charset, e);
        }
    }

    /**
     * Get ob uni version header length.
     */
    public static long getObUniVersionHeaderLength(long version, long payloadLen) {
        return Serialization.getNeedBytes(version) + Serialization.getNeedBytes(payloadLen);
    }

    /**
     * Encode ob uni version header.
     */
    public static byte[] encodeObUniVersionHeader(long version, long payloadLen) {
        byte[] bytes = new byte[(int) getObUniVersionHeaderLength(version, payloadLen)];
        int idx = 0;

        int len = Serialization.getNeedBytes(version);
        System.arraycopy(Serialization.encodeVi64(version), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(payloadLen);
        System.arraycopy(Serialization.encodeVi64(payloadLen), 0, bytes, idx, len);

        return bytes;
    }

}

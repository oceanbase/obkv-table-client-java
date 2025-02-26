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

import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

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
     * @param val input data
     * @return output data
     */
    public static byte[] encodeObString(String val) {
        byte[] strbytes = strToBytes(val);
        byte[] length = encodeI64(strbytes.length);

        byte[] bytes = new byte[length.length + strbytes.length + 1];
        System.arraycopy(length, 0, bytes, 0, length.length);
        System.arraycopy(strbytes, 0, bytes, length.length, strbytes.length);
        bytes[bytes.length - 1] = (byte) 0x00;

        return bytes;
    }

    /**
     * Encode ObString.
     * @param buf encoded buf
     * @param val input data
     */
    public static void encodeObString(ObByteBuf buf, String val) {
        byte[] strBytes = strToBytes(val);
        encodeI64(buf, strBytes.length);
        buf.writeBytes(strBytes);
        buf.writeByte((byte) 0x00);
    }

    /**
     * Str to bytes.
     * @param str input string
     * @return output byte array
     */
    public static byte[] strToBytes(String str) {
        if (str == null)
            throw new NullPointerException();
        return str.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytesToStr(byte[] bytes) {
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Get ob string serialize size.
     * @param val input data
     * @return output size
     */
    public static long getObStringSerializeSize(long val) {
        return encodeLengthVi64(val) + val + 1;
    }

    /**
     * Get ob string serialize size.
     * @param val input data
     * @return output size
     */
    public static long getObStringSerializeSize(String val) {
        return getObStringSerializeSize(val.length());
    }

    /**
     * Encode length vi64.
     * @param len data length
     * @return bytes need for serialize data length
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
     * @param val input data
     * @return output data
     */
    public static byte[] encodeI8(byte val) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) ((val) & 0xff);
        return bytes;
    }

    /**
     * Encode i8.
     * @param buf encoded buf
     * @param val input data
     */
    public static void encodeI8(ObByteBuf buf, byte val) {
        if (buf == null)
            throw new NullPointerException();
        buf.writeByte((byte) ((val) & 0xff));
    }

    /**
     * Encode i8.
     * @param val input data
     * @return output data
     */
    public static byte[] encodeI8(short val) {
        byte[] bytes = new byte[1];
        bytes[0] = (byte) ((val) & 0xff);
        return bytes;
    }

    /**
     * Encode i8.
     * @param buf encoded buf
     * @param val input data
     */
    public static void encodeI8(ObByteBuf buf, short val) {
        if (buf == null)
            throw new NullPointerException();
        buf.writeByte((byte) ((val) & 0xff));
    }

    /**
     * Encode i16.
     * @param val input data
     * @return output data
     */
    public static byte[] encodeI16(short val) {
        byte[] bytes = new byte[4];
        bytes[0] = (byte) ((val >> 8) & 0xff);
        bytes[1] = (byte) ((val) & 0xff);
        return bytes;
    }

    /**
     * Encode i16.
     * @param buf encoded buf
     * @param val input data
     */
    public static void encodeI16(ObByteBuf buf, short val) {
        if (buf == null)
            throw new NullPointerException();
        buf.writeByte((byte) ((val >> 8) & 0xff));
        buf.writeByte((byte) ((val) & 0xff));
    }

    /**
     * Encode i32.
     * @param val input data
     * @return output data
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
     * Encode i32.
     * @param buf encoded buf
     * @param val input data
     */
    public static void encodeI32(ObByteBuf buf, int val) {
        if (buf == null)
            throw new NullPointerException();
        buf.writeByte((byte) ((val >> 24) & 0xff));
        buf.writeByte((byte) ((val >> 16) & 0xff));
        buf.writeByte((byte) ((val >> 8) & 0xff));
        buf.writeByte((byte) ((val) & 0xff));
    }

    /**
     * Encode i64.
     * @param val input data
     * @return output data
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
     * Encode i64.
     * @param buf encoded buf
     * @param val input data
     */
    public static void encodeI64(ObByteBuf buf, long val) {
        if (buf == null)
            throw new NullPointerException();
        buf.writeByte((byte) ((val >> 56) & 0xff));
        buf.writeByte((byte) ((val >> 48) & 0xff));
        buf.writeByte((byte) ((val >> 40) & 0xff));
        buf.writeByte((byte) ((val >> 32) & 0xff));
        buf.writeByte((byte) ((val >> 24) & 0xff));
        buf.writeByte((byte) ((val >> 16) & 0xff));
        buf.writeByte((byte) ((val >> 8) & 0xff));
        buf.writeByte((byte) ((val) & 0xff));
    }

    /**
     * Decode i8.
     * @param val input data buffer
     * @return output i8 data.
     */
    public static byte decodeI8(ByteBuf val) {
        return (byte) (val.readByte() & 0xff);
    }

    /**
     * Decode UI8.
     * @param val input data buffer
     * @return output UI8 data.
     */
    public static short decodeUI8(ByteBuf val) {
        return (short) (val.readUnsignedByte() & 0xffff);
    }

    /**
     * Decode i8.
     * @param val input data
     * @return output i8 data.
     */
    public static byte decodeI8(byte val) {
        return (byte) (val & 0xff);
    }

    /**
     * Decode i8.
     * @param val input data buffer
     * @return output i8 data.
     */
    public static byte decodeI8(byte[] val) {
        return (byte) (val[0] & 0xff);
    }

    /**
     * Decode i16.
     * @param val input data
     * @return output i16 data.
     */
    public static short decodeI16(ByteBuf val) {
        short ret = (short) ((val.readByte() & 0xff) << 8);
        ret |= (val.readByte() & 0xff);
        return ret;
    }

    /**
     * Decode i16.
     * @param val input data buffer
     * @return output i16 data.
     */
    public static short decodeI16(byte[] val) {
        short ret = (short) ((val[0] & 0xff) << 8);
        ret |= (val[1] & 0xff);
        return ret;
    }

    /**
     * Decode i32.
     * @param val input data buffer
     * @return output i32 data.
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
     * @param val input data
     * @return output i32 data.
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
     * @param val input data buffer
     * @return output i64 data.
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
     * @param val input data
     * @return output i64 data.
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
     * @param buffer input data buffer
     * @return output double data.
     */
    public static double decodeDouble(ByteBuf buffer) {
        return Double.longBitsToDouble(Serialization.decodeVi64(buffer));
    }

    /**
     * Encode double.
     * @param d input double data
     * @return output data buffer
     */
    public static byte[] encodeDouble(double d) {
        return Serialization.encodeVi64(Double.doubleToRawLongBits(d));
    }

    /**
     * Encode double.
     * @param buf encoded buf
     * @param d input data
     */
    public static void encodeDouble(ObByteBuf buf, double d) {
        encodeVi64(buf, Double.doubleToRawLongBits(d));
    }

    /**
     * Decode float.
     * @param value input data buffer
     * @return output float data.
     */
    public static float decodeFloat(ByteBuf value) {
        return Float.intBitsToFloat(decodeVi32(value));
    }

    /**
     * Encode float.
     * @param f input float data
     * @return output data buffer
     */
    public static byte[] encodeFloat(float f) {
        return encodeVi32(Float.floatToRawIntBits(f));
    }

    /**
     * Encode float.
     * @param buf encoded buf
     * @param f input data
     */
    public static void encodeFloat(ObByteBuf buf, float f) {
        encodeVi32(buf, Float.floatToRawIntBits(f));
    }

    /**
     * Get need bytes.
     * @param l input data
     * @return bytes need for serialize long data
     */
    public static int getNeedBytes(long l) {
        if (l < 0) {
            return 10;
        }
        if (l == 0) {
            return 1;
        }
        // 计算有效位数，然后除以7向上取整
        return (Long.SIZE - Long.numberOfLeadingZeros(l) + 6) / 7;
    }

    /**
     * Get need bytes.
     * @param l input data
     * @return bytes need for serialize int data
     */
    public static int getNeedBytes(int l) {
        if (l < 0) {
            return 5;
        }
        if (l == 0) {
            return 1;
        }
        // 计算有效位数，然后除以7向上取整
        return (Integer.SIZE - Integer.numberOfLeadingZeros(l) + 6) / 7;
    }

    /**
     * Get need bytes.
     * @param str input data
     * @return bytes need for serialize string data
     */
    public static int getNeedBytes(ObBytesString str) {
        if (str == null) {
            str = new ObBytesString(new byte[0]);
        }
        return getNeedBytes(str.length()) + str.length() + 1;
    }

    /**
     * Get need bytes.
     * @param val input data
     * @return bytes need for serialize double data
     */
    public static int getNeedBytes(double val) {
        return getNeedBytes(Double.doubleToRawLongBits(val));
    }

    /**
     * Get need bytes.
     * @param val input data
     * @return bytes need for serialize float data
     */
    public static int getNeedBytes(float val) {
        return getNeedBytes(Float.floatToRawIntBits(val));
    }

    /**
     * Get need bytes.
     * @param bytes input data
     * @return bytes need for serialize byte array data
     */
    public static int getNeedBytes(byte[] bytes) {
        if (bytes == null) {
            bytes = new byte[0];
        }
        return getNeedBytes(bytes.length) + bytes.length;
    }

    /**
     * Get need bytes.
     * @param str input data
     * @return bytes need for serialize string data
     */
    public static int getNeedBytes(String str) {
        if (str == null)
            str = "";
        int utf8Length = 0;
        for (int i = 0; i < str.length();) {
            int codePoint = str.codePointAt(i);
            utf8Length += getUtf8Length(codePoint);
            i += Character.charCount(codePoint);
        }
        return getNeedBytes(utf8Length) + utf8Length + 1;
    }

    /**
     * Get need bytes.
     * @param strs input data
     * @return bytes need for serialize string[] data
     */
    public static int getNeedBytes(String[] strs) {
        if (strs == null)
            strs = new String[0];
        int needBytes = getNeedBytes(strs.length);
        for (int i = 0; i < strs.length; ++i) {
            needBytes += getNeedBytes(strs[i]);
        }
        return needBytes;
    }

    private static int getUtf8Length(int codePoint) {
        if (codePoint <= 0x7F) {
            return 1;
        } else if (codePoint <= 0x7FF) {
            return 2;
        } else if (codePoint <= 0xFFFF) {
            return 3;
        } else {
            return 4;
        }
    }

    /**
     * Encode vi32
     * @param i input data
     * @return output data buffer
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
     * Encode vi32.
     * @param buf encoded buf
     * @param i input data
     */
    public static void encodeVi32(ObByteBuf buf, int i) {
        if (buf == null)
            throw new NullPointerException();
        while (i < 0 || i > OB_MAX_V1B) {
            buf.writeByte((byte) (i | 0x80));
            i >>>= 7;
        }
        buf.writeByte((byte) (i & 0x7f));
    }

    /**
     * Decode vi32.
     * @param value input data buffer
     * @return output int data
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
     * @param buffer input data buffer
     * @return output int data
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
     * @param buffer input data buffer
     * @return output long data
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
     * Encode vi64
     * @param l input data
     * @return output data buffer
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
     * Encode vi64.
     * @param buf encoded buf
     * @param l input data
     */
    public static void encodeVi64(ObByteBuf buf, long l) {
        if (buf == null)
            throw new NullPointerException();
        while (l < 0 || l > OB_MAX_V1B) {
            buf.writeByte((byte) (l | 0x80));
            l >>>= 7;
        }
        buf.writeByte((byte) (l & 0x7f));
    }

    /**
     * Decode vi64.
     * @param value input data buffer
     * @return output long data
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
     * Encode VString
     * @param str input data
     * @return output data buffer
     */
    public static byte[] encodeVString(String str) {
        return encodeVString(str, StandardCharsets.UTF_8);
    }

    /**
     * Encode VString.
     * @param buf encoded buf
     * @param str input data
     */
    public static void encodeVString(ObByteBuf buf, String str) {
        encodeVString(buf, str, StandardCharsets.UTF_8);
    }

    /**
     * Encode VString Array.
     * @param buf encoded buf
     * @param strs input data
     */
    public static void encodeVStringArray(ObByteBuf buf, String[] strs) {
        if (strs == null) {
            strs = new String[0];
        }
        encodeVi32(buf, strs.length);
        for (int i = 0; i < strs.length; ++i) {
            encodeVString(buf, strs[i]);
        }
    }

    /**
     * Encode VString Array.
     * @param strs input data
     * @return output data buffer
     */
    public static byte[] encodeVStringArray(String[] strs) {
        if (strs == null) {
            strs = new String[0];
        }
        final int needBytes = getNeedBytes(strs);
        ObByteBuf buf = new ObByteBuf(needBytes);
        encodeVStringArray(buf, strs);
        return buf.bytes;
    }

    /**
     * Encode BytesString
     * @param str input data
     * @return output data buffer
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
     * Encode BytesString.
     * @param buf encoded buf
     * @param str input data
     */
    public static void encodeBytesString(ObByteBuf buf, ObBytesString str) {
        if (buf == null)
            throw new NullPointerException();
        int dataLen = (str == null ? 0 : str.length());
        encodeVi32(buf, dataLen);
        if (str != null) {
            buf.writeBytes(str.bytes);
        }
        buf.writeByte((byte) 0x00);
    }

    /**
     * Encode bytes
     * @param bytes input data
     * @return output data buffer
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
     * Encode bytes
     * @param buf encoded buf
     * @param bytes input data
     */
    public static void encodeBytes(ObByteBuf buf, byte[] bytes) {
        if (buf == null)
            throw new NullPointerException();
        int bytesLen = (bytes != null ? bytes.length : 0);
        encodeVi32(buf, bytesLen);
        if (bytes != null) {
            buf.writeBytes(bytes);
        }
    }

    /**
     * Encode VString
     * @param str input data
     * @param charset input data charset
     * @return output data buffer
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

    public static byte[] encodeVString(String str, Charset charset) {
        if (str == null)
            str = "";
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
    }

    /**
     * Encode VString
     * @param buf encoded buf
     * @param str input data
     * @param charset input data charset
     */
    public static void encodeVString(ObByteBuf buf, String str, Charset charset) {
        if (buf == null || str == null)
            throw new NullPointerException();
        byte[] strBytes = str.getBytes(charset);
        encodeVi32(buf, strBytes.length);
        buf.writeBytes(strBytes);
        buf.writeByte((byte) 0x00);
    }

    /**
     * Decode byte string
     * @param buf input data
     * @return output data
     */
    public static ObBytesString decodeBytesString(ByteBuf buf) {
        int dataLen = decodeVi32(buf);
        byte[] content = new byte[dataLen];
        buf.readBytes(content);
        buf.readByte();// skip the end byte '0'
        return new ObBytesString(content);
    }

    /**
     * Decode bytes
     * @param buf input data
     * @return output data
     */
    public static byte[] decodeBytes(ByteBuf buf) {
        int dataLen = decodeVi32(buf);
        byte[] content = new byte[dataLen];
        buf.readBytes(content);
        return content;
    }

    /**
     * Decode VString
     * @param buf input data
     * @return output data
     */
    public static String decodeVString(ByteBuf buf) {
        return decodeVString(buf, StandardCharsets.UTF_8);
    }

    /**
     * Decode byte string
     * @param buffer input data
     * @param charset input data charset
     * @return output data
     */
    public static String decodeVString(ByteBuf buffer, String charset) {
        int dataLen = decodeVi32(buffer);
        byte[] content = new byte[dataLen];
        buffer.readBytes(content);
        buffer.readByte();// skip the end byte '0'
        return decodeVString(content, charset);
    }

    public static String decodeVString(ByteBuf buffer, Charset charset) {
        int dataLen = decodeVi32(buffer);
        byte[] content = new byte[dataLen];
        buffer.readBytes(content);
        buffer.readByte();// skip the end byte '0'
        return decodeVString(content, charset);
    }

    /**
     * Decode byte string array
     * @param buffer input data
     * @return output data
     */
    public static String[] decodeVStringArray(ByteBuf buffer) {
        final int count = decodeVi32(buffer);
        String[] strs = new String[count];
        for (int i = 0; i < count; ++i) {
            strs[i] = decodeVString(buffer);
        }
        return strs;
    }

    public static String[] decodeVStringArray(ByteBuf buffer, String charset) {
        final int count = decodeVi32(buffer);
        String[] strs = new String[count];
        for (int i = 0; i < count; ++i) {
            strs[i] = decodeVString(buffer, charset);
        }
        return strs;
    }

    public static String[] decodeVStringArray(ByteBuf buffer, Charset charset) {
        final int count = decodeVi32(buffer);
        String[] strs = new String[count];
        for (int i = 0; i < count; ++i) {
            strs[i] = decodeVString(buffer, charset);
        }
        return strs;
    }

    /**
     * Decode VString
     * @param content input data
     * @return output data
     */
    public static String decodeVString(byte[] content) {
        return decodeVString(content, StandardCharsets.UTF_8);
    }

    /**
     * Decode byte string
     * @param content input data
     * @param charset input data charset
     * @return output data
     */
    public static String decodeVString(byte[] content, String charset) {
        try {
            return new String(content, charset);
        } catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException("can not decode to " + charset, e);
        }
    }

    public static String decodeVString(byte[] content, Charset charset) {
        return new String(content, charset);
    }

    /**
     * Get ob uni version header length.
     * @param version version
     * @param payloadLen paylaod length
     * @return bytes need for version and payloadLen
     */
    public static long getObUniVersionHeaderLength(long version, long payloadLen) {
        return Serialization.getNeedBytes(version) + Serialization.getNeedBytes(payloadLen);
    }

    /**
     * Encode ob uni version header.
     * @param version version
     * @param payloadLen payload length
     * @return output data buffer
     */
    public static byte[] encodeObUniVersionHeader(long version, long payloadLen) {
        int versionBytes = Serialization.getNeedBytes(version);
        int payloadLenBytes = Serialization.getNeedBytes(payloadLen);
        byte[] bytes = new byte[versionBytes + payloadLenBytes];
        int idx = 0;
        int len = versionBytes;
        System.arraycopy(Serialization.encodeVi64(version), 0, bytes, idx, len);
        idx += versionBytes;
        len = payloadLenBytes;
        System.arraycopy(Serialization.encodeVi64(payloadLen), 0, bytes, idx, len);

        return bytes;
    }

    /**
     * Encode ob uni version header.
     * @param buf encoded buf
     * @param version version
     * @param payloadLen payload length
     */
    public static void encodeObUniVersionHeader(ObByteBuf buf, long version, long payloadLen) {
        encodeVi64(buf, version);
        encodeVi64(buf, payloadLen);
    }
}

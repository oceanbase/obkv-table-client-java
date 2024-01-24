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

import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.ObVString;
import com.alipay.oceanbase.rpc.util.Serialization;
import com.alipay.oceanbase.rpc.util.TimeUtils;
import io.netty.buffer.ByteBuf;

import java.sql.Timestamp;
import java.time.*;
import java.util.*;

public enum ObObjType {

    ObNullType(0) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[0];
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return null;
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 0;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_IGNORABLE,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {
            return null;
        }

    }, // 空类型

    ObTinyIntType(1) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            if (obj instanceof Boolean) {
                return new byte[] { (Boolean) obj ? (byte) 1 : (byte) 0 };
            } else {
                return new byte[] { (Byte) obj };
            }
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeI8(buf.readByte());
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 1;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Byte parseToComparable(Object o, ObCollationType ct)
                                                                   throws IllegalArgumentException,
                                                                   FeatureNotSupportedException {
            Long value = parseToLongOrNull(o);

            if (value != null && value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
                return value.byteValue();
            }

            throw new IllegalArgumentException("ObTinyIntType can not parseToComparable argument:"
                                               + o);
        }
    }, // int8, aka mysql boolean type
    ObSmallIntType(2) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi32(((Number) obj).intValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return (short) Serialization.decodeVi32(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Number) obj).intValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Short parseToComparable(Object o, ObCollationType ct)
                                                                    throws IllegalArgumentException,
                                                                    FeatureNotSupportedException {
            Long value = parseToLongOrNull(o);

            if (value != null && value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                return value.shortValue();
            }

            throw new IllegalArgumentException("ObSmallIntType can not parseToComparable argument:"
                                               + o);
        }
    }, // int16
       // Not support
    @Deprecated
    ObMediumIntType(3) { // // TODO not suppor

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi32(((Number) obj).intValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeVi32(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Number) obj).intValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Integer parseToComparable(Object o, ObCollationType ct)
                                                                      throws IllegalArgumentException,
                                                                      FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObMediumIntType is not supported.");
        }
    }, // int24
    ObInt32Type(4) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi32(((Number) obj).intValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeVi32(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Number) obj).intValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Integer parseToComparable(Object o, ObCollationType ct)
                                                                      throws IllegalArgumentException,
                                                                      FeatureNotSupportedException {
            Long value = parseToLongOrNull(o);

            if (value != null && value >= Integer.MIN_VALUE && value <= Integer.MAX_VALUE) {
                return value.intValue();
            }

            throw new IllegalArgumentException("ObInt32Type can not parseToComparable argument:"
                                               + o);
        }
    }, // int32
    ObInt64Type(5) { // Origin name: ObIntType

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi64(((Number) obj).longValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeVi64(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Number) obj).longValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Long parseToComparable(Object o, ObCollationType ct)
                                                                   throws IllegalArgumentException,
                                                                   FeatureNotSupportedException {
            return parseLong(this, o, ct);
        }
    }, // int64, aka bigint

    ObUTinyIntType(6) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[] { (Byte) obj };
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeI8(buf.readByte());
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 1;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Short parseToComparable(Object o, ObCollationType ct)
                                                                    throws IllegalArgumentException,
                                                                    FeatureNotSupportedException {
            Long value = parseToLongOrNull(o);

            if (value != null && value >= 0 && value <= Constants.UNSIGNED_INT8_MAX) {
                return value.shortValue();
            }

            throw new IllegalArgumentException("ObUTinyIntType can not parseToComparable argument:"
                                               + o);
        }
    }, // uint8
    ObUSmallIntType(7) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi32(((Number) obj).intValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeVi32(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Number) obj).intValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Integer parseToComparable(Object o, ObCollationType ct)
                                                                      throws IllegalArgumentException,
                                                                      FeatureNotSupportedException {

            Long smallIntValue = parseToLongOrNull(o);

            if (smallIntValue != null && smallIntValue >= 0
                && smallIntValue <= Constants.UNSIGNED_INT16_MAX) {
                return smallIntValue.intValue();
            }

            throw new IllegalArgumentException(
                "ObUSmallIntType can not parseToComparable argument:" + o);
        }
    }, // uint16
    ObUMediumIntType(8) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi32(((Number) obj).intValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeVi32(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Number) obj).intValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Integer parseToComparable(Object o, ObCollationType ct)
                                                                      throws IllegalArgumentException,
                                                                      FeatureNotSupportedException {

            Long mediumIntValue = parseToLongOrNull(o);

            if (mediumIntValue != null && mediumIntValue >= 0
                && mediumIntValue <= Constants.UNSIGNED_INT24_MAX) {
                return mediumIntValue.intValue();
            }

            throw new IllegalArgumentException(
                "ObUMediumIntType can not parseToComparable argument:" + o);
        }
    }, // uint24
    ObUInt32Type(9) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi32(((Number) obj).intValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeVi32(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Number) obj).intValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) -1);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Long parseToComparable(Object o, ObCollationType ct)
                                                                   throws IllegalArgumentException,
                                                                   FeatureNotSupportedException {

            Long value = parseToLongOrNull(o);

            if (value != null && value >= 0 && value <= Constants.UNSIGNED_INT32_MAX) {
                return value;
            }

            throw new IllegalArgumentException("ObUInt32Type can not parseToComparable argument:"
                                               + o);
        }
    }, // uint32
    ObUInt64Type(10) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            // FIXME ObUInt64Type use long value to encode
            return Serialization.encodeVi64(((Number) obj).longValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            // FIXME ObUInt64Type decode to long value
            return Serialization.decodeVi64(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Number) obj).longValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Long parseToComparable(Object o, ObCollationType ct)
                                                                   throws IllegalArgumentException,
                                                                   FeatureNotSupportedException {
            return parseLong(this, o, ct);
        }
    }, // uint64

    ObFloatType(11) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeFloat(((Float) obj));
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeFloat(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes((Float) obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Float parseToComparable(Object o, ObCollationType ct)
                                                                    throws IllegalArgumentException,
                                                                    FeatureNotSupportedException {
            if (o instanceof Float) {
                return (Float) o;
            }

            if (o instanceof String) {
                return Float.valueOf((String) o);
            }

            if (o instanceof ObVString) {
                return Float.valueOf(((ObVString) o).getStringVal());
            }

            throw new IllegalArgumentException("ObFloatType can not parseToComparable argument:"
                                               + o);
        }
    }, // single-precision floating point
    ObDoubleType(12) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeDouble((Double) obj);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeDouble(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes((Double) obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Double parseToComparable(Object o, ObCollationType ct)
                                                                     throws IllegalArgumentException,
                                                                     FeatureNotSupportedException {
            if (o instanceof Double) {
                return (Double) o;
            }

            if (o instanceof String) {
                return Double.valueOf((String) o);
            }

            if (o instanceof ObVString) {
                return Double.valueOf(((ObVString) o).getStringVal());
            }

            throw new IllegalArgumentException("ObDoubleType can not parseToComparable argument:"
                                               + o);
        }
    }, // double-precision floating point

    ObUFloatType(13) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[0];
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return null;
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 0;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Float parseToComparable(Object o, ObCollationType ct)
                                                                    throws IllegalArgumentException,
                                                                    FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObUFloatType is not supported .");
        }
    }, // unsigned single-precision floating point
    ObUDoubleType(14) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[0];
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return null;
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 0;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Double parseToComparable(Object o, ObCollationType ct)
                                                                     throws IllegalArgumentException,
                                                                     FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObUDoubleType is not supported.");
        }
    }, // unsigned double-precision floating point

    ObNumberType(15) { // TODO

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[0];
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return null;
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 0;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObNumberType is not supported .");
        }
    }, // aka decimal/numeric
    ObUNumberType(16) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[0];
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return null;
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 0;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObUNumberType is not supported .");
        }
    },

    // The DATETIME type is used for values that contain both date and time parts.
    // MySQL retrieves and displays DATETIME values in 'YYYY-MM-DD HH:MM:SS' format. The supported range is '1000-01-01 00:00:00' to '9999-12-31 23:59:59'.
    ObDateTimeType(17) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            // Date do not have timezone, when we use getTime, system will recognize it as our system timezone and transform it into UTC Time, which will changed the time.
            // We should add back the lose part.
            long targetTs = ((Date) obj).getTime()
                            + OffsetDateTime.now().getOffset().getTotalSeconds() * 1000L;
            return Serialization.encodeVi64(targetTs * 1000L);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return new Date(Serialization.decodeVi64(buf) / 1000L
                            - OffsetDateTime.now().getOffset().getTotalSeconds() * 1000L);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Date) obj).getTime() * 1000L);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            // scale set into 6 means microSecond
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 6);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Date parseToComparable(Object o, ObCollationType ct)
                                                                   throws IllegalArgumentException,
                                                                   FeatureNotSupportedException {
            if (o instanceof String) {
                return TimeUtils.strToDate((String) o);
            }
            return (Date) o;
        }
    },
    // The TIMESTAMP data type is used for values that contain both date and time parts.
    // TIMESTAMP has a range of '1970-01-01 00:00:01' UTC to '2038-01-19 03:14:07' UTC.
    ObTimestampType(18) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            long timeInMicroseconds = ((Timestamp)obj).getTime() * 1_000;
            int nanoSeconds = ((Timestamp)obj).getNanos() % 1_000_000;
            timeInMicroseconds += nanoSeconds / 1_000;
            return Serialization.encodeVi64(timeInMicroseconds);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            long timestampMicro = Serialization.decodeVi64(buf);
            long timestampMilli = timestampMicro / 1_000;
            int nanos = (int) (timestampMicro % 1_000) * 1_000;
            Timestamp timestamp = new Timestamp(timestampMilli);
            timestamp.setNanos(timestamp.getNanos() + nanos);
            return timestamp;
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes(((Date) obj).getTime() * 1000L);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            // scale set into 6 means microSecond
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 6);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Timestamp parseToComparable(Object o, ObCollationType ct)
                                                                        throws IllegalArgumentException,
                                                                        FeatureNotSupportedException {
            return parseTimestamp(this, o, ct);
        }
    },
    // The DATE type is used for values with a date part but no time part.
    // MySQL retrieves and displays DATE values in 'YYYY-MM-DD' format. The supported range is '1000-01-01' to '9999-12-31'.
    ObDateType(19) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi32((int) ((Date) obj).getTime());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return new Date(Serialization.decodeVi32(buf) * 1000L);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes((int) ((Date) obj).getTime());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Date parseToComparable(Object o, ObCollationType ct)
                                                                   throws IllegalArgumentException,
                                                                   FeatureNotSupportedException {

            if (o instanceof Date) {
                return (Date) o;
            }

            if (o instanceof String) {
                return TimeUtils.strToDate((String) o);
            }

            if (o instanceof ObVString) {
                return TimeUtils.strToDate(((ObVString) o).getStringVal());
            }

            if (o instanceof Long) {
                return new Date((Long) o);
            }

            throw new IllegalArgumentException("ObDateType can not parseToComparable argument:" + o);
        }
    },
    ObTimeType(20) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi64((int) ((Date) obj).getTime());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return new Timestamp(Serialization.decodeVi64(buf));
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes((int) ((Date) obj).getTime());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Timestamp parseToComparable(Object o, ObCollationType ct)
                                                                        throws IllegalArgumentException,
                                                                        FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObTimeType is not supported .");
        }
    },
    ObYearType(21) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[] { (Byte) obj };
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeI8(buf.readByte());
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 1;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObYearType is not supported .");
        }
    },

    ObVarcharType(22) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            if (obj instanceof byte[]) {
                ObBytesString bytesString = new ObBytesString((byte[]) obj);
                return Serialization.encodeBytesString(bytesString);
            } else if (obj instanceof ObVString) {
                return ((ObVString) obj).getEncodeBytes();
            } else {
                return Serialization.encodeVString((String) obj);
            }
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return decodeText(buf, type);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return getTextEncodedSize(obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_EXPLICIT,
                ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {

            return parseTextToComparable(this, o, ct);
        }

    }, // charset: utf8mb4 or binary
    ObCharType(23) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVString((String) obj);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return decodeText(buf, type);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return getTextEncodedSize(obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_EXPLICIT,
                ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {
            return parseTextToComparable(this, o, ct);
        }

    }, // charset: utf8mb4 or binary

    @Deprecated
    ObHexStringType(24) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[0];
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return null;
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 0;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public String parseToComparable(Object o, ObCollationType ct)
                                                                     throws IllegalArgumentException,
                                                                     FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObHexStringType is not supported .");
        }

    }, // hexadecimal literal, e.g. X'42', 0x42, b'1001', 0b1001

    ObExtendType(25) { // MIN_OBJECT / MAX_OBJECT

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVi64((Long) obj);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return Serialization.decodeVi64(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return Serialization.getNeedBytes((Long) obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObUnknownType is not supported .");
        }

    }, // Min, Max, NOP etc.
    @Deprecated
    ObUnknownType(26) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return new byte[0];
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return null;
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return 0;
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {
            throw new FeatureNotSupportedException("ObUnknownType is not supported .");
        }

    }, // For question mark(?) in prepared statement, no need to serialize
       // @note future new types to be defined here !!!
    ObTinyTextType(27) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            return Serialization.encodeVString((String) obj);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return decodeText(buf, type);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return getTextEncodedSize(obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {

            return parseTextToComparable(this, o, ct);
        }

    },
    ObTextType(28) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            // ObTextType use string to encode
            return Serialization.encodeVString((String) obj);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return decodeText(buf, type);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return getTextEncodedSize(obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct)
                                                                         throws IllegalArgumentException,
                                                                         FeatureNotSupportedException {
            return parseTextToComparable(this, o, ct);
        }

    },
    ObMediumTextType(29) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            // ObMediumTextType use string to encode
            return Serialization.encodeVString((String) obj);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return decodeText(buf, type);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return getTextEncodedSize(obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct) {
            return parseTextToComparable(this, o, ct);
        }

    },
    ObLongTextType(30) {
        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            // ObLongTextType use string to encode
            return Serialization.encodeVString((String) obj);
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            return decodeText(buf, type);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            return getTextEncodedSize(obj);
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct) {
            return parseTextToComparable(this, o, ct);
        }
    },
    ObBitType(31) { // TODO not support

        /*
         * Encode.
         */
        @Override
        public byte[] encode(Object obj) {
            // FIXME ObBitType use long value to encode
            return Serialization.encodeVi64(((Number) obj).longValue());
        }

        /*
         * Decode.
         */
        @Override
        public Object decode(ByteBuf buf, ObCollationType type) {
            // FIXME ObBitType decode to long value
            return Serialization.decodeVi64(buf);
        }

        /*
         * Get encoded size.
         */
        @Override
        public int getEncodedSize(Object obj) {
            // FIXME ObBitType calculate long value bytes
            return Serialization.getNeedBytes(((Number) obj).longValue());
        }

        /*
         * Get default obj meta.
         */
        @Override
        public ObObjMeta getDefaultObjMeta() {
            // FIXME ObBitType default meta level is 5 and cs type is binary
            return new ObObjMeta(this, ObCollationLevel.CS_LEVEL_NUMERIC,
                ObCollationType.CS_TYPE_BINARY, (byte) 10);
        }

        /*
         * Parse to comparable.
         */
        @Override
        public Comparable parseToComparable(Object o, ObCollationType ct) {
            throw new FeatureNotSupportedException("ObBitType is not supported .");
        }
    };
    /*
    ObEnumType(36)
    ObSetType(37)
      ObMaxType                    // invalid type, or count of obj type
    */
    private int                            value;
    private static Map<Integer, ObObjType> map = new HashMap<Integer, ObObjType>();

    ObObjType(int value) {
        this.value = value;
    }

    static {
        for (ObObjType type : ObObjType.values()) {
            map.put(type.value, type);
        }
    }

    /*
     * Default obj meta.
     */
    public static ObObjMeta defaultObjMeta(Object object) {
        return valueOfType(object).getDefaultObjMeta();
    }

    /*
     * Value of type.
     */
    public static ObObjType valueOfType(Object object) {
        if (object == null) {
            // only for GET operation default value
            return ObNullType;
        } else if (object instanceof Boolean) {
            return ObTinyIntType;
        } else if (object instanceof Byte) {
            return ObTinyIntType;
        } else if (object instanceof Short) {
            return ObSmallIntType;
        } else if (object instanceof Integer) {
            return ObInt32Type;
        } else if (object instanceof Long) {
            return ObInt64Type;
        } else if (object instanceof String) {
            return ObVarcharType;
        } else if (object instanceof byte[]) {
            return ObVarcharType;
        } else if (object instanceof ObVString) {
            return ObVarcharType;
        } else if (object instanceof Double) {
            return ObDoubleType;
        } else if (object instanceof Float) {
            return ObFloatType;
        } else if (object instanceof Timestamp) {
            return ObTimestampType;
        } else if (object instanceof java.sql.Date) {
            return ObDateTimeType;
        } else if (object instanceof Date) {
            return ObDateTimeType;
        } else if (object instanceof ObObj) {
            return ((ObObj) object).getMeta().getType();
        }
        throw new IllegalArgumentException("cannot get ObObjType, invalid object type: "
                                           + object.getClass().getName());
    }

    /*
     * Value of.
     */
    public static ObObjType valueOf(int value) {
        return map.get(value);
    }

    /*
     * Get value.
     */
    public int getValue() {
        return value;
    }

    /*
     * Get byte value.
     */
    public byte getByteValue() {
        return (byte) value;
    }

    public abstract byte[] encode(Object obj);

    public abstract Object decode(ByteBuf buf, ObCollationType type);

    public abstract int getEncodedSize(Object obj);

    public abstract ObObjMeta getDefaultObjMeta();

    public abstract Comparable parseToComparable(Object o, ObCollationType ct)
                                                                              throws IllegalArgumentException,
                                                                              FeatureNotSupportedException;

    /*
     * Parse to long or null.
     */
    public static Long parseToLongOrNull(Object object) {
        Long value = null;

        if (object instanceof String) {
            value = Long.valueOf((String) object);
        } else if (object instanceof ObVString) {
            value = Long.valueOf(((ObVString) object).getStringVal());
        } else if (object instanceof Long) {
            value = (Long) object;
        } else if (object instanceof Integer) {
            value = Long.valueOf((Integer) object);
        } else if (object instanceof Short) {
            value = Long.valueOf((Short) object);
        } else if (object instanceof Byte) {
            value = Long.valueOf((Byte) object);
        }

        return value;
    }

    /*
     * Decode text.
     */
    public Object decodeText(ByteBuf buf, ObCollationType type) {
        if (ObCollationType.CS_TYPE_BINARY.equals(type)) {
            return Serialization.decodeBytesString(buf).bytes;
        } else {
            return Serialization.decodeVString(buf);
        }
    }

    /*
     * Get text encoded size.
     */
    public static int getTextEncodedSize(Object obj) {
        if (obj instanceof byte[]) {
            ObBytesString bytesString = new ObBytesString((byte[]) obj);
            return Serialization.getNeedBytes(bytesString);
        } else if (obj instanceof ObVString) {
            return ((ObVString) obj).getEncodeNeedBytes();
        } else {
            return Serialization.getNeedBytes((String) obj);
        }
    }

    /*
     * Parse text to comparable.
     */
    public static Comparable parseTextToComparable(ObObjType obObjType, Object object,
                                                   ObCollationType collationType) {
        if (collationType == ObCollationType.CS_TYPE_BINARY) {

            if (object instanceof ObBytesString) {
                return (ObBytesString) object;
            }

            if (object instanceof byte[]) {
                return new ObBytesString((byte[]) object);
            }

            if (object instanceof String) {
                return new ObBytesString((String) object);
            }

            if (object instanceof ObVString) {
                return new ObBytesString(((ObVString) object).getBytesVal());
            }
        } else {

            if (object instanceof String) {
                return (String) object;
            }
            if (object instanceof ObBytesString) {
                return Serialization.decodeVString(((ObBytesString) object).bytes);
            }

            if (object instanceof byte[]) {
                try {
                    return Serialization.decodeVString((byte[]) object);
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException(
                        obObjType.name()
                                + "can not parseToComparable byte array to string with utf-8 charset",
                        e);
                }
            }

            if (object instanceof ObVString) {
                return ((ObVString) object).getStringVal();
            }
        }

        throw new IllegalArgumentException(obObjType.name() + "can not parseToComparable argument:"
                                           + object);
    }

    /*
     * Parse timestamp.
     */
    public static Timestamp parseTimestamp(ObObjType obObjType, Object object,
                                           ObCollationType collationType) {
        if (object instanceof Timestamp) {
            return (Timestamp) object;
        }

        if (object instanceof String) {
            return TimeUtils.strToTimestamp((String) object);
        }

        if (object instanceof ObVString) {
            return TimeUtils.strToTimestamp(((ObVString) object).getStringVal());
        }

        if (object instanceof Long) {
            return new Timestamp((Long) object);
        }

        throw new IllegalArgumentException(obObjType.name() + " can not parseToComparable with "
                                           + collationType + "argument:" + object);
    }

    /*
     * Parse long.
     */
    public static Long parseLong(ObObjType obObjType, Object object, ObCollationType collationType) {
        Long value = parseToLongOrNull(object);

        if (value != null) {
            return value;
        }

        throw new IllegalArgumentException("ObUInt64Type can not parseToComparable argument:"
                                           + object);
    }

}

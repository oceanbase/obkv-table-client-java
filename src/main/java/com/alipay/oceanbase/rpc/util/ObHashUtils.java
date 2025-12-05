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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.location.model.partition.ObPartFuncType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.hash.MurmurHash;
import com.alipay.oceanbase.rpc.util.hash.ObHashSortBin;
import com.alipay.oceanbase.rpc.util.hash.ObHashSortUtf8mb4;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType.*;

public class ObHashUtils {
    /**
     * Varchar hash.
     * @param varchar input varchar data
     * @param collationType collation type
     * @param hashCode old hashCode
     * @param partFuncType partition function type
     * @return new hashCode
     */
    public static long varcharHash(Object varchar, ObCollationType collationType, long hashCode,
                                   ObPartFuncType partFuncType) {
        return varcharHash(varchar, collationType, hashCode, partFuncType, 0);
    }

    /**
     * Varchar hash with OB version.
     * @param varchar input varchar data
     * @param collationType collation type
     * @param hashCode old hashCode
     * @param partFuncType partition function type
     * @param obVersion OceanBase server version (0 means use global version)
     * @return new hashCode
     */
    public static long varcharHash(Object varchar, ObCollationType collationType, long hashCode,
                                   ObPartFuncType partFuncType, long obVersion) {
        // magic number, the same number with observer
        long seed = 0xc6a4a7935bd1e995L;
        byte[] bytes;
        // TODO: Complete support all encodings supported in OceanBase.
        // Right Now, only UTF8 String is supported, aligned with the Serialization.
        if (varchar instanceof String) {
            try {
                bytes = ((String) varchar).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("Unsupported Encoding for Object = " + varchar,
                    e);
            }
        } else if (varchar instanceof byte[]) {
            bytes = (byte[]) varchar;
        } else if (varchar instanceof ObBytesString) {
            bytes = ((ObBytesString) varchar).bytes;
        } else {
            throw new IllegalArgumentException("varchar not supported , ObCollationType = "
                                               + collationType + " Object =" + varchar);
        }
        int obVsnMajor = ObGlobal.getObVsnMajorRequired(obVersion);
        switch (collationType) {
            case CS_TYPE_UTF8MB4_GENERAL_CI:
                if (partFuncType == ObPartFuncType.KEY_V3
                    || partFuncType == ObPartFuncType.KEY_IMPLICIT_V2 || obVsnMajor >= 4) {
                    hashCode = ObHashSortUtf8mb4.obHashSortUtf8Mb4(bytes, bytes.length, hashCode,
                        seed, true);
                } else {
                    hashCode = ObHashSortUtf8mb4.obHashSortUtf8Mb4(bytes, bytes.length, hashCode,
                        seed, false);
                }
                break;
            case CS_TYPE_UTF8MB4_BIN:
                if (partFuncType == ObPartFuncType.KEY_V3
                    || partFuncType == ObPartFuncType.KEY_IMPLICIT_V2 || obVsnMajor >= 4) {
                    hashCode = MurmurHash.hash64a(bytes, bytes.length, hashCode);
                } else {
                    hashCode = ObHashSortUtf8mb4.obHashSortMbBin(bytes, bytes.length, hashCode,
                        seed);
                }
                break;
            case CS_TYPE_BINARY:
                if (partFuncType == ObPartFuncType.KEY_V3
                    || partFuncType == ObPartFuncType.KEY_IMPLICIT_V2 || obVsnMajor >= 4) {
                    hashCode = MurmurHash.hash64a(bytes, bytes.length, hashCode);
                } else {
                    hashCode = ObHashSortBin.obHashSortBin(bytes, bytes.length, hashCode, seed);
                }
                break;
            case CS_TYPE_INVALID:
            case CS_TYPE_COLLATION_FREE:
            case CS_TYPE_MAX:
            default:
                throw new IllegalArgumentException("not supported collation type, type = "
                                                   + collationType);
        }

        return hashCode;
    }

    /**
     * To hash code
     * @param value input data
     * @param refColumn data info, include type and collation type
     * @param hashCode old hashCode
     * @param partFuncType partition function type
     * @return new hashCode
     */
    public static long toHashcode(Object value, ObColumn refColumn, long hashCode,
                                  ObPartFuncType partFuncType) {
        return toHashcode(value, refColumn, hashCode, partFuncType, 0);
    }

    /**
     * To hash code with OB version
     * @param value input data
     * @param refColumn data info, include type and collation type
     * @param hashCode old hashCode
     * @param partFuncType partition function type
     * @param obVersion OceanBase server version (0 means use global version)
     * @return new hashCode
     */
    public static long toHashcode(Object value, ObColumn refColumn, long hashCode,
                                  ObPartFuncType partFuncType, long obVersion) {

        ObObjType type = refColumn.getObObjType();
        int typeValue = type.getValue();
        ObCollationType collationType = refColumn.getObCollationType();

        if (typeValue >= ObTinyIntType.getValue() && typeValue <= ObUInt64Type.getValue()) {
            if (value instanceof Integer) {
                return ObHashUtils.longHash(((Integer) value).longValue(), hashCode);
            } else if (value instanceof Short) {
                return ObHashUtils.longHash(((Short) value).longValue(), hashCode);
            } else if (value instanceof Byte) {
                return ObHashUtils.longHash(((Byte) value).longValue(), hashCode);
            } else if (value instanceof Boolean) {
                return ObHashUtils.longHash((Boolean) value ? 1L : 0L, hashCode);
            } else {
                return ObHashUtils.longHash((Long) value, hashCode);
            }
        } else if (ObTimestampType.getValue() == typeValue) {
            return ObHashUtils.timeStampHash((Timestamp) value, hashCode);
        } else if (ObDateTimeType.getValue() == typeValue) {
            return ObHashUtils.dateTimeHash((java.util.Date) value, hashCode);
        } else if (ObDateType.getValue() == typeValue) {
            return ObHashUtils.dateHash((Date) value, hashCode);
        } else if (ObVarcharType.getValue() == typeValue || ObCharType.getValue() == typeValue) {
            return ObHashUtils.varcharHash(value, collationType, hashCode, partFuncType, obVersion);
        }

        throw new ClassCastException("unexpected type" + type);
    }

    private static byte[] longToByteArray(long l) {
        return new byte[] { (byte) (l & 0xFF), (byte) ((l >> 8) & 0xFF), (byte) ((l >> 16) & 0xFF),
                (byte) ((l >> 24) & 0xFF), (byte) ((l >> 32) & 0xFF), (byte) ((l >> 40) & 0xFF),
                (byte) ((l >> 48) & 0xFF), (byte) ((l >> 56) & 0xFF) };
    }

    /**
     * Long hash
     * @param l input data
     * @param hashCode old hashCode
     * @return new hashCode
     */
    public static long longHash(long l, long hashCode) {
        return MurmurHash.hash64a(longToByteArray(l), 8, hashCode);
    }

    /**
     * Date hash.
     * @param d input data
     * @param hashCode old hashCode
     * @return new hashCode
     */
    public static long dateHash(Date d, long hashCode) {
        return longHash(d.getTime(), hashCode);
    }

    /**
     * Datetime hash (support DateTime(6)).
     * @param d input data
     * @param hashCode old hashCode
     * @return new hashCode
     */
    public static long dateTimeHash(java.util.Date d, long hashCode) {
        return longHash(
            (d.getTime() + OffsetDateTime.now().getOffset().getTotalSeconds() * 1000L) * 1000,
            hashCode);
    }

    /**
     * Time stamp hash
     * @param ts input data
     * @param hashCode old hashCode
     * @return new hashCode
     */
    public static long timeStampHash(Timestamp ts, long hashCode) {
        return longHash(ts.getTime(), hashCode);
    }
}

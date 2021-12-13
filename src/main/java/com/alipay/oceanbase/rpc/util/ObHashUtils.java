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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.hash.MurmurHash;
import com.alipay.oceanbase.rpc.util.hash.ObHashSortBin;
import com.alipay.oceanbase.rpc.util.hash.ObHashSortUtf8mb4;

import java.io.UnsupportedEncodingException;
import java.sql.Date;
import java.sql.Timestamp;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType.*;

public class ObHashUtils {
    /**
     * Varchar hash.
     * @param varchar input varchar data
     * @param collationType collation type
     * @param hashCode old hashCode
     * @return new hashCode
     */
    public static long varcharHash(Object varchar, ObCollationType collationType, long hashCode) {
        // magic number, the same number with observer
        long seed = 0xc6a4a7935bd1e995L;
        byte[] bytes;
        // TODO: Complete support all encodings supported in OceanBase.
        // Right Now, only UTF8 String is supported, aligned with the Serialization.
        if (varchar instanceof String) {
            try {
                bytes = ((String) varchar).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("Unsupported Encoding for Object = " + varchar);
            }
        } else if (varchar instanceof byte[]) {
            bytes = (byte[]) varchar;
        } else if (varchar instanceof ObBytesString) {
            bytes = ((ObBytesString) varchar).bytes;
        } else {
            throw new IllegalArgumentException("varchar not supported , ObCollationType = "
                                               + collationType + " Object =" + varchar);
        }
        switch (collationType) {
            case CS_TYPE_UTF8MB4_GENERAL_CI:
                hashCode = ObHashSortUtf8mb4.obHashSortUtf8Mb4(bytes, bytes.length, hashCode, seed);
                break;
            case CS_TYPE_UTF8MB4_BIN:
                hashCode = ObHashSortUtf8mb4.obHashSortMbBin(bytes, bytes.length, hashCode, seed);
                break;
            case CS_TYPE_BINARY:
                hashCode = ObHashSortBin.obHashSortBin(bytes, bytes.length, hashCode, seed);
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
     * @return new hashCode
     */
    public static long toHashcode(Object value, ObColumn refColumn, long hashCode) {

        ObObjType type = refColumn.getObObjType();
        int typeValue = type.getValue();
        ObCollationType collationType = refColumn.getObCollationType();

        if (typeValue >= ObTinyIntType.getValue() && typeValue <= ObUInt64Type.getValue()) {
            return ObHashUtils.longHash((Long) value, hashCode);
        } else if (ObDateTimeType.getValue() == typeValue
                   || ObTimestampType.getValue() == typeValue) {
            return ObHashUtils.timeStampHash((Timestamp) value, hashCode);
        } else if (ObDateType.getValue() == typeValue) {
            return ObHashUtils.dateHash((Date) value, hashCode);
        } else if (ObVarcharType.getValue() == typeValue || ObCharType.getValue() == typeValue) {
            return ObHashUtils.varcharHash(value, collationType, hashCode);
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
        return MurmurHash.hash64(longToByteArray(l), 8, hashCode);
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
     * Time stamp hash
     * @param ts input data
     * @param hashCode old hashCode
     * @return new hashCode
     */
    public static long timeStampHash(Timestamp ts, long hashCode) {
        return longHash(ts.getTime(), hashCode);
    }
}

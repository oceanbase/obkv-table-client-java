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

package com.alipay.oceanbase.rpc.protocol.packet;

public enum ObCompressType {
    INVALID_COMPRESSOR(0), NONE_COMPRESSOR(1), LZ4_COMPRESSOR(2), SNAPPY_COMPRESSOR(3), ZLIB_COMPRESSOR(
                                                                                                        4), ZSTD_COMPRESSOR(
                                                                                                                            5);

    private final int code;

    ObCompressType(int code) {
        this.code = code;
    }

    /**
     * Get code.
     */
    public int getCode() {
        return code;
    }

    /**
     * Value of.
     */
    public static ObCompressType valueOf(int code) {
        switch (code) {
            case 1:
                return NONE_COMPRESSOR;
            case 2:
                return LZ4_COMPRESSOR;
            case 3:
                return SNAPPY_COMPRESSOR;
            case 4:
                return ZLIB_COMPRESSOR;
            case 5:
                return ZSTD_COMPRESSOR;
            default:
                return INVALID_COMPRESSOR;
        }
    }
}

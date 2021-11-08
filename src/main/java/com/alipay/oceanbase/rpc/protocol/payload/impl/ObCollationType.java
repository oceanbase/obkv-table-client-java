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

import java.util.HashMap;
import java.util.Map;

public enum ObCollationType {

    CS_TYPE_INVALID(0), //
    CS_TYPE_UTF8MB4_GENERAL_CI(45), // 默认。对大小写不敏感
    CS_TYPE_UTF8MB4_BIN(46), // 对大小写敏感
    CS_TYPE_BINARY(63), //
    CS_TYPE_COLLATION_FREE(100), // mysql中间没有使用这个
    CS_TYPE_MAX(101);

    private int                                  value;
    private static Map<Integer, ObCollationType> map = new HashMap<Integer, ObCollationType>();

    ObCollationType(int value) {
        this.value = value;
    }

    static {
        for (ObCollationType type : ObCollationType.values()) {
            map.put(type.value, type);
        }
    }

    /**
     * Value of.
     */
    public static ObCollationType valueOf(int value) {
        return map.get(value);
    }

    /**
     * Get value.
     */
    public int getValue() {
        return value;
    }

    /**
     * Get byte value.
     */
    public byte getByteValue() {
        return (byte) value;
    }

}

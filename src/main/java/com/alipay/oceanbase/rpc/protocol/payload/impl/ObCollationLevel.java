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

public enum ObCollationLevel {

    CS_LEVEL_EXPLICIT(0), //
    CS_LEVEL_NONE(1), //
    CS_LEVEL_IMPLICIT(2), //
    CS_LEVEL_SYSCONST(3), //
    CS_LEVEL_COERCIBLE(4), //
    CS_LEVEL_NUMERIC(5), //
    CS_LEVEL_IGNORABLE(6), //
    // here we didn't define CS_LEVEL_INVALID as 0,
    // since 0 is a valid value for CS_LEVEL_EXPLICIT in mysql 5.6.
    // fortunately we didn't need to use it to define array like charset_arr,
    // and we didn't persist it on storage.
    CS_LEVEL_INVALID(127);

    private int                                   value;
    private static Map<Integer, ObCollationLevel> map = new HashMap<Integer, ObCollationLevel>();

    ObCollationLevel(int value) {
        this.value = value;
    }

    static {
        for (ObCollationLevel type : ObCollationLevel.values()) {
            map.put(type.value, type);
        }
    }

    /*
     * Value of.
     */
    public static ObCollationLevel valueOf(int value) {
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

}

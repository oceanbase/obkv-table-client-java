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

import java.util.HashMap;
import java.util.Map;

public enum ObReadConsistency {

    STRONG(0), WEAK(1);

    private int                                          value;
    private static Map<Integer, ObReadConsistency> map = new HashMap<Integer, ObReadConsistency>();

    ObReadConsistency(int value) {
        this.value = value;
    }

    static {
        for (ObReadConsistency type : ObReadConsistency.values()) {
            map.put(type.value, type);
        }
    }

    public static ObReadConsistency getByName(String name) throws IllegalArgumentException {
        if (name.equalsIgnoreCase("weak")) {
            return WEAK;
        } else if (name.equalsIgnoreCase("strong")) {
            return STRONG;
        } else {
            throw new IllegalArgumentException("readConsistency is invalid: " + name);
        }
    }

    /*
     * Value of.
     */
    public static ObReadConsistency valueOf(int value) {
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


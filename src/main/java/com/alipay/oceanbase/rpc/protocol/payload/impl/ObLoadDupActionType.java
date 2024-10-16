/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

public enum ObLoadDupActionType {

    STOP_ON_DUP(0), REPLACE(1), IGNORE(2), INVALID_MODE(3);

    private final int                                      value;
    private static final Map<Integer, ObLoadDupActionType> map = new HashMap<Integer, ObLoadDupActionType>();

    static {
        for (ObLoadDupActionType type : ObLoadDupActionType.values()) {
            map.put(type.value, type);
        }
    }

    public static ObLoadDupActionType valueOf(int value) {
        return map.get(value);
    }

    ObLoadDupActionType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public byte getByteValue() {
        return (byte) value;
    }

}

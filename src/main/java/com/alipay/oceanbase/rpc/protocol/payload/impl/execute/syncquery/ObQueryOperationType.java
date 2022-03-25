/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2022 Ant Financial Services Group
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery;

import java.util.HashMap;
import java.util.Map;

public enum ObQueryOperationType {
    QUERY_START(0), QUERY_NEXT(1);

    private int                                       value;
    private static Map<Integer, ObQueryOperationType> map = new HashMap<Integer, ObQueryOperationType>();

    ObQueryOperationType(int value) {
        this.value = value;
    }

    static {
        for (ObQueryOperationType type : ObQueryOperationType.values()) {
            map.put(type.value, type);
        }
    }

    public static ObQueryOperationType valueOf(int value) {
        return map.get(value);
    }

    public int getValue() {
        return value;
    }

    public byte getByteValue() {
        return (byte) value;
    }
}

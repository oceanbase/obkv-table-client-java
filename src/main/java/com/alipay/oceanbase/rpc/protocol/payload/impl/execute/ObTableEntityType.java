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

public enum ObTableEntityType {

    DYNAMIC(0), KV(1), HKV(2);

    private int                                    value;
    private static Map<Integer, ObTableEntityType> map = new HashMap<Integer, ObTableEntityType>();

    ObTableEntityType(int value) {
        this.value = value;
    }

    static {
        for (ObTableEntityType type : ObTableEntityType.values()) {
            map.put(type.value, type);
        }
    }

    /**
     * Value of.
     */
    public static ObTableEntityType valueOf(int value) {
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

/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

import com.alipay.oceanbase.rpc.protocol.payload.Constants;

import java.util.HashMap;
import java.util.Map;

public enum ObTableSingleOpType {
    SINGLE_GET(0),
    SINGLE_INSERT(1),
    SINGLE_DEL(2),
    SINGLE_UPDATE(3),
    SINGLE_INSERT_OR_UPDATE(4),
    SINGLE_REPLACE(5),
    SINGLE_INCREMENT(6),
    SINGLE_APPEND(7),
    SINGLE_MAX(63), // reserved
    SYNC_QUERY(64),
    ASYNC_QUERY(65),
    QUERY_AND_MUTATE(66),
    SINGLE_OP_TYPE_MAX(67);

    private int value;
    private static Map<Integer, ObTableSingleOpType> map = new HashMap<>();

    ObTableSingleOpType(int value) {
        this.value = value;
    }

    static {
        for (ObTableSingleOpType type : ObTableSingleOpType.values()) {
            map.put(type.value, type);
        }
    }

    /*
     * Value of.
     */
    public static ObTableSingleOpType valueOf(byte value) {
        return map.get(value);
    }

    /*
     * Get value.
     */
    public byte getValue() {
        return (byte) value;
    }

    /*
     * Set value.
     */
    public void setValue(byte value) {
        this.value = value;
    }

}

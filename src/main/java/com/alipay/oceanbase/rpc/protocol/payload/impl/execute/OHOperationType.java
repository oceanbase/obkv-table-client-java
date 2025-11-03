/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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

import java.util.*;

public enum OHOperationType {
    INVALID(0),
    PUT(1),
    PUT_LIST(2),
    DELETE(3),
    DELETE_LIST(4),
    GET(5),
    GET_LIST(6),
    EXISTS(7),
    EXISTS_LIST(8),
    BATCH(9),
    BATCH_CALLBACK(10),
    SCAN(11),
    CHECK_AND_PUT(12),
    CHECK_AND_DELETE(13),
    CHECK_AND_MUTATE(14),
    APPEND(15),
    INCREMENT(16),
    INCREMENT_COLUMN_VALUE(17),
    MUTATE_ROW(18);

    private final int                                  value;
    private static final Map<Integer, OHOperationType> map = new HashMap<Integer, OHOperationType>();

    static {
        for (OHOperationType type : OHOperationType.values()) {
            map.put(type.value, type);
        }
    }

    OHOperationType(int value) {
        this.value = value;
    }

    public static OHOperationType valueOf(int value) {
        return map.get(value);
    }

    public int getValue() {
        return value;
    }

    public byte getByteValue() {
        return (byte) value;
    }

    /*
     * CHECK_AND_PUT -> checkAndPut
     * PUT -> put
     */
    public String toCamelCase() {
        String name = this.name();
        if (name == null || name.isEmpty()) {
            return name;
        }

        String[] parts = name.split("_");
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < parts.length; i++) {
            String part = parts[i];
            if (part == null || part.isEmpty()) {
                continue;
            }

            if (i == 0) {
                sb.append(part.toLowerCase());
            } else {
                if (!part.isEmpty()) {
                    sb.append(Character.toUpperCase(part.charAt(0)));
                    if (part.length() > 1) {
                        sb.append(part.substring(1).toLowerCase());
                    }
                }
            }
        }
        return sb.toString();
    }
}

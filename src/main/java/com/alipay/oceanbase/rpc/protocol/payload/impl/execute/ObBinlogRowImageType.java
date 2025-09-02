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

import java.util.HashMap;
import java.util.Map;

public enum ObBinlogRowImageType {
    MINIMAL(0), NOBLOB(1), FULL(2);
    private int                                       value;
    private static Map<Integer, ObBinlogRowImageType> map = new HashMap<Integer, ObBinlogRowImageType>();

    ObBinlogRowImageType(int value) {
        this.value = value;
    }

    static {
        for (ObBinlogRowImageType type : ObBinlogRowImageType.values()) {
            map.put(type.value, type);
        }
    }

    /*
     * Value of.
     */
    public static ObBinlogRowImageType valueOf(int value) {
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

/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation;

public enum ObTableAggregationType {
    INVAILD(0), MAX(1), MIN(2), COUNT(3), SUM(4), AVG(5);

    private int value;

    /*
     * Get agg type from byte
     */
    public static ObTableAggregationType fromByte(byte i) {
        switch (i) {
            case 0:
                return INVAILD;
            case 1:
                return MAX;
            case 2:
                return MIN;
            case 3:
                return COUNT;
            case 4:
                return SUM;
            case 5:
                return AVG;
            default:
                throw new IllegalArgumentException("Invalid value: " + i);
        }
    }

    /*
     * For serialize.
     */
    public byte getByteValue() {
        return (byte) value;
    }

    ObTableAggregationType(int value) {
        this.value = value;
    }
};

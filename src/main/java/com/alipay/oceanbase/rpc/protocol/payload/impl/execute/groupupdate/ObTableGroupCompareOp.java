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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.groupupdate;

import java.util.HashMap;
import java.util.Map;

/**
 * @author xuanchao.xc
 * @since  2022-12-28
 */
public enum ObTableGroupCompareOp {
    LT(0), LE(1), GT(2), GE(3), EQ(4), NE(5), INVALID(6);

    private final int                                        value;
    private static final Map<Integer, ObTableGroupCompareOp> map = new HashMap<>();

    ObTableGroupCompareOp(int value) {
        this.value = value;
    }

    static {
        for (ObTableGroupCompareOp op : ObTableGroupCompareOp.values()) {
            map.put(op.value, op);
        }
    }

    /**
     * Value of.
     */
    public static ObTableGroupCompareOp valueOf(int value) {
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

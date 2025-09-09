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
public enum ObTableGroupUpdatePhase {
    CREATE_SESSION(0), GET_RESULT(1), INVALID(2);

    private final int                                          value;
    private static final Map<Integer, ObTableGroupUpdatePhase> map = new HashMap<>();

    ObTableGroupUpdatePhase(int value) {
        this.value = value;
    }

    static {
        for (ObTableGroupUpdatePhase phase : ObTableGroupUpdatePhase.values()) {
            map.put(phase.value, phase);
        }
    }

    /**
     * Value of.
     */
    public static ObTableGroupUpdatePhase valueOf(int value) {
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

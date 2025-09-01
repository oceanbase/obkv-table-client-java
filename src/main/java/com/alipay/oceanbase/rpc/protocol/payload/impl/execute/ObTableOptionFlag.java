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

public enum ObTableOptionFlag {
    DEFAULT(0),
    RETURNING_ROWKEY(1 << 0),
    USE_PUT(1 << 1),
    RETURN_ONE_RES(1 << 2),
    SERVER_CAN_RETRY(1 << 3);

    private int                                    value;
    private static Map<Integer, ObTableOptionFlag> map = new HashMap<Integer, ObTableOptionFlag>();

    ObTableOptionFlag(int value) {
        this.value = value;
    }

    static {
        for (ObTableOptionFlag type : ObTableOptionFlag.values()) {
            map.put(type.value, type);
        }
    }

    /*
     * Value of.
     */
    public static ObTableOptionFlag valueOf(int value) {
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

    /*
     * Set isReturningRowKey.
     */
    public void setReturningRowkey(boolean returningRowKey) {
        if (returningRowKey) {
            this.value |= RETURNING_ROWKEY.value;
        }
    }

    /*
     * Get isReturningRowKey.
     */
    public boolean isReturningRowKey() {
        return (this.value & RETURNING_ROWKEY.value) != 0;
    }

    /*
     * Get usePut.
     */
    public boolean isUsePut() {
        return (this.value & USE_PUT.value) != 0;
    }

    /*
     * Set usePut.
     */
    public void setUsePut(boolean usePut) {
        if (usePut) {
            this.value |= USE_PUT.value;
        }
    }

    public boolean isReturnOneResult() {
        return (this.value & RETURN_ONE_RES.value) != 0;
    }

    public void setReturnOneResult(boolean returnOneResult) {
        if (returnOneResult) {
            this.value |= RETURN_ONE_RES.value;
        } else {
            this.value &= ~(RETURN_ONE_RES.value);
        }
    }

    public boolean isServerCanRetry() {
        return (this.value & SERVER_CAN_RETRY.value) != 0;
    }

    public void setServerCanRetry(boolean serverCanRetry) {
        if (serverCanRetry) {
            this.value |= SERVER_CAN_RETRY.value;
        } else {
            this.value &= ~(SERVER_CAN_RETRY.value);
        }
    }
}

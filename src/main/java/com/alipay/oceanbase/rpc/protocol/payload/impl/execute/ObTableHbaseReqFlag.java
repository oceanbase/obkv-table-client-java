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

public class ObTableHbaseReqFlag {
    private static final int FLAG_SERVER_CAN_RETRY = 1 << 0;
    private long             flags                 = 0;

    public long getValue() {
        return flags;
    }

    public void setValue(long flags) {
        this.flags = flags;
    }

    public void setFlagServerCanRetry(boolean serverCanRetry) {
        if (serverCanRetry) {
            flags |= FLAG_SERVER_CAN_RETRY;
        } else {
            flags &= ~FLAG_SERVER_CAN_RETRY;
        }
    }

    public boolean getFlagServerCanRetry() {
        return (this.flags & FLAG_SERVER_CAN_RETRY) != 0;
    }
}

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

public class ObTableSingleOpFlag {
    private static final int FLAG_IS_CHECK_NOT_EXISTS = 1 << 0;
    private long             flags                    = 0;

    public void setIsCheckNotExists(boolean isCheckNotExists) {
        if (isCheckNotExists) {
            flags |= FLAG_IS_CHECK_NOT_EXISTS;
        } else {
            flags &= ~FLAG_IS_CHECK_NOT_EXISTS;
        }
    }

    public long getValue() {
        return flags;
    }

    public boolean isCheckNotExists() {
        return (flags & FLAG_IS_CHECK_NOT_EXISTS) != 0;
    }

    void setValue(long value) {
        flags = value;
    }
}

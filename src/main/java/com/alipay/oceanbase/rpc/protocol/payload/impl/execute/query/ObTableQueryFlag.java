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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query;

public class ObTableQueryFlag {
    private static final int HOT_ONLY = 1 << 0;

    private long             value    = 0;

    public ObTableQueryFlag(long value) {
        this.value = value;
    }

    public long getValue() {
        return value;
    }

    public void setValue(long value) {
        this.value = value;
    }

    public boolean isHotOnly() {
        return (value & HOT_ONLY) != 0;
    }

    public void setHotOnly(boolean hotOnly) {
        if (hotOnly) {
            value = value | HOT_ONLY;
        }
    }

    @Override
    public String toString() {
        return "ObTableQueryFlag{" +
                "value=" + value +
                ", isHotOnly=" + isHotOnly() +
                '}';
    }

}

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

package com.alipay.oceanbase.rpc.table;

public class ObTableServerCapacity {
    private static final int DISTRIBUTED_EXECUTE = 1 << 0;
    private static final int SECONDARY_PARTITION = 1 << 1;
    private static final int TIME_SERIES         = 1 << 2;
    private static final int CAPACITY_MAX        = 1 << 31;
    private              int flags               = 0;

    public int getFlags() { return flags; }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public boolean isSupportDistributedExecute() {
        return (flags & DISTRIBUTED_EXECUTE) != 0;
    }

    public boolean isSupportSecondaryPartition() {
        return (flags & SECONDARY_PARTITION) != 0;
    }

    public boolean isSupportTimeSeries() {
        return (flags & TIME_SERIES) != 0;
    }
}

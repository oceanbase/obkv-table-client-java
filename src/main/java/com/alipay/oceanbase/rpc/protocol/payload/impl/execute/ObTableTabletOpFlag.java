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

public class ObTableTabletOpFlag {
    private static final int FLAG_IS_SAME_TYPE             = 1 << 0;
    // Maybe useless, we use isSameProperties flag from LSOp
    private static final int FLAG_IS_SAME_PROPERTIES_NAMES = 1 << 1;
    private static final int FLAG_IS_READ_ONLY             = 1 << 2;
    private long             flags                         = 0;

    public void setFlagIsSameType(boolean isSameType) {
        if (isSameType) {
            flags |= FLAG_IS_SAME_TYPE;
        } else {
            flags &= ~FLAG_IS_SAME_TYPE;
        }
    }

    public void setFlagIsSamePropertiesNames(boolean isSamePropertiesNames) {
        if (isSamePropertiesNames) {
            flags |= FLAG_IS_SAME_PROPERTIES_NAMES;
        } else {
            flags &= ~FLAG_IS_SAME_PROPERTIES_NAMES;
        }
    }

    public void setFlagIsReadOnly(boolean isReadOnly) {
        if (isReadOnly) {
            flags |= FLAG_IS_READ_ONLY;
        } else {
            flags &= ~FLAG_IS_READ_ONLY;
        }
    }

    public long getValue() {
        return flags;
    }

    public void setValue(long flags) {
        this.flags = flags;
    }

    public boolean getFlagIsSameType() {
        return (flags & FLAG_IS_SAME_TYPE) != 0;
    }

    public boolean getFlagIsSamePropertiesNames() {
        return (flags & FLAG_IS_SAME_PROPERTIES_NAMES) != 0;
    }

    public boolean getFlagIsReadOnly() {
        return (flags & FLAG_IS_READ_ONLY) != 0;
    }
}

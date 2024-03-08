/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2024 OceanBase
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

public class ObTableLSOpFlag {
    private static final int FLAG_IS_SAME_TYPE             = 1 << 0;
    private static final int FLAG_IS_SAME_PROPERTIES_NAMES = 1 << 1;
    private static final int FLAG_RETURN_ONE_RESULT        = 1 << 2;
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

    public void setReturnOneResult(boolean returnOneResult) {
        if (returnOneResult) {
            flags |= FLAG_RETURN_ONE_RESULT;
        } else {
            flags &= ~FLAG_RETURN_ONE_RESULT;
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
}

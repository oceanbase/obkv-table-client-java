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

package com.alipay.oceanbase.rpc.location.model.partition;

import org.apache.commons.lang.builder.ToStringBuilder;

public class ObComparableKV<A extends Comparable<A>, B> implements Comparable<ObComparableKV<A, B>> {
    public final A key;
    public final B value;

    /*
     * Ob comparable k v.
     */
    public ObComparableKV(A key, B value) {
        this.key = key;
        this.value = value;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("key", key).append("value", value).toString();
    }

    /*
     * Compare to.
     */
    @Override
    public int compareTo(ObComparableKV<A, B> o) {
        return this.key.compareTo(o.key);
    }
}

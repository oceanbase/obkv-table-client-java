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

public enum ObFetchPartitionMetaType {
    GET_PARTITION_META("GET_PARTITION_META", 0), INVALID("INVALID", 1);

    private final String name;
    private final int    index;

    ObFetchPartitionMetaType(String name, int index) {
        this.name = name;
        this.index = index;
    }

    /*
     * Value of.
     */
    public static ObFetchPartitionMetaType valueOf(int index) {
        if (GET_PARTITION_META.index == index) {
            return GET_PARTITION_META;
        } else {
            return INVALID;
        }
    }

    /*
     * Get index.
     */
    public int getIndex() {
        return index;
    }

    /*
     * Get name.
     */
    public String getName() {
        return name;
    }

}

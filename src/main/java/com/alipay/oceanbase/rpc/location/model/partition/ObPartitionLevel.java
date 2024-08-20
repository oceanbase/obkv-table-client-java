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

public enum ObPartitionLevel {

    LEVEL_ZERO("LEVEL_ZERO", 0), LEVEL_ONE("LEVEL_ONE", 1), LEVEL_TWO("LEVEL_TWO", 2), UNKNOWN(
                                                                                               "UNKNOWN",
                                                                                               3);

    private final String name;
    private final long   index;

    ObPartitionLevel(String name, long index) {
        this.name = name;
        this.index = index;
    }

    /*
     * Value of.
     */
    public static ObPartitionLevel valueOf(long index) {

        if (LEVEL_ZERO.index == index) {
            return LEVEL_ZERO;
        } else if (LEVEL_ONE.index == index) {
            return LEVEL_ONE;
        } else if (LEVEL_TWO.index == index) {
            return LEVEL_TWO;
        } else {
            return UNKNOWN;
        }
    }

    /*
     * Get index.
     */
    public long getIndex() {
        return index;
    }

    /*
     * Get name.
     */
    public String getName() {
        return name;
    }

}

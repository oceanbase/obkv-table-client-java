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

package com.alipay.oceanbase.rpc.dds.util;

public class ElasticWeightConfig {
    private final int     index;
    private final String  dbkey;
    private final int     readWeight;
    private final int     writeWeight;
    private final boolean isWriteWeight;
    private final long    deprecatedTimestamp;

    public ElasticWeightConfig(int index, String dbkey, int readWeight, int writeWeight,
                               boolean isWriteWeight, long deprecatedTimestamp) {
        this.index = index;
        this.dbkey = dbkey;
        this.readWeight = readWeight;
        this.writeWeight = writeWeight;
        this.isWriteWeight = isWriteWeight;
        this.deprecatedTimestamp = deprecatedTimestamp;
    }

    public int getIndex() {
        return index;
    }

    public String getDbkey() {
        return dbkey;
    }

    public int getReadWeight() {
        return readWeight;
    }

    public int getWriteWeight() {
        return writeWeight;
    }

    public boolean isWriteWeight() {
        return isWriteWeight;
    }

    public long getDeprecatedTimestamp() {
        return deprecatedTimestamp;
    }
}

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

package com.alipay.oceanbase.rpc.direct_load;

public class ObDirectLoadTraceId {

    private final long uniqueId;
    private final long sequence;

    public ObDirectLoadTraceId(long uniqueId, long sequence) {
        this.uniqueId = uniqueId;
        this.sequence = sequence;
    }

    public String toString() {
        return String.format("Y%X-%016X", uniqueId, sequence);
    }

    public long getUniqueId() {
        return uniqueId;
    }

    public long getSequence() {
        return sequence;
    }

    public static final ObDirectLoadTraceId DEFAULT_TRACE_ID;

    static {
        DEFAULT_TRACE_ID = new ObDirectLoadTraceId(0, 0);
    }

}

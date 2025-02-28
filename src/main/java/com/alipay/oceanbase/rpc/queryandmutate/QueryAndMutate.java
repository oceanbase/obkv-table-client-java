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

package com.alipay.oceanbase.rpc.queryandmutate;

import com.alipay.oceanbase.rpc.mutation.Mutation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;

public class QueryAndMutate {
    private ObTableQuery query;
    private Mutation     mutation;

    public QueryAndMutate(ObTableQuery query, Mutation mutation) {
        this.query = query;
        this.mutation = mutation;
    }

    public ObTableQuery getQuery() {
        return query;
    }

    public Mutation getMutation() {
        return mutation;
    }
}

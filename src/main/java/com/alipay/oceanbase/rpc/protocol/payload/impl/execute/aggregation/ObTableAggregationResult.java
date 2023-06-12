/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation;

import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObTableAggregationResult {

    private Map<String, Object>   row = new HashMap<>();
    private final QueryResultSet  queryResultSet;

    public ObTableAggregationResult(QueryResultSet queryResultSet) throws Exception {
        this.queryResultSet = queryResultSet;
        this.init();
    }

    /*
     * Init for aggregation result.
     */
    public void init() throws Exception {
        if (this.queryResultSet.next()) {
            row = this.queryResultSet.getRow();
        }
    }

    /*
     * Get an aggregation result from the column name.
     */
    public Object get(String columName) throws Exception {
        return row.get(columName);
    }

    /*
     * Get the aggregation row.
     */
    public Row getRow() throws Exception {
        return new Row(this.row);
    }
}

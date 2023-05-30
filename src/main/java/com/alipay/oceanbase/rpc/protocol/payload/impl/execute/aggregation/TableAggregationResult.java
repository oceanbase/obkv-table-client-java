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

public class TableAggregationResult {

    private Map<Integer, String> index;
    private final QueryResultSet queryResultSet;
    private Map<String, Object> row = new HashMap<>();

    public TableAggregationResult(QueryResultSet queryResultSet, Map<Integer, String> index) throws Exception {
        this.queryResultSet = queryResultSet;
        this.index = index;
        this.init();
    }

    public void init() throws Exception {
        this.queryResultSet.next();
        List<ObObj> init_row;
        init_row = this.queryResultSet.get_Row();
        for (int i = 0; i < init_row.size(); i++) {
            row.put(index.get(i), init_row.get(i).getValue());
        }
    }

    public Object get(String columName) throws Exception {
        return row.get(columName);
    }

    public Row getRow() throws Exception {
        return new Row(this.row);
    }
}

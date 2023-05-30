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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation;

import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableAggregationResult {
    public TableAggregationResult(QueryResultSet queryResultSet_, Map<Integer, String> index_) {
        this.queryResultSet = queryResultSet_;
        this.index = index_;
        this.isInit = false;
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
        if (!this.isInit) {
            this.init();
            this.changeInit();
        }
        return row.get(columName);
    }

    public Row getRow() throws Exception {
        if (!this.isInit) {
            this.init();
            this.changeInit();
        }
        return new Row(this.row);
    }

    public void changeInit() {
        this.isInit = true;
    }
    private final QueryResultSet queryResultSet;
    private Map<String, Object> row = new HashMap<>();
    private Map<Integer, String> index;

    private boolean isInit = false;
}

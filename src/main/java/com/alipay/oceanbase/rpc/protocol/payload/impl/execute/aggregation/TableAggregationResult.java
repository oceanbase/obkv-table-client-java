package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation;

import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;

import java.util.ArrayList;
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

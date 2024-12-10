package com.alipay.oceanbase.rpc.get;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.mutation.ColumnValue;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.table.api.Table;

import java.util.Map;

public class Get {
    private Table           client;
    private String          tableName;
    protected Row           rowKey;
    protected String[]  selectColumns;

    public Get() {
        tableName = null;
        client = null;
        rowKey = null;
        selectColumns = null;
    }

    public Get(Table client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    public Get setRowKey(Row rowKey) {
        this.rowKey = rowKey;
        if (null != tableName && ((ObTableClient) client).getRowKeyElement(tableName) == null) {
            ((ObTableClient) client).addRowKeyElement(tableName, this.rowKey.getColumns());
        }
        return this;
    }

    public Get setRowKey(ColumnValue... rowKey) {
        this.rowKey = new Row(rowKey);
        if (null != tableName && ((ObTableClient) client).getRowKeyElement(tableName) == null) {
            ((ObTableClient) client).addRowKeyElement(tableName, this.rowKey.getColumns());
        }
        return this;
    }

    public Row getRowKey() {
        return rowKey;
    }

    public Get select(String... columns) {
        this.selectColumns = columns;
        return this;
    }

    public String[] getSelectColumns() {
        return selectColumns;
    }

    public Map<String, Object> execute() throws Exception {
        if (client == null) {
            throw new IllegalArgumentException("client is null");
        }
        return ((ObTableClient)client).get(tableName, rowKey, selectColumns);
    }
}

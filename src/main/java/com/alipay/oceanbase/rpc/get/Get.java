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

package com.alipay.oceanbase.rpc.get;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.mutation.ColumnValue;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObReadConsistency;
import com.alipay.oceanbase.rpc.table.api.Table;

import java.util.Map;

public class Get {
    private Table             client          = null;
    private String            tableName       = null;
    private Row               rowKey          = null;
    private String[]          selectColumns   = null;
    private ObReadConsistency readConsistency = null;

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

    public Get setReadConsistency(ObReadConsistency readConsistency) {
        this.readConsistency = readConsistency;
        return this;
    }

    public ObReadConsistency getReadConsistency() {
        return readConsistency;
    }

    public String[] getSelectColumns() {
        return selectColumns;
    }

    public Map<String, Object> execute() throws Exception {
        if (client == null) {
            throw new IllegalArgumentException("client is null");
        }
        return ((ObTableClient) client).get(tableName, rowKey, selectColumns, readConsistency);
    }
}

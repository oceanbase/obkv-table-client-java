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

import com.alipay.oceanbase.rpc.ObClusterTableQuery;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;

import java.util.HashMap;
import java.util.Map;

public class TableAggregation {

    private ObClusterTableQuery tablequery;
    private Map<Integer, String> message = new HashMap<>();
    private Integer index                = 0;

    public TableAggregation(ObClusterTableQuery obClusterTableQuery) {
        this.tablequery = obClusterTableQuery;
    }

    public void min(String columnName) {
        this.tablequery.AddAggregation(AggregationType.MIN, columnName);
        message.put(getIndex(), "min(" + columnName + ")");
        updateIndex();
    }

    public void min(String columnName, String aliasName) {
        this.tablequery.AddAggregation(AggregationType.MIN, columnName);
        message.put(getIndex(), "min(" + aliasName + ")");
        updateIndex();
    }

    public void max(String columnName) {
        this.tablequery.AddAggregation(AggregationType.MAX, columnName);
        message.put(getIndex(), "max(" + columnName + ")");
        updateIndex();
    }

    public void max(String columnName, String aliasName) {
        this.tablequery.AddAggregation(AggregationType.MAX, columnName);
        message.put(getIndex(), "max(" + aliasName + ")");
        updateIndex();
    }

    public void count() {
        this.tablequery.AddAggregation(AggregationType.COUNT, "*");
        message.put(getIndex(), "count(*)");
        updateIndex();
    }
    public void count(String aliasName) {
        this.tablequery.AddAggregation(AggregationType.COUNT, "*");
        message.put(getIndex(), "count(" + aliasName + ")");
        updateIndex();
    }

    public void sum(String columnName) {
        this.tablequery.AddAggregation(AggregationType.SUM, columnName);
        message.put(getIndex(), "sum(" + columnName + ")");
        updateIndex();
    }

    public void Sum(String columnName, String aliasName) {
        this.tablequery.AddAggregation(AggregationType.SUM, columnName);
        message.put(getIndex(), "sum(" + aliasName + ")");
        updateIndex();
    }

    public void avg(String columnName) {
        this.tablequery.AddAggregation(AggregationType.AVG, columnName);
        message.put(getIndex(), "avg(" + columnName + ")");
        updateIndex();
    }

    public void avg(String columnName, String aliasName) {
        this.tablequery.AddAggregation(AggregationType.AVG, columnName);
        message.put(getIndex(), "avg(" + aliasName + ")");
        updateIndex();
    }

    public void setScanRangeColumns(String... columns) {
        this.tablequery.setScanRangeColumns(columns);
    }

    public void addScanRange(Object start, Object end) {
        this.tablequery.addScanRange(start, end);
    }

    public void addScanRange(Object[] start, Object[] end) {
        this.tablequery.addScanRange(start, end);
    }

    public void indexName(String indexName) {
        this.tablequery.indexName(indexName);
    }

    public void setFilter(ObTableFilter filter) {
        this.tablequery.setFilter(filter);
    }

    public void setOperationTimeout(long operationTimeout) {
        this.tablequery.setOperationTimeout(operationTimeout);
    }

    public TableAggregationResult execute() throws Exception {
        if (this.getIndex() == 0) {
            throw new Exception(
                    "please add aggregations"
            );
        }
        this.tablequery.select(new String[getIndex()]);
        return new TableAggregationResult(this.tablequery.execute(), this.message);
    }

    public Integer getIndex() {
        return this.index;
    }

    public void updateIndex() {
        this.index++;
    }
}

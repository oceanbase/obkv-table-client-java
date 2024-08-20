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

import java.util.ArrayList;
import java.util.List;

public class ObTableAggregation {

    //this message is used to record the aggregation order and the corresponding aggregation name
    private List<String> message = new ArrayList<>();
    private ObClusterTableQuery tablequery;

    public ObTableAggregation(ObClusterTableQuery obClusterTableQuery) {
        this.tablequery = obClusterTableQuery;
    }

    public void min(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.MIN, columnName);
        message.add("min(" + columnName + ")");
    }

    public void min(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.MIN, columnName);
        message.add("min(" + aliasName + ")");
    }

    public void max(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.MAX, columnName);
        message.add("max(" + columnName + ")");
    }

    public void max(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.MAX, columnName);
        message.add("max(" + aliasName + ")");
    }

    public void count() {
        this.tablequery.addAggregation(ObTableAggregationType.COUNT, "*");
        message.add("count(*)");
    }
    public void count(String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.COUNT, "*");
        message.add("count(" + aliasName + ")");
    }

    public void sum(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.SUM, columnName);
        message.add("sum(" + columnName + ")");
    }

    public void sum(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.SUM, columnName);
        message.add("sum(" + aliasName + ")");
    }

    public void avg(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.AVG, columnName);
        message.add("avg(" + columnName + ")");
    }

    public void avg(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.AVG, columnName);
        message.add("avg(" + aliasName + ")");
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

    public void limit(int offset, int limit) { this.tablequery.limit(offset, limit); }

    public void setOperationTimeout(long operationTimeout) {
        this.tablequery.setOperationTimeout(operationTimeout);
    }

    public ObTableAggregationResult execute() throws Exception {
        if (this.message.size() == 0) {
            throw new IllegalArgumentException("please add aggregations.");
        }
        // In order to get cache size.
        this.tablequery.select(message.toArray(new String[message.size()]));
        return new ObTableAggregationResult(this.tablequery.execute());
    }
}

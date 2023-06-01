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

public class ObTableAggregation {

    //this message is used to record the aggregation order and the corresponding aggregation name
    private Map<Integer, String> message = new HashMap<>();
    private ObClusterTableQuery tablequery;

    public ObTableAggregation(ObClusterTableQuery obClusterTableQuery) {
        this.tablequery = obClusterTableQuery;
    }

    public void min(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.MIN, columnName);
        message.put(this.message.size(), "min(" + columnName + ")");
    }

    public void min(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.MIN, columnName);
        message.put(this.message.size(), "min(" + aliasName + ")");
    }

    public void max(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.MAX, columnName);
        message.put(this.message.size(), "max(" + columnName + ")");
    }

    public void max(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.MAX, columnName);
        message.put(this.message.size(), "max(" + aliasName + ")");
    }

    public void count() {
        this.tablequery.addAggregation(ObTableAggregationType.COUNT, "*");
        message.put(this.message.size(), "count(*)");
    }
    public void count(String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.COUNT, "*");
        message.put(this.message.size(), "count(" + aliasName + ")");
    }

    public void sum(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.SUM, columnName);
        message.put(this.message.size(), "sum(" + columnName + ")");
    }

    public void Sum(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.SUM, columnName);
        message.put(this.message.size(), "sum(" + aliasName + ")");
    }

    public void avg(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.AVG, columnName);
        message.put(this.message.size(), "avg(" + columnName + ")");
    }

    public void avg(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.AVG, columnName);
        message.put(this.message.size(), "avg(" + aliasName + ")");
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

    public ObTableAggregationResult execute() throws Exception {
        if (this.message.size() == 0) {
            throw new Exception(
                    "please add aggregations"
            );
        }
        // In order to get cache size.
        this.tablequery.select(new String[this.message.size()]);
        return new ObTableAggregationResult(this.tablequery.execute(), this.message);
    }
}

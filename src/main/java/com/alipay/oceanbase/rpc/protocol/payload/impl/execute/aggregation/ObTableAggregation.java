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

    // this message is used to record the aggregation order and the corresponding aggregation name
    private Map<Integer, String> message = new HashMap<>();
    // this index is used to record the aggregation order
    private Integer index                = 0;               
    private ObClusterTableQuery tablequery;

    public ObTableAggregation(ObClusterTableQuery obClusterTableQuery) {
        this.tablequery = obClusterTableQuery;
    }

    public void min(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.MIN, columnName);
        message.put(getIndex(), "min(" + columnName + ")");
        updateIndex();
    }

    public void min(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.MIN, columnName);
        message.put(getIndex(), "min(" + aliasName + ")");
        updateIndex();
    }

    public void max(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.MAX, columnName);
        message.put(getIndex(), "max(" + columnName + ")");
        updateIndex();
    }

    public void max(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.MAX, columnName);
        message.put(getIndex(), "max(" + aliasName + ")");
        updateIndex();
    }

    public void count() {
        this.tablequery.addAggregation(ObTableAggregationType.COUNT, "*");
        message.put(getIndex(), "count(*)");
        updateIndex();
    }
    public void count(String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.COUNT, "*");
        message.put(getIndex(), "count(" + aliasName + ")");
        updateIndex();
    }

    public void sum(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.SUM, columnName);
        message.put(getIndex(), "sum(" + columnName + ")");
        updateIndex();
    }

    public void Sum(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.SUM, columnName);
        message.put(getIndex(), "sum(" + aliasName + ")");
        updateIndex();
    }

    public void avg(String columnName) {
        this.tablequery.addAggregation(ObTableAggregationType.AVG, columnName);
        message.put(getIndex(), "avg(" + columnName + ")");
        updateIndex();
    }

    public void avg(String columnName, String aliasName) {
        this.tablequery.addAggregation(ObTableAggregationType.AVG, columnName);
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

    public ObTableAggregationResult execute() throws Exception {
        if (this.getIndex() == 0) {
            throw new Exception(
                    "please add aggregations"
            );
        }
        this.tablequery.select(new String[getIndex()]);
        return new ObTableAggregationResult(this.tablequery.execute(), this.message);
    }

    /*
     * Get the index.
     */
    public Integer getIndex() {
        return this.index;
    }

    /*
     * Update the index.
     */
    public void updateIndex() {
        this.index++;
    }
 }
 
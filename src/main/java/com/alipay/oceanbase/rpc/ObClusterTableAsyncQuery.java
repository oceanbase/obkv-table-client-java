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

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.stream.async.ObTableClientQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHTableFilter;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.AbstractTableQuery;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableClientQueryAsyncImpl;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

public class ObClusterTableAsyncQuery extends AbstractTableQuery {
    private final ObTableClientQueryAsyncImpl tableClientQueryAsync;

    ObClusterTableAsyncQuery(ObTableClientQueryAsyncImpl tableClientQueryAsync) {
        this.tableClientQueryAsync = tableClientQueryAsync;
    }

    @Override
    public ObTableQuery getObTableQuery() {
        return tableClientQueryAsync.getObTableQuery();
    }

    @Override
    public String getTableName() {
        return tableClientQueryAsync.getTableName();
    }

    @Override
    public QueryResultSet execute() throws Exception {
        return tableClientQueryAsync.execute();
    }

    @Override
    public QueryResultSet executeInit(ObPair<Long, ObTableParam> entry) throws Exception {
        return tableClientQueryAsync.executeInit(entry);
    }

    @Override
    public QueryResultSet executeNext(ObPair<Long, ObTableParam> entry) throws Exception {
        return tableClientQueryAsync.executeNext(entry);
    }

    ObTableClientQueryAsyncStreamResult executeInternal(ObQueryOperationType type) throws Exception {
        return tableClientQueryAsync.executeInternal(type);
    }

    @Override
    public TableQuery select(String... columns) {
        tableClientQueryAsync.select(columns);
        return this;
    }

    @Override
    public TableQuery setKeys(String... keys) {
        throw new IllegalArgumentException("Not needed");
    }

    @Override
    public TableQuery limit(int offset, int limit) {
        tableClientQueryAsync.limit(offset, limit);
        return this;
    }

    @Override
    public TableQuery addScanRange(Object[] start, boolean startEquals, Object[] end,
                                   boolean endEquals) {
        tableClientQueryAsync.addScanRange(start, startEquals, end, endEquals);
        return this;
    }

    @Override
    public TableQuery addScanRangeStartsWith(Object[] start, boolean startEquals) {
        tableClientQueryAsync.addScanRangeStartsWith(start, startEquals);
        return this;
    }

    @Override
    public TableQuery addScanRangeEndsWith(Object[] end, boolean endEquals) {
        tableClientQueryAsync.addScanRangeStartsWith(end, endEquals);
        return this;
    }

    @Override
    public TableQuery scanOrder(boolean forward) {
        tableClientQueryAsync.scanOrder(forward);
        return this;
    }

    @Override
    public TableQuery indexName(String indexName) {
        tableClientQueryAsync.indexName(indexName);
        return this;
    }

    @Override
    public TableQuery filterString(String filterString) {
        tableClientQueryAsync.filterString(filterString);
        return this;
    }

    @Override
    public TableQuery setHTableFilter(ObHTableFilter obHTableFilter) {
        return tableClientQueryAsync.setHTableFilter(obHTableFilter);
    }

    @Override
    public TableQuery setBatchSize(int batchSize) {
        return tableClientQueryAsync.setBatchSize(batchSize);
    }

    @Override
    public TableQuery setMaxResultSize(long maxResultSize) {
        return tableClientQueryAsync.setMaxResultSize(maxResultSize);
    }

    @Override
    public void clear() {
        tableClientQueryAsync.clear();
    }

    public void setEntityType(ObTableEntityType entityType) {
        super.setEntityType(entityType);
        tableClientQueryAsync.setEntityType(entityType);
    }
}

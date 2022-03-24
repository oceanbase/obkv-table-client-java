/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2022 Ant Financial Services Group
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
import com.alipay.oceanbase.rpc.table.api.TableQuery;

public class ObClusterTableAsyncQuery extends AbstractTableQuery {
    private final ObTableClientQueryAsyncImpl tableClientQuerySync;

    ObClusterTableAsyncQuery(ObTableClientQueryAsyncImpl tableClientQuerySync) {
        this.tableClientQuerySync = tableClientQuerySync;
    }

    @Override
    public ObTableQuery getObTableQuery() {
        return tableClientQuerySync.getObTableQuery();
    }

    @Override
    public String getTableName() {
        return tableClientQuerySync.getTableName();
    }

    @Override
    public QueryResultSet execute() throws Exception {
        return tableClientQuerySync.execute();
    }

    @Override
    public QueryResultSet executeInit(ObPair<Long, ObTable> entry) throws Exception {
        return tableClientQuerySync.executeInit(entry);
    }

    @Override
    public QueryResultSet executeNext(ObPair<Long, ObTable> entry) throws Exception {
        return tableClientQuerySync.executeNext(entry);
    }

    ObTableClientQueryAsyncStreamResult executeInternal(ObQueryOperationType type) throws Exception {
        return tableClientQuerySync.executeInternal(type);
    }

    @Override
    public TableQuery select(String... columns) {
        tableClientQuerySync.select(columns);
        return this;
    }

    @Override
    public TableQuery setKeys(String... keys) {
        throw new IllegalArgumentException("Not needed");
    }

    @Override
    public TableQuery limit(int offset, int limit) {
        tableClientQuerySync.limit(offset, limit);
        return this;
    }

    @Override
    public TableQuery addScanRange(Object[] start, boolean startEquals, Object[] end,
                                   boolean endEquals) {
        tableClientQuerySync.addScanRange(start, startEquals, end, endEquals);
        return this;
    }

    @Override
    public TableQuery addScanRangeStartsWith(Object[] start, boolean startEquals) {
        tableClientQuerySync.addScanRangeStartsWith(start, startEquals);
        return this;
    }

    @Override
    public TableQuery addScanRangeEndsWith(Object[] end, boolean endEquals) {
        tableClientQuerySync.addScanRangeStartsWith(end, endEquals);
        return this;
    }

    @Override
    public TableQuery scanOrder(boolean forward) {
        tableClientQuerySync.scanOrder(forward);
        return this;
    }

    @Override
    public TableQuery indexName(String indexName) {
        tableClientQuerySync.indexName(indexName);
        return this;
    }

    @Override
    public TableQuery filterString(String filterString) {
        tableClientQuerySync.filterString(filterString);
        return this;
    }

    @Override
    public TableQuery setHTableFilter(ObHTableFilter obHTableFilter) {
        return tableClientQuerySync.setHTableFilter(obHTableFilter);
    }

    @Override
    public TableQuery setBatchSize(int batchSize) {
        return tableClientQuerySync.setBatchSize(batchSize);
    }

    @Override
    public TableQuery setMaxResultSize(long maxResultSize) {
        return tableClientQuerySync.setMaxResultSize(maxResultSize);
    }

    @Override
    public void clear() {
        tableClientQuerySync.clear();
    }

    public void setEntityType(ObTableEntityType entityType) {
        super.setEntityType(entityType);
        tableClientQuerySync.setEntityType(entityType);
    }
}

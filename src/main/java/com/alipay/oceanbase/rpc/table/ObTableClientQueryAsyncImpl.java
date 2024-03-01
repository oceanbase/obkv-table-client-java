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

package com.alipay.oceanbase.rpc.table;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.location.model.ObIndexInfo;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.stream.async.ObTableClientQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.stream.async.ObTableQueryAsyncClientResultSet;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObBorderFlag;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObTableClientQueryAsyncImpl extends AbstractTableQueryImpl {
    private final String                          tableName;
    private final ObTableClient                   obTableClient;
    private long                                  sessionId;
    private Map<Long, ObPair<Long, ObTableParam>> partitionObTables;
    private boolean                               hasMore;

    public ObTableClientQueryAsyncImpl(String tableName, ObTableClient client) {
        this.tableName = tableName;
        this.indexTableName = tableName;
        this.obTableClient = client;
        this.tableQuery = new ObTableQuery();
    }

    public ObTableClientQueryAsyncImpl(String tableName, ObTableQuery tableQuery,
                                       ObTableClient client) {
        this.tableName = tableName;
        this.indexTableName = tableName;
        this.obTableClient = client;
        this.tableQuery = tableQuery;
    }

    @Override
    public ObTableQuery getObTableQuery() {
        return tableQuery;
    }

    public TableQuery getTableQuery() {
        return this;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public QueryResultSet execute() throws Exception {
        return new QueryResultSet(new ObTableQueryAsyncClientResultSet(this));
    }

    public QueryResultSet execute(ObQueryOperationType type, ObPair<Long, ObTableParam> entry)
                                                                                              throws Exception {
        ObTableClientQueryAsyncStreamResult obTableClientQueryAsyncStreamResult = executeInternal(
            type, entry);
        QueryResultSet queryResultSet = new QueryResultSet(obTableClientQueryAsyncStreamResult);
        queryResultSet.setHasMore(obTableClientQueryAsyncStreamResult.hasMore());
        queryResultSet.setSessionId(obTableClientQueryAsyncStreamResult.getSessionId());
        return queryResultSet;
    }

    @Override
    public QueryResultSet executeInit(ObPair<Long, ObTableParam> entry) throws Exception {
        return execute(ObQueryOperationType.QUERY_START, entry);
    }

    @Override
    public QueryResultSet executeNext(ObPair<Long, ObTableParam> entry) throws Exception {
        return execute(ObQueryOperationType.QUERY_NEXT, entry);
    }

    @Override
    public TableQuery setKeys(String... keys) {
        throw new IllegalArgumentException("Not needed");
    }

    @Override
    public void clear() {

    }

    public ObTableClientQueryAsyncStreamResult executeInternal(ObQueryOperationType type)
                                                                                         throws Exception {
        Map<Long, ObPair<Long, ObTableParam>> partitionObTables = getPartitions();
        ObTableClientQueryAsyncStreamResult obTableClientQueryASyncStreamResult = new ObTableClientQueryAsyncStreamResult();
        obTableClientQueryASyncStreamResult.setTableQuery(tableQuery);
        obTableClientQueryASyncStreamResult.setEntityType(entityType);
        obTableClientQueryASyncStreamResult.setTableName(tableName);
        obTableClientQueryASyncStreamResult.setIndexTableName(indexTableName);
        obTableClientQueryASyncStreamResult.setExpectant(partitionObTables);
        obTableClientQueryASyncStreamResult.setOperationTimeout(operationTimeout);
        obTableClientQueryASyncStreamResult.setClient(obTableClient);
        obTableClientQueryASyncStreamResult.init(type, sessionId);

        QueryResultSet queryAsyncResultSet = new QueryResultSet(obTableClientQueryASyncStreamResult);
        this.hasMore = !obTableClientQueryASyncStreamResult.isEnd();
        queryAsyncResultSet.setHasMore(this.hasMore);
        obTableClientQueryASyncStreamResult.setHasMore(this.hasMore);
        queryAsyncResultSet.setSessionId(obTableClientQueryASyncStreamResult.getSessionId());
        this.sessionId = obTableClientQueryASyncStreamResult.getSessionId();

        return obTableClientQueryASyncStreamResult;
    }

    public ObTableClientQueryAsyncStreamResult executeInternal(ObQueryOperationType type,
                                                               ObPair<Long, ObTableParam> entry)
                                                                                                throws Exception {
        ObTableClientQueryAsyncStreamResult obTableClientQueryASyncStreamResult = new ObTableClientQueryAsyncStreamResult();
        obTableClientQueryASyncStreamResult.setTableQuery(tableQuery);
        obTableClientQueryASyncStreamResult.setEntityType(entityType);
        obTableClientQueryASyncStreamResult.setTableName(tableName);
        obTableClientQueryASyncStreamResult.setIndexTableName(indexTableName);
        obTableClientQueryASyncStreamResult.setExpectant(partitionObTables);
        obTableClientQueryASyncStreamResult.setOperationTimeout(operationTimeout);
        obTableClientQueryASyncStreamResult.setClient(obTableClient);
        obTableClientQueryASyncStreamResult.init(type, entry, sessionId);

        QueryResultSet queryAsyncResultSet = new QueryResultSet(obTableClientQueryASyncStreamResult);
        this.hasMore = !obTableClientQueryASyncStreamResult.isEnd();
        queryAsyncResultSet.setHasMore(this.hasMore);
        obTableClientQueryASyncStreamResult.setHasMore(this.hasMore);
        queryAsyncResultSet.setSessionId(obTableClientQueryASyncStreamResult.getSessionId());
        this.sessionId = obTableClientQueryASyncStreamResult.getSessionId();

        return obTableClientQueryASyncStreamResult;
    }

    public Map<Long, ObPair<Long, ObTableParam>> getPartitions() throws Exception {
        String indexName = tableQuery.getIndexName();
        if (!this.obTableClient.isOdpMode()) {
            indexTableName = obTableClient.getIndexTableName(tableName, indexName,
                tableQuery.getScanRangeColumns(), false);
        }

        this.partitionObTables = new HashMap<Long, ObPair<Long, ObTableParam>>();
        for (ObNewRange rang : this.tableQuery.getKeyRanges()) {
            ObRowKey startKey = rang.getStartKey();
            int startKeySize = startKey.getObjs().size();
            ObRowKey endKey = rang.getEndKey();
            int endKeySize = endKey.getObjs().size();
            Object[] start = new Object[startKeySize];
            Object[] end = new Object[endKeySize];
            for (int i = 0; i < startKeySize; i++) {
                start[i] = startKey.getObj(i).getValue();
            }

            for (int i = 0; i < endKeySize; i++) {
                end[i] = endKey.getObj(i).getValue();
            }
            ObBorderFlag borderFlag = rang.getBorderFlag();
            List<ObPair<Long, ObTableParam>> pairs = this.obTableClient.getTables(indexTableName,
                start, borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd(), false,
                false);
            for (ObPair<Long, ObTableParam> pair : pairs) {
                partitionObTables.put(pair.getLeft(), pair);
            }
        }
        return partitionObTables;
    }

    public long getSessionId() {
        return sessionId;
    }
}

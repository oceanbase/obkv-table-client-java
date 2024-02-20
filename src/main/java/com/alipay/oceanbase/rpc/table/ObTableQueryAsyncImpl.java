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

import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.stream.async.ObTableQueryAsyncResultSet;
import com.alipay.oceanbase.rpc.stream.async.ObTableQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncRequest;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.HashMap;
import java.util.Map;

public class ObTableQueryAsyncImpl extends AbstractTableQueryImpl {
    private String  tableName;
    private ObTable table;
    private long    sessionId;

    public ObTableQueryAsyncImpl(String tableName, ObTable table) {
        this.tableName = tableName;
        this.table = table;
        resetRequest();
    }

    private void resetRequest() {
        ObTableQueryAsyncRequest obTableQueryAsyncRequest = new ObTableQueryAsyncRequest();
        this.tableQuery = new ObTableQuery();
        ObTableQueryRequest obTableQueryRequest = new ObTableQueryRequest();

        obTableQueryRequest.setTableQuery(tableQuery);
        obTableQueryRequest.setTableName(tableName);
        obTableQueryRequest.setTableId(Constants.OB_INVALID_ID);
        obTableQueryRequest.setPartitionId(Constants.INVALID_TABLET_ID);

        obTableQueryAsyncRequest.setObTableQueryRequest(obTableQueryRequest);
    }

    @Override
    public ObTableQuery getObTableQuery() {
        return tableQuery;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    public TableQuery getTableQuery() {
        return this;
    }

    /**
     * Get table.
     * @return table
     */
    public ObTable getTable() {
        return table;
    }

    @Override
    public QueryResultSet execute() throws Exception {
        return new QueryResultSet(new ObTableQueryAsyncResultSet(this));
    }

    public QueryResultSet execute(ObQueryOperationType type) throws Exception {
        Map<Long, ObPair<Long, ObTableParam>> partitionObTable = new HashMap<Long, ObPair<Long, ObTableParam>>();
        partitionObTable.put(0L, new ObPair<Long, ObTableParam>(0L, new ObTableParam(table)));
        ObTableQueryAsyncStreamResult obTableQueryAsyncStreamResult = new ObTableQueryAsyncStreamResult();
        obTableQueryAsyncStreamResult.setTableQuery(tableQuery);
        obTableQueryAsyncStreamResult.setEntityType(entityType);
        obTableQueryAsyncStreamResult.setTableName(tableName);
        obTableQueryAsyncStreamResult.setExpectant(partitionObTable);
        obTableQueryAsyncStreamResult.setOperationTimeout(operationTimeout);

        obTableQueryAsyncStreamResult.init(type, sessionId);

        QueryResultSet queryAsyncResultSet = new QueryResultSet(obTableQueryAsyncStreamResult);
        boolean hasMore = !obTableQueryAsyncStreamResult.isEnd();
        queryAsyncResultSet.setHasMore(hasMore);
        queryAsyncResultSet.setSessionId(obTableQueryAsyncStreamResult.getSessionId());
        this.sessionId = obTableQueryAsyncStreamResult.getSessionId();
        return queryAsyncResultSet;
    }

    @Override
    public QueryResultSet executeInit(ObPair<Long, ObTableParam> entry) throws Exception {
        return execute(ObQueryOperationType.QUERY_START);
    }

    @Override
    public QueryResultSet executeNext(ObPair<Long, ObTableParam> entry) throws Exception {
        return execute(ObQueryOperationType.QUERY_NEXT);
    }

    @Override
    public TableQuery setKeys(String... keys) {
        throw new IllegalArgumentException("Not needed");
    }

    @Override
    public void clear() {
    }

}

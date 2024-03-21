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

package com.alipay.oceanbase.rpc.stream;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableStreamRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ObTableClientQueryAsyncStreamResult extends AbstractQueryStreamResult {
    private static final Logger logger = LoggerFactory
                                           .getLogger(ObTableClientQueryStreamResult.class);
    protected ObTableClient     client;
    private boolean             isEnd  = true;
    private long                sessionId = Constants.OB_INVALID_ID;
    private ObTableQueryAsyncRequest asyncRequest = new ObTableQueryAsyncRequest();

    @Override
    public void init() throws Exception {
        if (initialized) {
            return;
        }
        // init request
        ObTableQueryRequest request = new ObTableQueryRequest();
        request.setTableName(tableName);
        request.setTableQuery(tableQuery);
        request.setEntityType(entityType);
        request.setConsistencyLevel(getReadConsistency().toObTableConsistencyLevel());

        // construct async query request
        asyncRequest.setObTableQueryRequest(request);
        asyncRequest.setQueryType(ObQueryOperationType.QUERY_START);
        asyncRequest.setQuerySessionId(Constants.OB_INVALID_ID);

        // send to first tablet
        if (!expectant.isEmpty()) {
            Map.Entry<Long, ObPair<Long, ObTableParam>> firstEntry = expectant.entrySet().iterator().next();
            referToNewPartition(firstEntry.getValue());
        }
        initialized = true;
    }

    protected void cacheResultRows(ObTableQueryAsyncResult tableQueryResult) {
        cacheRows.clear();
        cacheRows.addAll(tableQueryResult.getAffectedEntity().getPropertiesRows());
        cacheProperties = tableQueryResult.getAffectedEntity().getPropertiesNames();
    }

    protected ObTableQueryAsyncResult referToNewPartition(ObPair<Long, ObTableParam> partIdWithObTable)
            throws Exception {
        ObTableParam obTableParam = partIdWithObTable.getRight();
        ObTableQueryRequest queryRequest = asyncRequest.getObTableQueryRequest();

        // refresh request info
        queryRequest.setPartitionId(obTableParam.getPartitionId());
        queryRequest.setTableId(obTableParam.getTableId());
        if (operationTimeout > 0) {
            queryRequest.setTimeout(operationTimeout);
        } else {
            queryRequest.setTimeout(obTableParam.getObTable().getObTableOperationTimeout());
        }

        // refresh async query request
        // since we try to access a new server, we set corresponding status
        asyncRequest.setQueryType(ObQueryOperationType.QUERY_START);
        asyncRequest.setQuerySessionId(Constants.OB_INVALID_ID);

        // async execute
        ObTableQueryAsyncResult ret = executeAsync(partIdWithObTable, asyncRequest);

        // remove useless expectant if it is end
        if (isEnd()) {
            expectant.remove(partIdWithObTable.getLeft());
        }

        return ret;
    }

    protected ObTableQueryAsyncResult referToLastStreamResult(ObPair<Long, ObTableParam> partIdWithObTable)
            throws Exception {
        ObTableParam obTableParam = partIdWithObTable.getRight();
        ObTableQueryRequest queryRequest = asyncRequest.getObTableQueryRequest();

        // refresh request info
        queryRequest.setPartitionId(obTableParam.getPartitionId());
        queryRequest.setTableId(obTableParam.getTableId());

        // refresh async query request
        asyncRequest.setQueryType(ObQueryOperationType.QUERY_NEXT);
        asyncRequest.setQuerySessionId(sessionId);

        // async execute
        ObTableQueryAsyncResult ret = executeAsync(partIdWithObTable, asyncRequest);

        // remove useless expectant if it is end
        if (isEnd()) {
            expectant.remove(partIdWithObTable.getLeft());
        }

        return ret;
    }

    @Override
    public boolean next() throws Exception {
        checkStatus();
        lock.lock();
        try {
            // firstly, refer to the cache
            if (!cacheRows.isEmpty()) {
                nextRow();
                return true;
            }

            // secondly, refer to the last stream result
            if (!isEnd() && !expectant.isEmpty()) {
                Map.Entry<Long, ObPair<Long, ObTableParam>> lastEntry = expectant.entrySet().iterator().next();
                referToLastStreamResult(lastEntry.getValue());
                if (!cacheRows.isEmpty()) {
                    nextRow();
                    return true;
                }
            }

            // lastly, refer to the new partition
            boolean hasNext = false;
            for (Map.Entry<Long, ObPair<Long, ObTableParam>> entry : expectant.entrySet()) {
                // try access new partition, async will remove useless expectant
                referToNewPartition(entry.getValue());
                if (!cacheRows.isEmpty()) {
                    hasNext = true;
                    nextRow();
                    break;
                }
            }

            return hasNext;
        } finally {
            lock.unlock();
        }
    }

    @Override
    protected ObTableQueryResult execute(ObPair<Long, ObTableParam> partIdWithObTable,
                                         ObPayload streamRequest) throws Exception {
        throw new IllegalArgumentException("not support this execute");
    }

    /*
     * Attention: executeAsync will remove useless expectant
     */
    @Override
    protected ObTableQueryAsyncResult executeAsync(ObPair<Long, ObTableParam> partIdWithObTable,
                                                   ObPayload streamRequest) throws Exception {
        // execute request
        ObTableQueryAsyncResult result = (ObTableQueryAsyncResult) commonExecute(this.client, logger, partIdWithObTable, streamRequest);

        // cache result
        cacheResultRows(result);

        // refresh async query status
        if (result.isEnd()) {
            isEnd = true;
        } else {
            isEnd = false;
        }
        sessionId = result.getSessionId();
        return result;
    }

    public ObTableClient getClient() {
        return client;
    }

    /**
     * Set client.
     * @param client client want to set
     */
    public void setClient(ObTableClient client) {
        this.client = client;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }
}

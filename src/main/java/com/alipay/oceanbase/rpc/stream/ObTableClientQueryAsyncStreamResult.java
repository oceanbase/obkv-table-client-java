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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.bolt.transport.ObTableConnection;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableNeedFetchAllException;
import com.alipay.oceanbase.rpc.exception.ObTableRetryExhaustedException;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.util.ByteUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;

public class ObTableClientQueryAsyncStreamResult extends AbstractQueryStreamResult {
    private static final Logger      logger         = LoggerFactory
                                                        .getLogger(ObTableClientQueryStreamResult.class);
    private boolean                  isEnd          = true;
    private long                     sessionId      = Constants.OB_INVALID_ID;
    private ObTableQueryAsyncRequest asyncRequest   = new ObTableQueryAsyncRequest();
    private ObTableConnection        prevConnection = null;

    private Iterator<Map.Entry<Long, ObPair<Long, ObTableParam>>> handleNextException(ObTableClient client,
                                                                                      int maxRetryTimes,
                                                                                      String tableName,
                                                                                      Iterator<Map.Entry<Long, ObPair<Long, ObTableParam>>> iterator,
                                                                                      Map.Entry<Long, ObPair<Long, ObTableParam>> entry,
                                                                                      Exception e,
                                                                                      int retryTimes)
                                                                                                     throws Exception {

        if (client.isOdpMode()) {
            if ((retryTimes - 1) < maxRetryTimes) {
                if (e instanceof ObTableException) {
                    logger.warn(
                        "execute while meet Exception, errorCode: {} , errorMsg: {}, try times {}",
                        ((ObTableException) e).getErrorCode(), e.getMessage(), retryTimes);
                } else {
                    logger.warn("execute while meet Exception, exception: {}, try times {}", e,
                        retryTimes);
                }
            } else {
                throw e;
            }
        } else {
            if (e instanceof ObTableException && ((ObTableException) e).isNeedRefreshTableEntry()) {
                if (client.isRetryOnChangeMasterTimes() && retryTimes <= maxRetryTimes) {
                    if (e instanceof ObTableNeedFetchAllException) {
                        TableEntry tableEntry = client.getOrRefreshTableEntry(tableName, false,
                            false, false);
                        // Calculate the next partition only when the range partition is affected by a split, based on the keys already scanned.
                        if (ObGlobal.obVsnMajor() >= 4
                            && tableEntry.isPartitionTable()
                            && tableEntry.getPartitionInfo().getFirstPartDesc().getPartFuncType()
                                .isRangePart()) {
                            this.asyncRequest.getObTableQueryRequest().getTableQuery()
                                .adjustStartKey(currentStartKey);
                            setExpectant(refreshPartition(this.asyncRequest
                                .getObTableQueryRequest().getTableQuery(), tableName));
                            setEnd(true);
                        } else {
                            setExpectant(refreshPartition(this.asyncRequest
                                .getObTableQueryRequest().getTableQuery(), tableName));
                        }
                        // Return a new iterator  
                        return expectant.entrySet().iterator();
                    }
                } else {
                    client.calculateContinuousFailure(tableName, e.getMessage());
                    throw e;
                }
            }
        }
        return iterator; // Return the original iterator if no changes are made  
    }

    @Override
    public void init() throws Exception {
        if (initialized) {
            return;
        }
        int maxRetries = client.getTableEntryRefreshTryTimes();
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
            Iterator<Map.Entry<Long, ObPair<Long, ObTableParam>>> it = expectant.entrySet()
                .iterator();
            int retryTimes = 0;
            while (it.hasNext()) {
                executeWithRetry(client, client.getRuntimeRetryTimes(), tableName, it, this::referToNewPartition, this::handleException);
                break;
            }
            if (isEnd())
                it.remove();
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

        return ret;
    }

    protected void closeLastStreamResult(ObPair<Long, ObTableParam> partIdWithObTable)
                                                                                      throws Exception {
        ObTableParam obTableParam = partIdWithObTable.getRight();
        ObTableQueryRequest queryRequest = asyncRequest.getObTableQueryRequest();

        // refresh request info
        queryRequest.setPartitionId(obTableParam.getPartitionId());
        queryRequest.setTableId(obTableParam.getTableId());

        // set end async query
        asyncRequest.setQueryType(ObQueryOperationType.QUERY_END);
        asyncRequest.setQuerySessionId(sessionId);

        // async execute
        ObTableQueryAsyncResult ret = executeAsync(partIdWithObTable, asyncRequest);

        if (!isEnd()) {
            throw new ObTableException("failed to close last stream result");
        }
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
                Iterator<Map.Entry<Long, ObPair<Long, ObTableParam>>> it = expectant.entrySet()
                    .iterator();
                executeWithRetry(client, client.getRuntimeRetryTimes(), tableName, it, this::referToLastStreamResult, this::handleNextException);
                // remove useless expectant if it is end
                if (isEnd())
                    it.remove();

                if (!cacheRows.isEmpty()) {
                    nextRow();
                    return true;
                }
            }

            // lastly, refer to the new partition
            boolean hasNext = false;
            Iterator<Map.Entry<Long, ObPair<Long, ObTableParam>>> it = expectant.entrySet()
                .iterator();
            while (it.hasNext()) {
                executeWithRetry(client, client.getRuntimeRetryTimes(), tableName, it, this::referToNewPartition, this::handleException);
                // remove useless expectant if it is end
                if (isEnd())
                    it.remove();

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
        // Construct connection reference
        AtomicReference<ObTableConnection> connectionRef = new AtomicReference<>();
        if (client.isOdpMode() && !isEnd && prevConnection != null) {
            connectionRef.set(prevConnection);
        }

        // execute request
        ObTableQueryAsyncResult result = (ObTableQueryAsyncResult) commonExecute(this.client,
            logger, partIdWithObTable, streamRequest, connectionRef);

        // cache result
        cacheResultRows(result);

        // refresh async query status
        if (result.isEnd()) {
            isEnd = true;
        } else {
            isEnd = false;
            prevConnection = connectionRef.get();
        }
        sessionId = result.getSessionId();
        return result;
    }

    @Override
    public void close() throws Exception {
        // set status
        if (closed) {
            return;
        }
        closed = true;

        // send end packet to last tablet
        if (!isEnd() && !expectant.isEmpty()) {
            Iterator<Map.Entry<Long, ObPair<Long, ObTableParam>>> it = expectant.entrySet()
                .iterator();
            // get the last tablet
            Map.Entry<Long, ObPair<Long, ObTableParam>> lastEntry = it.next();
            // try access new partition, async will not remove useless expectant
            closeLastStreamResult(lastEntry.getValue());
        }
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }
}

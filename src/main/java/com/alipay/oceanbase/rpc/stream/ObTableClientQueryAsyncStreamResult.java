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

import com.alipay.oceanbase.rpc.bolt.transport.ObTableConnection;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static com.alipay.oceanbase.rpc.constant.Constants.INVALID_TABLET_ID;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;

public class ObTableClientQueryAsyncStreamResult extends AbstractQueryStreamResult {
    private static final Logger      logger              = LoggerFactory
                                                             .getLogger(ObTableClientQueryStreamResult.class);
    private boolean                  isEnd               = true;
    private long                     sessionId           = Constants.OB_INVALID_ID;
    private ObTableQueryAsyncRequest asyncRequest        = new ObTableQueryAsyncRequest();
    private ObTableConnection        prevConnection      = null;
    private boolean                  allowDistributeScan = true;                                              // false when partition scan

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
                Map.Entry<Long, ObPair<Long, ObTableParam>> firstEntry = it.next();
                try {
                    // try access new partition, async will not remove useless expectant
                    referToNewPartition(firstEntry.getValue());
                    break;
                } catch (Exception e) {
                    if (e instanceof ObTableNeedFetchMetaException) {
                        setExpectant(refreshPartition(this.asyncRequest.getObTableQueryRequest()
                            .getTableQuery(), client.getPhyTableNameFromTableGroup(entityType,
                            tableName)));
                        it = expectant.entrySet().iterator();
                        retryTimes++;
                        if (retryTimes > maxRetries) {
                            RUNTIME.error("Fail to get refresh table entry response after {}",
                                retryTimes);
                            throw new ObTableRetryExhaustedException(
                                "Fail to get refresh table entry response after " + retryTimes
                                        + "errorCode:"
                                        + ((ObTableNeedFetchMetaException) e).getErrorCode());

                        }
                    } else {
                        throw e;
                    }
                }
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
        long partitionId = isDistributeScan() ? INVALID_TABLET_ID : obTableParam.getPartitionId();
        // refresh request info
        queryRequest.setPartitionId(partitionId);
        queryRequest.setTableId(obTableParam.getTableId());
        if (operationTimeout > 0) {
            asyncRequest.setTimeout(operationTimeout);
        } else {
            asyncRequest.setTimeout(obTableParam.getObTable().getObTableOperationTimeout());
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
        long partitionId = isDistributeScan() ? INVALID_TABLET_ID : obTableParam.getPartitionId();
        queryRequest.setPartitionId(partitionId);
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
        long partitionId = isDistributeScan() ? INVALID_TABLET_ID : obTableParam.getPartitionId();
        queryRequest.setPartitionId(partitionId);
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
    protected Map<Long, ObPair<Long, ObTableParam>> refreshPartition(ObTableQuery tableQuery,
                                                                     String tableName)
                                                                                      throws Exception {
        return buildPartitions(client, tableQuery, tableName);
    }

    // This function is designed for HBase-type requests.
    // It is used to extend the session duration of a scan
    @Override
    public void renewLease() throws Exception {
        if (!isEnd() && !expectant.isEmpty()) {
            Iterator<Map.Entry<Long, ObPair<Long, ObTableParam>>> it = expectant.entrySet()
                .iterator();
            Map.Entry<Long, ObPair<Long, ObTableParam>> lastEntry = it.next();
            ObPair<Long, ObTableParam> partIdWithObTable = lastEntry.getValue();
            // try access new partition, async will not remove useless expectant
            ObTableParam obTableParam = partIdWithObTable.getRight();
            ObTableQueryRequest queryRequest = asyncRequest.getObTableQueryRequest();

            // refresh request info
            long partitionId = isDistributeScan() ? INVALID_TABLET_ID : obTableParam
                .getPartitionId();
            queryRequest.setPartitionId(partitionId);
            queryRequest.setTableId(obTableParam.getTableId());

            // refresh async query request
            asyncRequest.setQueryType(ObQueryOperationType.QUERY_RENEW);
            asyncRequest.setQuerySessionId(sessionId);
            executeAsync(partIdWithObTable, asyncRequest);
        } else {
            throw new ObTableException("query end or expectant is null");
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

                Map.Entry<Long, ObPair<Long, ObTableParam>> lastEntry = it.next();
                try {
                    // try access new partition, async will not remove useless expectant
                    referToLastStreamResult(lastEntry.getValue());
                } catch (Exception e) {
                    if (e instanceof ObTableNeedFetchMetaException) {
                        String realTableName = client.getPhyTableNameFromTableGroup(entityType,
                            tableName);
                        TableEntry entry = client.getOrRefreshTableEntry(realTableName, false);
                        // Calculate the next partition only when the range partition is affected by a split, based on the keys already scanned.
                        if (entry.isPartitionTable()
                            && entry.getPartitionInfo().getFirstPartDesc().getPartFuncType()
                                .isRangePart()) {
                            this.asyncRequest.getObTableQueryRequest().getTableQuery()
                                .adjustStartKey(currentStartKey);
                            setExpectant(refreshPartition(this.asyncRequest
                                .getObTableQueryRequest().getTableQuery(), realTableName));
                            setEnd(true);
                        }
                    } else {
                        throw e;
                    }
                }
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
            int retryTimes = 0;
            while (it.hasNext()) {
                Map.Entry<Long, ObPair<Long, ObTableParam>> entry = it.next();
                try {
                    // try access new partition, async will not remove useless expectant
                    referToNewPartition(entry.getValue());
                } catch (Exception e) {
                    if (e instanceof ObTableNeedFetchMetaException) {
                        String realTableName = client.getPhyTableNameFromTableGroup(entityType,
                            tableName);
                        TableEntry tableEntry = client.getOrRefreshTableEntry(realTableName, false);
                        if (tableEntry.isPartitionTable()
                            && tableEntry.getPartitionInfo().getFirstPartDesc().getPartFuncType()
                                .isRangePart()) {
                            this.asyncRequest.getObTableQueryRequest().getTableQuery()
                                .adjustStartKey(currentStartKey);
                            setExpectant(refreshPartition(this.asyncRequest
                                .getObTableQueryRequest().getTableQuery(), realTableName));
                        }
                        it = expectant.entrySet().iterator();
                        retryTimes++;
                        if (retryTimes > client.getTableEntryRefreshTryTimes()) {
                            RUNTIME.error("Fail to get refresh table entry response after {}",
                                retryTimes);
                            throw new ObTableRetryExhaustedException(
                                "Fail to get refresh table entry response after " + retryTimes);
                        }
                        continue;
                    } else {
                        throw e;
                    }
                }

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
        ObTableQueryAsyncResult result = null;
        int tryTimes = 0;
        long startExecute = System.currentTimeMillis();
        while (true) {
            long costMillis = System.currentTimeMillis() - startExecute;
            if (costMillis > client.getRuntimeMaxWait()) {
                logger.error("tableName: {} has tried " + tryTimes + " times and it has waited " + costMillis +
                        " ms which execeeds runtime max wait timeout " + client.getRuntimeMaxWait() + " ms", tableName);
                throw new ObTableTimeoutExcetion("query timeout and retried " + tryTimes + " times");
            }
            tryTimes++;
            try {
                result = (ObTableQueryAsyncResult) commonExecute(this.client,
                        logger, partIdWithObTable, streamRequest, connectionRef);
                break;
            } catch (ObTableServerCacheExpiredException e) {
                client.syncRefreshMetadata(false);
            }  catch (ObTableEntryRefreshException e) {
                if (e.isConnectInactive()) {
                    client.syncRefreshMetadata(true);
                } else {
                    throw e;
                }
            } catch (Throwable t) {
                throw t;
            }
        }
        if (result == null) {
            throw new ObTableRetryExhaustedException("query timeout and retried " + tryTimes + " times");
        }
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

    private boolean isDistributeScan() {
        return allowDistributeScan && client.getServerCapacity().isSupportDistributedExecute();
    }

    public void setAllowDistributeScan(boolean allowDistributeScan) {
        this.allowDistributeScan = allowDistributeScan;
    }
}

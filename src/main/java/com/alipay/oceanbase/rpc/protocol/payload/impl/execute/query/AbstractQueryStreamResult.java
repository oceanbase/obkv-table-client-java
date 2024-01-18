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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query;

import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.location.model.ObReadConsistency;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableStreamRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.QueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObQueryOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractQueryStreamResult extends AbstractPayload implements
                                                                       QueryStreamResult {

    private ReentrantLock                                                      lock                = new ReentrantLock();
    private volatile boolean                                                   initialized         = false;
    private volatile boolean                                                   closed              = false;
    protected volatile List<ObObj>                                             row                 = null;
    protected volatile int                                                     rowIndex            = -1;
    protected ObTableQuery                                                     tableQuery;
    protected long                                                             operationTimeout    = -1;
    protected String                                                           tableName;
    protected ObTableEntityType                                                entityType;
    private Map<Long, ObPair<Long, ObTableParam>>                              expectant;                                                                                     // Map<logicId, ObPair<logicId, param>>
    private List<String>                                                       cacheProperties     = new LinkedList<String>();
    private LinkedList<List<ObObj>>                                            cacheRows           = new LinkedList<List<ObObj>>();
    private LinkedList<ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult>> partitionLastResult = new LinkedList<ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult>>();
    private ObReadConsistency                                                  readConsistency     = ObReadConsistency.STRONG;

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE_QUERY;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        throw new FeatureNotSupportedException("stream result can not decode from bytes");
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        throw new FeatureNotSupportedException("stream result can not decode from bytes");
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        throw new FeatureNotSupportedException("stream result has no pay load size");
    }

    /*
     * Next.
     */
    public boolean next() throws Exception {
        checkStatus();
        lock.lock();
        try {
            // firstly, refer to the cache
            if (cacheRows.size() > 0) {
                nextRow();
                return true;
            }
            // secondly, refer to the last stream result
            ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult> referLastResult;
            while ((referLastResult = partitionLastResult.poll()) != null) {

                ObTableQueryResult lastResult = referLastResult.getRight();
                if (lastResult.isStream() && lastResult.isStreamNext()) {
                    ObTableQueryResult tableQueryResult = referToLastStreamResult(
                        referLastResult.getLeft(), lastResult);
                    if (tableQueryResult.getRowCount() == 0) {
                        continue;
                    }
                    nextRow();
                    return true;
                }
            }

            // lastly, refer to the new partition
            boolean hasNext = false;
            List<Map.Entry<Long, ObPair<Long, ObTableParam>>> referPartition = new ArrayList<Map.Entry<Long, ObPair<Long, ObTableParam>>>();
            for (Map.Entry<Long, ObPair<Long, ObTableParam>> entry : expectant.entrySet()) {
                // mark the refer partition
                referPartition.add(entry);
                ObTableQueryResult tableQueryResult = referToNewPartition(entry.getValue());
                if (tableQueryResult.getRowCount() == 0) {
                    continue;
                }
                hasNext = true;
                nextRow();
                break;
            }

            // remove refer partition
            for (Map.Entry<Long, ObPair<Long, ObTableParam>> entry : referPartition) {
                expectant.remove(entry.getKey());
            }

            return hasNext;
        } finally {
            lock.unlock();
        }
    }

    private void nextRow() {
        rowIndex = rowIndex + 1;
        row = cacheRows.poll();
    }

    private void checkStatus() throws IllegalStateException {
        if (!initialized) {
            throw new IllegalStateException("table " + tableName
                                            + "query stream result is not initialized");
        }

        if (closed) {
            throw new IllegalStateException("table " + tableName
                                            + " query stream result is  closed");
        }
    }

    protected ObTableQueryResult checkObTableQueryResult(Object result) {
        if (result == null) {
            throw new ObTableException("client get unexpected NULL result");
        }

        if (!(result instanceof ObTableQueryResult)) {
            throw new ObTableException("client get unexpected result: "
                                       + result.getClass().getName() + "expect "
                                       + ObTableQueryResult.class.getName());
        }
        return (ObTableQueryResult) result;
    }

    protected ObTableQueryAsyncResult checkObTableQuerySyncResult(Object result) {
        if (result == null) {
            throw new ObTableException("client get unexpected NULL result");
        }

        if (!(result instanceof ObTableQueryAsyncResult)) {
            throw new ObTableException("client get unexpected result: "
                                       + result.getClass().getName() + "expect "
                                       + ObTableQueryAsyncResult.class.getName());
        }
        return (ObTableQueryAsyncResult) result;
    }

    private ObTableQueryResult referToLastStreamResult(ObPair<Long, ObTableParam> partIdWithObTable,
                                                       ObTableQueryResult lastResult)
                                                                                     throws Exception {
        ObTableStreamRequest streamRequest = new ObTableStreamRequest();
        streamRequest.setSessionId(lastResult.getSessionId());
        streamRequest.setStreamNext();
        if (operationTimeout > 0) {
            streamRequest.setTimeout(operationTimeout);
        } else {
            streamRequest.setTimeout(partIdWithObTable.getRight().getObTable()
                .getObTableOperationTimeout());
        }
        return execute(partIdWithObTable, streamRequest);
    }

    private void closeLastStreamResult(ObPair<Long, ObTableParam> partIdWithObTable,
                                       ObTableQueryResult lastResult) throws Exception {
        ObTableStreamRequest streamRequest = new ObTableStreamRequest();
        streamRequest.setSessionId(lastResult.getSessionId());
        streamRequest.setStreamLast();
        if (operationTimeout > 0) {
            streamRequest.setTimeout(operationTimeout);
        } else {
            streamRequest.setTimeout(partIdWithObTable.getRight().getObTable()
                .getObTableOperationTimeout());
        }
        partIdWithObTable.getRight().getObTable().execute(streamRequest);
    }

    private ObTableQueryResult referToNewPartition(ObPair<Long, ObTableParam> partIdWithObTable)
                                                                                                throws Exception {
        ObTableQueryRequest request = new ObTableQueryRequest();
        request.setTableName(tableName);
        request.setTableQuery(tableQuery);
        request.setPartitionId(partIdWithObTable.getRight().getPartitionId());
        request.setTableId(partIdWithObTable.getRight().getTableId());
        request.setEntityType(entityType);
        if (operationTimeout > 0) {
            request.setTimeout(operationTimeout);
        } else {
            request.setTimeout(partIdWithObTable.getRight().getObTable()
                .getObTableOperationTimeout());
        }
        request.setConsistencyLevel(getReadConsistency().toObTableConsistencyLevel());
        return execute(partIdWithObTable, request);
    }

    private ObTableQueryAsyncResult referToNewPartition(ObPair<Long, ObTableParam> partIdWithObTable,
                                                        ObQueryOperationType type, long sessionID)
                                                                                                  throws Exception {
        ObTableQueryAsyncRequest asyncRequest = new ObTableQueryAsyncRequest();
        ObTableQueryRequest request = new ObTableQueryRequest();

        request.setTableName(tableName);
        request.setTableQuery(tableQuery);
        request.setPartitionId(partIdWithObTable.getRight().getPartitionId());
        request.setTableId(partIdWithObTable.getRight().getTableId());
        request.setEntityType(entityType);
        asyncRequest.setObTableQueryRequest(request);
        asyncRequest.setQueryType(type);
        asyncRequest.setQuerySessionId(sessionID);
        if (operationTimeout > 0) {
            asyncRequest.setTimeout(operationTimeout);
        } else {
            asyncRequest.setTimeout(partIdWithObTable.getRight().getObTable()
                .getObTableOperationTimeout());
        }
        return executeAsync(partIdWithObTable, asyncRequest);
    }

    protected abstract ObTableQueryResult execute(ObPair<Long, ObTableParam> partIdWithObTable,
                                                  ObPayload streamRequest) throws Exception;

    protected abstract ObTableQueryAsyncResult executeAsync(ObPair<Long, ObTableParam> partIdWithObTable,
                                                            ObPayload streamRequest)
                                                                                    throws Exception;

    private void cacheResultRows(ObTableQueryResult tableQueryResult) {
        cacheRows.addAll(tableQueryResult.getPropertiesRows());
        cacheProperties = tableQueryResult.getPropertiesNames();
    }

    protected void cacheStreamNext(ObPair<Long, ObTableParam> partIdWithObTable,
                                   ObTableQueryResult tableQueryResult) {
        cacheResultRows(tableQueryResult);
        if (tableQueryResult.isStream() && tableQueryResult.isStreamNext()) {
            partitionLastResult.addLast(new ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult>(
                partIdWithObTable, tableQueryResult));
        }
    }

    private void cacheResultRows(ObTableQueryAsyncResult tableQuerySyncResult) {
        cacheRows.addAll(tableQuerySyncResult.getAffectedEntity().getPropertiesRows());
        cacheProperties = tableQuerySyncResult.getAffectedEntity().getPropertiesNames();
    }

    protected void cacheStreamNext(ObPair<Long, ObTableParam> partIdWithObTable,
                                   ObTableQueryAsyncResult tableQuerySyncResult) {
        cacheResultRows(tableQuerySyncResult);
        if (tableQuerySyncResult.getAffectedEntity().isStream()
            && tableQuerySyncResult.getAffectedEntity().isStreamNext()) {
            partitionLastResult.addLast(new ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult>(
                partIdWithObTable, tableQuerySyncResult.getAffectedEntity()));
        }
    }

    /**
     * Get row.
     */
    public List<ObObj> getRow() {
        if (rowIndex == -1) {
            throw new IllegalStateException("before result set start");
        }
        return row;
    }

    /*
     * Get row index.
     */
    @Override
    public int getRowIndex() {
        return rowIndex;
    }

    /*
     * Init.
     */
    @Override
    public void init() throws Exception {
        if (initialized) {
            return;
        }
        if (tableQuery.getBatchSize() == -1) {
            for (Map.Entry<Long, ObPair<Long, ObTableParam>> entry : expectant.entrySet()) {
                // mark the refer partition
                referToNewPartition(entry.getValue());
            }
            expectant.clear();
        } else {
            // query not support BatchSize, use queryByBatch instead queryByBatchV2
            throw new ObTableException(
                "query not support BatchSize, use queryByBatch / queryByBatchV2"
                        + " instead, BatchSize:" + tableQuery.getBatchSize());
        }
        initialized = true;
    }

    public void init(ObQueryOperationType type, long sessionID) throws Exception {
        if (initialized) {
            return;
        }
        for (Map.Entry<Long, ObPair<Long, ObTableParam>> entry : expectant.entrySet()) {
            // mark the refer partition
            referToNewPartition(entry.getValue(), type, sessionID);
        }
        expectant.clear();
        initialized = true;
    }

    public void init(ObQueryOperationType type, ObPair<Long, ObTableParam> entry, long sessionID)
                                                                                                 throws Exception {
        if (initialized) {
            return;
        }
        referToNewPartition(entry, type, sessionID);
        expectant.clear();
        initialized = true;
    }

    /**
     * Close.
     */
    public void close() throws Exception {
        if (closed) {
            return;
        }
        closed = true;
        ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult> referLastResult;
        while ((referLastResult = partitionLastResult.poll()) != null) {
            ObTableQueryResult lastResult = referLastResult.getRight();
            closeLastStreamResult(referLastResult.getLeft(), lastResult);
        }
    }

    /*
     * Get cache properties.
     */
    public List<String> getCacheProperties() {
        return cacheProperties;
    }

    /*
     * Get cache rows.
     */
    public LinkedList<List<ObObj>> getCacheRows() {
        return cacheRows;
    }

    public LinkedList<ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult>> getPartitionLastResult() {
        return partitionLastResult;
    }

    /*
     * Get table query.
     */
    public ObTableQuery getTableQuery() {
        return tableQuery;
    }

    /*
     * Set table query.
     */
    public void setTableQuery(ObTableQuery tableQuery) {
        this.tableQuery = tableQuery;
    }

    /*
     * Get operation timeout.
     */
    public long getOperationTimeout() {
        return operationTimeout;
    }

    /*
     * Set operation timeout.
     */
    public void setOperationTimeout(long operationTimeout) {
        this.operationTimeout = operationTimeout;
    }

    /*
     * Get table name.
     */
    public String getTableName() {
        return tableName;
    }

    /*
     * Set table name.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /*
     * Get entity type.
     */
    public ObTableEntityType getEntityType() {
        return entityType;
    }

    /*
     * Set entity type.
     */
    public void setEntityType(ObTableEntityType entityType) {
        this.entityType = entityType;
    }

    public Map<Long, ObPair<Long, ObTableParam>> getExpectant() {
        return expectant;
    }

    /*
     * Set expectant.
     */
    public void setExpectant(Map<Long, ObPair<Long, ObTableParam>> expectant) {
        this.expectant = expectant;
    }

    /*
     * Get Read Consistency
     */
    public ObReadConsistency getReadConsistency() {
        return readConsistency;
    }

    /*
     * Set Read Consistency
     *
     * @param readConsistency
     */
    public void setReadConsistency(ObReadConsistency readConsistency) {
        this.readConsistency = readConsistency;
    }
}

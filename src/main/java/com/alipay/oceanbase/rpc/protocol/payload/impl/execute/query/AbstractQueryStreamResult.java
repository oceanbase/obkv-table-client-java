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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.bolt.transport.ObTableConnection;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.model.ObReadConsistency;
import com.alipay.oceanbase.rpc.location.model.ObServerRoute;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableApiMove;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableStreamRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.QueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractQueryStreamResult extends AbstractPayload implements
                                                                       QueryStreamResult {

    protected ReentrantLock                                                    lock                = new ReentrantLock();
    protected volatile boolean                                                 initialized         = false;
    protected volatile boolean                                                 closed              = false;
    protected volatile List<ObObj>                                             row                 = null;
    protected volatile int                                                     rowIndex            = -1;
    // 调整它的startKey
    protected ObTableQuery                                                     tableQuery;
    protected long                                                             operationTimeout    = -1;
    protected String                                                           tableName;
    // use to store the TableEntry Key:
    // primary index or local index: key is primary table name
    // global index: key is index table name (be like: __idx_<data_table_id>_<index_name>)
    protected String                                                           indexTableName;
    protected ObTableEntityType                                                entityType;
    protected Map<Long, ObPair<Long, ObTableParam>>                            expectant;
    protected List<String>                                                     cacheProperties     = new LinkedList<String>();
    protected LinkedList<List<ObObj>>                                          cacheRows           = new LinkedList<List<ObObj>>();
    private LinkedList<ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult>> partitionLastResult = new LinkedList<ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult>>();
    private ObReadConsistency                                                  readConsistency     = ObReadConsistency.STRONG;
    // ObRowKey objs: [startKey, MIN_OBJECT, MIN_OBJECT]
    public List<ObObj>                                                         currentStartKey;

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
     * Common logic for execute, send the query request to server
     */
    protected ObPayload commonExecute(ObTableClient client, Logger logger,
                                      ObPair<Long, ObTableParam> partIdWithIndex,
                                      ObPayload request,
                                      AtomicReference<ObTableConnection> connectionRef)
                                                                                       throws Exception {
        ObPayload result;
        ObTable subObTable = partIdWithIndex.getRight().getObTable();
        boolean needRefreshTableEntry = false;
        boolean odpNeedRenew = false;
        int tryTimes = 0;
        long startExecute = System.currentTimeMillis();
        Set<String> failedServerList = null;
        ObServerRoute route = null;
        while (true) {
            client.checkStatus();
            long currentExecute = System.currentTimeMillis();
            long costMillis = currentExecute - startExecute;
            if (costMillis > client.getRuntimeMaxWait()) {
                long uniqueId = request.getUniqueId();
                long sequence = request.getSequence();
                String trace = String.format("Y%X-%016X", uniqueId, sequence);
                throw new ObTableTimeoutExcetion("[" + trace + "]" + " has tried " + tryTimes
                                                 + " times and it has waited " + costMillis
                                                 + "/ms which exceeds response timeout "
                                                 + client.getRuntimeMaxWait() + "/ms");
            }
            tryTimes++;
            try {
                if (tryTimes > 1) {
                    if (client.isOdpMode()) {
                        subObTable = client
                            .getODPTableWithPartId(tableName, partIdWithIndex.getLeft(),
                                odpNeedRenew).getRight().getObTable();
                    } else {
                        if (route == null) {
                            route = client.getReadRoute();
                        }
                        if (failedServerList != null) {
                            route.setBlackList(failedServerList);
                        }
                        subObTable = client
                            .getTableWithPartId(indexTableName, partIdWithIndex.getLeft(),
                                needRefreshTableEntry, client.isTableEntryRefreshIntervalWait(),
                                false, route).getRight().getObTable();
                    }
                }
                if (client.isOdpMode()) {
                    result = subObTable.executeWithConnection(request, connectionRef);
                } else {
                    result = subObTable.execute(request);

                    if (result != null && result.getPcode() == Pcodes.OB_TABLE_API_MOVE) {
                        ObTableApiMove moveResponse = (ObTableApiMove) result;
                        client.getRouteTableRefresher().addTableIfAbsent(indexTableName, true);
                        client.getRouteTableRefresher().triggerRefreshTable();
                        subObTable = client.getTable(moveResponse);
                        result = subObTable.execute(request);
                        if (result instanceof ObTableApiMove) {
                            ObTableApiMove move = (ObTableApiMove) result;
                            logger
                                .warn(
                                    "The server has not yet completed the master switch, and returned an incorrect leader with an IP address of {}. "
                                            + "Rerouting return IP is {}", moveResponse
                                        .getReplica().getServer().ipToString(), move.getReplica()
                                        .getServer().ipToString());
                            throw new ObTableRoutingWrongException();
                        }
                    }
                }
                client.resetExecuteContinuousFailureCount(indexTableName);
                break;
            } catch (Exception e) {
                if (client.isOdpMode()) {
                    if ((tryTimes - 1) < client.getRuntimeRetryTimes()) {
                        if (e instanceof ObTableException) {
                            logger
                                .warn(
                                    "tablename:{} stream query execute while meet Exception needing retry, errorCode: {}, errorMsg: {}, try times {}",
                                    indexTableName, ((ObTableException) e).getErrorCode(),
                                    e.getMessage(), tryTimes);
                            if (e instanceof ObTablePartitionChangeException
                                && ((ObTablePartitionChangeException) e).getErrorCode() == ResultCodes.OB_ERR_KV_ROUTE_ENTRY_EXPIRE.errorCode) {
                                odpNeedRenew = true;
                            } else {
                                throw e;
                            }
                        } else if (e instanceof IllegalArgumentException) {
                            logger
                                .warn(
                                    "tablename:{} stream query execute while meet Exception needing retry, try times {}, errorMsg: {}",
                                    indexTableName, tryTimes, e.getMessage());
                            throw e;
                        } else {
                            logger
                                .warn(
                                    "tablename:{} stream query execute while meet Exception needing retry, try times {}",
                                    indexTableName, tryTimes, e);
                            throw e;
                        }
                    } else {
                        throw e;
                    }
                } else {
                    if (e instanceof ObTableReplicaNotReadableException) {
                        if ((tryTimes - 1) < client.getRuntimeRetryTimes()) {
                            logger.warn(
                                "tablename:{} partition id:{} retry when replica not readable: {}",
                                indexTableName, partIdWithIndex.getLeft(), e.getMessage(), e);
                            if (failedServerList == null) {
                                failedServerList = new HashSet<String>();
                            }
                            failedServerList.add(subObTable.getIp());
                        } else {
                            logger
                                .warn(
                                    "tablename:{} partition id:{} exhaust retry when replica not readable: {}",
                                    indexTableName, partIdWithIndex.getLeft(), e.getMessage(), e);
                            throw e;
                        }
                    } else if (e instanceof ObTableGlobalIndexRouteException) {
                        if ((tryTimes - 1) < client.getRuntimeRetryTimes()) {
                            logger
                                .warn(
                                    "meet global index route expcetion: indexTableName:{} partition id:{}, errorCode: {}, retry times {}",
                                    indexTableName, partIdWithIndex.getLeft(),
                                    ((ObTableException) e).getErrorCode(), tryTimes, e);
                            indexTableName = client.getIndexTableName(tableName,
                                tableQuery.getIndexName(), tableQuery.getScanRangeColumns(), true);
                        } else {
                            logger
                                .warn(
                                    "meet global index route expcetion: indexTableName:{} partition id:{}, errorCode: {}, reach max retry times {}",
                                    indexTableName, partIdWithIndex.getLeft(),
                                    ((ObTableException) e).getErrorCode(), tryTimes, e);
                            throw e;
                        }
                    } else if (e instanceof ObTableException) {
                        if ((((ObTableException) e).getErrorCode() == ResultCodes.OB_TABLE_NOT_EXIST.errorCode || ((ObTableException) e)
                            .getErrorCode() == ResultCodes.OB_NOT_SUPPORTED.errorCode)
                            && ((request instanceof ObTableQueryAsyncRequest && ((ObTableQueryAsyncRequest) request)
                                .getObTableQueryRequest().getTableQuery().isHbaseQuery()) || (request instanceof ObTableQueryRequest && ((ObTableQueryRequest) request)
                                .getTableQuery().isHbaseQuery()))
                            && client.getTableGroupInverted().get(indexTableName) != null) {
                            // table not exists && hbase mode && table group exists , three condition both
                            client.eraseTableGroupFromCache(tableName);
                        }
                        if (((ObTableException) e).isNeedRefreshTableEntry()) {
                            needRefreshTableEntry = true;
                            logger
                                .warn(
                                    "tablename:{} partition id:{} stream query refresh table while meet Exception needing refresh, errorCode: {}",
                                    indexTableName, partIdWithIndex.getLeft(),
                                    ((ObTableException) e).getErrorCode(), e);
                            if (client.isRetryOnChangeMasterTimes()
                                && (tryTimes - 1) < client.getRuntimeRetryTimes()) {
                                logger
                                    .warn(
                                        "tablename:{} partition id:{} stream query retry while meet Exception needing refresh, errorCode: {} , retry times {}",
                                        indexTableName, partIdWithIndex.getLeft(),
                                        ((ObTableException) e).getErrorCode(), tryTimes, e);
                                // tablet not exists, refresh table entry
                                if (e instanceof ObTableNeedFetchAllException) {
                                    client.getOrRefreshTableEntry(indexTableName, true, true, true);
                                    throw e;
                                }
                            } else {
                                client.calculateContinuousFailure(indexTableName, e.getMessage());
                                throw e;
                            }
                        } else {
                            client.calculateContinuousFailure(indexTableName, e.getMessage());
                            throw e;
                        }
                    } else {
                        client.calculateContinuousFailure(indexTableName, e.getMessage());
                        throw e;
                    }
                }
            }
            Thread.sleep(client.getRuntimeRetryInterval());
        }
        return result;
    }

    /*
     * RenewLease.
     */
    public void renewLease() throws Exception {
        throw new IllegalStateException("renew only support stream query");
    }

    /*
     * Next.
     */
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
            Iterator<Map.Entry<Long, ObPair<Long, ObTableParam>>> it = expectant.entrySet()
                .iterator();
            while (it.hasNext()) {
                Map.Entry<Long, ObPair<Long, ObTableParam>> entry = it.next();
                referPartition.add(entry);
                try {
                    // Mark the refer partition  
                    referPartition.add(entry);

                    // Try accessing the new partition  
                    ObTableQueryResult tableQueryResult = (ObTableQueryResult) referToNewPartition(entry
                        .getValue());

                    if (tableQueryResult.getRowCount() == 0) {
                        continue;
                    }

                    hasNext = true;
                    nextRow();
                    break;

                } catch (Exception e) {
                    if (e instanceof ObTableNeedFetchAllException) {
                        // Adjust the start key and refresh the expectant
                        this.tableQuery.adjustStartKey(currentStartKey);
                        setExpectant(refreshPartition(tableQuery, tableName));

                        // Reset the iterator to start over  
                        it = expectant.entrySet().iterator();
                        referPartition.clear(); // Clear the referPartition if needed
                    } else {
                        throw e;
                    }
                }
            }

            for (Map.Entry<Long, ObPair<Long, ObTableParam>> entry : expectant.entrySet()) {
                // mark the refer partition
                referPartition.add(entry);
                ObTableQueryResult tableQueryResult = (ObTableQueryResult) referToNewPartition(entry
                    .getValue());
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

    protected Map<Long, ObPair<Long, ObTableParam>> buildPartitions(ObTableClient client, ObTableQuery tableQuery, String tableName) throws Exception {
        Map<Long, ObPair<Long, ObTableParam>> partitionObTables = new LinkedHashMap<>();
        String indexName = tableQuery.getIndexName();
        String indexTableName = null;

        if (!client.isOdpMode()) {
            indexTableName = client.getIndexTableName(tableName, indexName, tableQuery.getScanRangeColumns(), false);
        }

        for (ObNewRange range : tableQuery.getKeyRanges()) {
            ObRowKey startKey = range.getStartKey();
            int startKeySize = startKey.getObjs().size();
            ObRowKey endKey = range.getEndKey();
            int endKeySize = endKey.getObjs().size();
            Object[] start = new Object[startKeySize];
            Object[] end = new Object[endKeySize];

            for (int i = 0; i < startKeySize; i++) {
                start[i] = startKey.getObj(i).isMinObj() || startKey.getObj(i).isMaxObj() ?
                        startKey.getObj(i) : startKey.getObj(i).getValue();
            }

            for (int i = 0; i < endKeySize; i++) {
                end[i] = endKey.getObj(i).isMinObj() || endKey.getObj(i).isMaxObj() ?
                        endKey.getObj(i) : endKey.getObj(i).getValue();
            }

            ObBorderFlag borderFlag = range.getBorderFlag();
            List<ObPair<Long, ObTableParam>> pairs = client.getTables(indexTableName,
                    tableQuery, start, borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd(),
                    false, false);

            if (tableQuery.getScanOrder() == ObScanOrder.Reverse) {
                for (int i = pairs.size() - 1; i >= 0; i--) {
                    partitionObTables.put(pairs.get(i).getLeft(), pairs.get(i));
                }
            } else {
                for (ObPair<Long, ObTableParam> pair : pairs) {
                    partitionObTables.put(pair.getLeft(), pair);
                }
            }
        }

        return partitionObTables;
    }

    protected void nextRow() {
        rowIndex = rowIndex + 1;
        row = cacheRows.poll();
        if (row != null) {
            currentStartKey = row;
        }
    }

    protected void checkStatus() throws IllegalStateException {
        if (!initialized) {
            throw new IllegalStateException("table " + tableName
                                            + "query stream result is not initialized");
        }

        if (closed) {
            throw new IllegalStateException("table " + tableName + " query stream result is closed");
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

    protected ObTableQueryAsyncResult checkObTableQueryAsyncResult(Object result) {
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

    protected abstract ObPayload referToNewPartition(ObPair<Long, ObTableParam> partIdWithObTable)
                                                                                                  throws Exception;

    protected abstract ObTableQueryResult execute(ObPair<Long, ObTableParam> partIdWithObTable,
                                                  ObPayload streamRequest) throws Exception;

    protected abstract ObTableQueryAsyncResult executeAsync(ObPair<Long, ObTableParam> partIdWithObTable,
                                                            ObPayload streamRequest)
                                                                                    throws Exception;

    protected abstract Map<Long, ObPair<Long, ObTableParam>> refreshPartition(ObTableQuery tableQuery,
                                                                              String tableName)
                                                                                               throws Exception;

    protected void cacheResultRows(ObTableQueryResult tableQueryResult) {
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

    private void cacheResultRows(ObTableQueryAsyncResult tableQueryAsyncResult) {
        cacheRows.addAll(tableQueryAsyncResult.getAffectedEntity().getPropertiesRows());
        cacheProperties = tableQueryAsyncResult.getAffectedEntity().getPropertiesNames();
    }

    protected void cacheStreamNext(ObPair<Long, ObTableParam> partIdWithObTable,
                                   ObTableQueryAsyncResult tableQueryAsyncResult) {
        cacheResultRows(tableQueryAsyncResult);
        if (tableQueryAsyncResult.getAffectedEntity().isStream()
            && tableQueryAsyncResult.getAffectedEntity().isStreamNext()) {
            partitionLastResult.addLast(new ObPair<ObPair<Long, ObTableParam>, ObTableQueryResult>(
                partIdWithObTable, tableQueryAsyncResult.getAffectedEntity()));
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
            // query not support BatchSize
            throw new ObTableException(
                "simple query not support BatchSize, use executeAsync() instead, BatchSize:"
                        + tableQuery.getBatchSize());
        }
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
     * Get index table name.
     */
    public String getIndexTableName() {
        return indexTableName;
    }

    /*
     * Set index table name.
     */
    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
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

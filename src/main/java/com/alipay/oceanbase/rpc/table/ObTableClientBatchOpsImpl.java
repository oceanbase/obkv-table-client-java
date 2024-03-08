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
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.model.ObServerRoute;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.result.*;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.MonitorUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;
import static com.alipay.oceanbase.rpc.util.TraceUtil.formatTraceMessage;

public class ObTableClientBatchOpsImpl extends AbstractTableBatchOps {

    private static final Logger   logger                  = TableClientLoggerFactory
                                                              .getLogger(ObTableClientBatchOpsImpl.class);

    private final ObTableClient   obTableClient;

    private ExecutorService       executorService;
    private boolean               returningAffectedEntity = false;
    private ObTableBatchOperation batchOperation;

    /*
     * Ob table client batch ops impl.
     */
    public ObTableClientBatchOpsImpl(String tableName, ObTableClient obTableClient) {
        this.tableName = tableName;
        this.obTableClient = obTableClient;
        this.batchOperation = new ObTableBatchOperation();
    }

    /*
     * Ob table client batch ops impl.
     */
    public ObTableClientBatchOpsImpl(String tableName, ObTableBatchOperation batchOperation,
                                     ObTableClient obTableClient) {
        this.tableName = tableName;
        this.obTableClient = obTableClient;
        this.batchOperation = batchOperation;
    }

    /*
     * Get ob table batch operation.
     */
    @Override
    public ObTableBatchOperation getObTableBatchOperation() {
        return batchOperation;
    }

    /*
     * Get.
     */
    @Override
    public void get(Object[] rowkeys, String[] columns) {
        addObTableClientOperation(ObTableOperationType.GET, rowkeys, columns, null);
    }

    /*
     * Update.
     */
    @Override
    public void update(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.UPDATE, rowkeys, columns, values);
    }

    /*
     * Delete.
     */
    @Override
    public void delete(Object[] rowkeys) {
        addObTableClientOperation(ObTableOperationType.DEL, rowkeys, null, null);
    }

    /*
     * Insert.
     */
    @Override
    public void insert(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.INSERT, rowkeys, columns, values);
    }

    /*
     * Replace.
     */
    @Override
    public void replace(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.REPLACE, rowkeys, columns, values);
    }

    /*
     * Insert or update.
     */
    @Override
    public void insertOrUpdate(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.INSERT_OR_UPDATE, rowkeys, columns, values);
    }

    /*
     * Increment.
     */
    @Override
    public void increment(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        returningAffectedEntity = withResult;
        addObTableClientOperation(ObTableOperationType.INCREMENT, rowkeys, columns, values);
    }

    /*
     * Append.
     */
    @Override
    public void append(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        returningAffectedEntity = withResult;
        addObTableClientOperation(ObTableOperationType.APPEND, rowkeys, columns, values);
    }

    /*
     * Put.
     */
    @Override
    public void put(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.PUT, rowkeys, columns, values);
    }

    private void addObTableClientOperation(ObTableOperationType type, Object[] rowkeys,
                                           String[] columns, Object[] values) {
        ObTableOperation instance = ObTableOperation.getInstance(type, rowkeys, columns, values);
        batchOperation.addTableOperation((instance));
    }

    /*
     * Execute.
     */
    public List<Object> execute() throws Exception {
        // consistent can not be sure
        List<Object> results = new ArrayList<Object>(batchOperation.getTableOperations().size());
        for (ObTableOperationResult result : executeInternal().getResults()) {
            int errCode = result.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                switch (result.getOperationType()) {
                    case GET:
                    case INCREMENT:
                    case APPEND:
                        results.add(result.getEntity().getSimpleProperties());
                        break;
                    default:
                        results.add(result.getAffectedRows());
                }
            } else {
                results.add(ExceptionUtil.convertToObTableException(result.getExecuteHost(),
                    result.getExecutePort(), result.getSequence(), result.getUniqueId(), errCode,
                    result.getHeader().getErrMsg()));
            }
        }
        return results;
    }

    /*
     * Execute with result
     */
    public List<Object> executeWithResult() throws Exception {
        // consistent can not be sure
        List<Object> results = new ArrayList<Object>(batchOperation.getTableOperations().size());
        for (ObTableOperationResult result : executeInternal().getResults()) {
            int errCode = result.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                switch (result.getOperationType()) {
                    case GET:
                    case INSERT:
                    case DEL:
                    case UPDATE:
                    case INSERT_OR_UPDATE:
                    case REPLACE:
                    case INCREMENT:
                    case APPEND:
                    case PUT:
                        results.add(new MutationResult(result));
                        break;
                    default:
                        throw new ObTableException("unknown operation type "
                                                   + result.getOperationType());
                }
            } else {
                results.add(ExceptionUtil.convertToObTableException(result.getExecuteHost(),
                    result.getExecutePort(), result.getSequence(), result.getUniqueId(), errCode,
                    result.getHeader().getErrMsg()));
            }
        }
        return results;
    }

    public Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> partitionPrepare()
                                                                                                      throws Exception {
        // consistent can not be sure
        List<ObTableOperation> operations = batchOperation.getTableOperations();
        Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> partitionOperationsMap = new HashMap<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>>();

        if (obTableClient.isOdpMode()) {
            ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>> obTableOperations = new ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>(
                new ObTableParam(obTableClient.getOdpTable()),
                new ArrayList<ObPair<Integer, ObTableOperation>>());
            for (int i = 0; i < operations.size(); i++) {
                ObTableOperation operation = operations.get(i);
                obTableOperations.getRight().add(
                    new ObPair<Integer, ObTableOperation>(i, operation));
            }
            partitionOperationsMap.put(0L, obTableOperations);
            return partitionOperationsMap;
        }

        for (int i = 0; i < operations.size(); i++) {
            ObTableOperation operation = operations.get(i);
            ObRowKey rowKeyObject = operation.getEntity().getRowKey();
            int rowKeySize = rowKeyObject.getObjs().size();
            Object[] rowKey = new Object[rowKeySize];
            for (int j = 0; j < rowKeySize; j++) {
                rowKey[j] = rowKeyObject.getObj(j).getValue();
            }
            ObPair<Long, ObTableParam> tableObPair = obTableClient.getTable(tableName, rowKey,
                false, false, obTableClient.getRoute(batchOperation.isReadOnly()));
            ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>> obTableOperations = partitionOperationsMap
                .get(tableObPair.getLeft());
            if (obTableOperations == null) {
                obTableOperations = new ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>(
                    tableObPair.getRight(), new ArrayList<ObPair<Integer, ObTableOperation>>());
                partitionOperationsMap.put(tableObPair.getLeft(), obTableOperations);
            }
            obTableOperations.getRight().add(new ObPair<Integer, ObTableOperation>(i, operation));
        }

        return partitionOperationsMap;
    }

    /*
     * Partition execute.
     */
    public void partitionExecute(ObTableOperationResult[] results,
                                 Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> partitionOperation)
                                                                                                                                   throws Exception {
        ObTableParam tableParam = partitionOperation.getValue().getLeft();
        long tableId = tableParam.getTableId();
        long partId = tableParam.getPartitionId();
        long originPartId = tableParam.getPartId();
        ObTable subObTable = tableParam.getObTable();
        List<ObPair<Integer, ObTableOperation>> subOperationWithIndexList = partitionOperation
            .getValue().getRight();

        ObTableBatchOperationRequest subRequest = new ObTableBatchOperationRequest();
        ObTableBatchOperation subOperations = new ObTableBatchOperation();
        for (ObPair<Integer, ObTableOperation> operationWithIndex : subOperationWithIndexList) {
            subOperations.addTableOperation(operationWithIndex.getRight());
        }
        subOperations.setSameType(batchOperation.isSameType());
        subOperations.setReadOnly(batchOperation.isReadOnly());
        subOperations.setSamePropertiesNames(batchOperation.isSamePropertiesNames());
        subRequest.setBatchOperation(subOperations);
        subRequest.setTableName(tableName);
        subRequest.setReturningAffectedEntity(returningAffectedEntity);
        subRequest.setReturningAffectedRows(true);
        subRequest.setTableId(tableId);
        subRequest.setPartitionId(partId);
        subRequest.setEntityType(entityType);
        subRequest.setTimeout(subObTable.getObTableOperationTimeout());
        if (batchOperation.isReadOnly()) {
            subRequest.setConsistencyLevel(obTableClient.getReadConsistency()
                .toObTableConsistencyLevel());
        }
        subRequest.setBatchOperationAsAtomic(isAtomicOperation());
        subRequest.setBatchOpReturnOneResult(isReturnOneResult());
        ObTableBatchOperationResult subObTableBatchOperationResult;

        boolean needRefreshTableEntry = false;
        int tryTimes = 0;
        long startExecute = System.currentTimeMillis();
        Set<String> failedServerList = null;
        ObServerRoute route = null;

        while (true) {
            obTableClient.checkStatus();
            long currentExecute = System.currentTimeMillis();
            long costMillis = currentExecute - startExecute;
            if (costMillis > obTableClient.getRuntimeMaxWait()) {
                logger.error(
                    "tablename:{} partition id:{} it has tried " + tryTimes
                            + " times and it has waited " + costMillis
                            + "/ms which exceeds response timeout "
                            + obTableClient.getRuntimeMaxWait() + "/ms", tableName, partId);
                throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                                                 + " times and it has waited " + costMillis
                                                 + "/ms which exceeds response timeout "
                                                 + obTableClient.getRuntimeMaxWait() + "/ms");
            }
            tryTimes++;
            try {
                if (obTableClient.isOdpMode()) {
                    subObTable = obTableClient.getOdpTable();
                } else {
                    // getTable() when we need retry
                    // we should use partIdx to get table
                    if (tryTimes > 1) {
                        if (route == null) {
                            route = obTableClient.getRoute(batchOperation.isReadOnly());
                        }
                        if (failedServerList != null) {
                            route.setBlackList(failedServerList);
                        }
                        subObTable = obTableClient
                            .getTable(tableName, originPartId, needRefreshTableEntry,
                                obTableClient.isTableEntryRefreshIntervalWait(), route).getRight()
                            .getObTable();
                    }
                }
                subObTableBatchOperationResult = (ObTableBatchOperationResult) subObTable
                    .execute(subRequest);
                obTableClient.resetExecuteContinuousFailureCount(tableName);
                break;
            } catch (Exception ex) {
                if (obTableClient.isOdpMode()) {
                    if ((tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger
                            .warn(
                                "batch ops execute while meet Exception, tablename:{}, errorCode: {} , errorMsg: {}, try times {}",
                                tableName, ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                tryTimes);
                    } else {
                        throw ex;
                    }
                } else if (ex instanceof ObTableReplicaNotReadableException) {
                    if ((tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger.warn(
                            "tablename:{} partition id:{} retry when replica not readable: {}",
                            tableName, partId, ex.getMessage());
                        if (failedServerList == null) {
                            failedServerList = new HashSet<String>();
                        }
                        failedServerList.add(subObTable.getIp());
                    } else {
                        logger.warn("exhaust retry when replica not readable: {}", ex.getMessage());
                        throw ex;
                    }
                } else if (ex instanceof ObTableException
                           && ((ObTableException) ex).isNeedRefreshTableEntry()) {
                    needRefreshTableEntry = true;
                    logger
                        .warn(
                            "tablename:{} partition id:{} batch ops refresh table while meet ObTableMasterChangeException, errorCode: {}",
                            tableName, partId, ((ObTableException) ex).getErrorCode(), ex);
                    if (obTableClient.isRetryOnChangeMasterTimes()
                        && (tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger
                            .warn(
                                "tablename:{} partition id:{} batch ops retry while meet ObTableMasterChangeException, errorCode: {} , retry times {}",
                                tableName, partId, ((ObTableException) ex).getErrorCode(),
                                tryTimes, ex);
                    } else {
                        obTableClient.calculateContinuousFailure(tableName, ex.getMessage());
                        throw ex;
                    }
                } else {
                    obTableClient.calculateContinuousFailure(tableName, ex.getMessage());
                    throw ex;
                }
            }
            Thread.sleep(obTableClient.getRuntimeRetryInterval());
        }

        long endExecute = System.currentTimeMillis();

        if (subObTableBatchOperationResult == null) {
            RUNTIME
                .error(
                    "tablename:{} partition id:{} check batch operation result error: client get unexpected NULL result",
                    tableName, partId);
            throw new ObTableUnexpectedException(
                "check batch operation result error: client get unexpected NULL result");
        }

        List<ObTableOperationResult> subObTableOperationResults = subObTableBatchOperationResult
            .getResults();

        if (subObTableOperationResults.size() < subOperations.getTableOperations().size()) {
            // only one result when it across failed
            // only one result when hkv puts
            if (subObTableOperationResults.size() == 1) {
                ObTableOperationResult subObTableOperationResult = subObTableOperationResults
                    .get(0);
                subObTableOperationResult.setExecuteHost(subObTable.getIp());
                subObTableOperationResult.setExecutePort(subObTable.getPort());
                for (ObPair<Integer, ObTableOperation> aSubOperationWithIndexList : subOperationWithIndexList) {
                    results[aSubOperationWithIndexList.getLeft()] = subObTableOperationResult;
                }
            } else {
                // unexpected result found
                throw new IllegalArgumentException(
                    "check batch operation result size error: operation size ["
                            + subOperations.getTableOperations().size() + "] result size ["
                            + subObTableOperationResults.size() + "]");
            }
        } else {
            if (subOperationWithIndexList.size() != subObTableOperationResults.size()) {
                throw new ObTableUnexpectedException("check batch result error: partition "
                                                     + partId + " expect result size "
                                                     + subOperationWithIndexList.size()
                                                     + " actual result size "
                                                     + subObTableOperationResults.size());
            }
            for (int i = 0; i < subOperationWithIndexList.size(); i++) {
                ObTableOperationResult subObTableOperationResult = subObTableOperationResults
                    .get(i);
                subObTableOperationResult.setExecuteHost(subObTable.getIp());
                subObTableOperationResult.setExecutePort(subObTable.getPort());
                results[subOperationWithIndexList.get(i).getLeft()] = subObTableOperationResult;
            }
        }
        String endpoint = subObTable.getIp() + ":" + subObTable.getPort();
        MonitorUtil.info(subRequest, subObTable.getDatabase(), tableName,
            "BATCH-partitionExecute-", endpoint, subOperations, partId,
            subObTableOperationResults.size(), endExecute - startExecute,
            obTableClient.getslowQueryMonitorThreshold());
    }

    /*
     * Execute internal.
     */
    public ObTableBatchOperationResult executeInternal() throws Exception {

        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        long start = System.currentTimeMillis();
        List<ObTableOperation> operations = batchOperation.getTableOperations();
        final ObTableOperationResult[] obTableOperationResults = new ObTableOperationResult[operations
            .size()];
        Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> partitions = partitionPrepare();
        long getTableTime = System.currentTimeMillis();
        final Map<Object, Object> context = ThreadLocalMap.getContextMap();
        if (executorService != null && !executorService.isShutdown() && partitions.size() > 1) {
            final ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(executorService,
                partitions.size());
            for (final Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> entry : partitions
                .entrySet()) {
                executor.execute(new ConcurrentTask() {
                    /*
                     * Do task.
                     */
                    @Override
                    public void doTask() {
                        try {
                            ThreadLocalMap.transmitContextMap(context);
                            partitionExecute(obTableOperationResults, entry);
                        } catch (Exception e) {
                            logger.error(LCD.convert("01-00026"), e);
                            executor.collectExceptions(e);
                        } finally {
                            ThreadLocalMap.reset();
                        }
                    }
                });
            }
            long estimate = obTableClient.getRuntimeBatchMaxWait() * 1000L * 1000L;
            try {
                while (estimate > 0) {
                    long nanos = System.nanoTime();
                    try {
                        executor.waitComplete(1, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new ObTableUnexpectedException(
                            "Batch Concurrent Execute interrupted", e);
                    }

                    if (executor.getThrowableList().size() > 0) {
                        throw new ObTableUnexpectedException("Batch Concurrent Execute Error",
                            executor.getThrowableList().get(0));
                    }

                    if (executor.isComplete()) {
                        break;
                    }

                    estimate = estimate - (System.nanoTime() - nanos);
                }
            } finally {
                executor.stop();
            }

            if (executor.getThrowableList().size() > 0) {
                throw new ObTableUnexpectedException("Batch Concurrent Execute Error", executor
                    .getThrowableList().get(0));
            }

            if (!executor.isComplete()) {
                throw new ObTableUnexpectedException("Batch Concurrent Execute Error ["
                                                     + obTableClient.getRpcExecuteTimeout()
                                                     + "]/ms");
            }

        } else {
            for (final Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> entry : partitions
                .entrySet()) {
                partitionExecute(obTableOperationResults, entry);
            }
        }

        ObTableBatchOperationResult batchOperationResult = new ObTableBatchOperationResult();
        for (ObTableOperationResult obTableOperationResult : obTableOperationResults) {
            batchOperationResult.addResult(obTableOperationResult);
        }

        MonitorUtil.info(batchOperationResult, obTableClient.getDatabase(), tableName, "BATCH", "",
            obTableOperationResults.length, getTableTime - start, System.currentTimeMillis()
                                                                  - getTableTime,
            obTableClient.getslowQueryMonitorThreshold());

        return batchOperationResult;
    }

    /*
     * clear batch operations1
     */
    public void clear() {
        batchOperation = new ObTableBatchOperation();
    }

    /*
     * Set executor service.
     */
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public boolean isReturningAffectedEntity() {
        return returningAffectedEntity;
    }

    public void setReturningAffectedEntity(boolean returningAffectedEntity) {
        this.returningAffectedEntity = returningAffectedEntity;
    }
}

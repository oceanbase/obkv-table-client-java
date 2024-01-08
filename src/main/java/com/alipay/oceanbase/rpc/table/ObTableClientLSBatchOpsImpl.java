/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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
import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.model.ObServerRoute;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.Mutation;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutate;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.MonitorUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;

public class ObTableClientLSBatchOpsImpl extends AbstractTableBatchOps {

    private static final Logger   logger                  = TableClientLoggerFactory
                                                              .getLogger(ObTableClientBatchOpsImpl.class);
    private final ObTableClient   obTableClient;
    private ExecutorService       executorService;
    private boolean               returningAffectedEntity = false;
    private List<ObTableSingleOp> batchOperation;

    /*
     * Ob table client batch ops impl.
     */
    public ObTableClientLSBatchOpsImpl(String tableName, ObTableClient obTableClient) {
        this.tableName = tableName;
        this.obTableClient = obTableClient;
        this.executorService = obTableClient.getRuntimeBatchExecutor();
        this.batchOperation = new ArrayList<>();
    }

    /*
     * Get ob table batch operation.
     */
    @Override
    public ObTableBatchOperation getObTableBatchOperation() {
        return null;
    }

    public List<ObTableSingleOp> getSingleOperations() {
        return batchOperation;
    }

    /*
     * Get.
     */
    @Override
    public void get(Object[] rowkeys, String[] columns) {
        throw new FeatureNotSupportedException();
    }

    /*
     * Update.
     */
    @Override
    public void update(Object[] rowkeys, String[] columns, Object[] values) {
        throw new FeatureNotSupportedException();
    }

    /*
     * Delete.
     */
    @Override
    public void delete(Object[] rowkeys) {
        throw new FeatureNotSupportedException();
    }

    /*
     * Insert.
     */
    @Override
    public void insert(Object[] rowkeys, String[] columns, Object[] values) {
        throw new FeatureNotSupportedException();
    }

    /*
     * Replace.
     */
    @Override
    public void replace(Object[] rowkeys, String[] columns, Object[] values) {
        throw new FeatureNotSupportedException();
    }

    /*
     * Insert or update.
     */
    @Override
    public void insertOrUpdate(Object[] rowkeys, String[] columns, Object[] values) {
        throw new FeatureNotSupportedException();
    }

    /*
     * Increment.
     */
    @Override
    public void increment(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        throw new FeatureNotSupportedException();
    }

    /*
     * Append.
     */
    @Override
    public void append(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        throw new FeatureNotSupportedException();
    }

    private void addOperation(ObTableSingleOp singleOp) {
        batchOperation.add(singleOp);
    }

    public void addOperation(CheckAndInsUp checkAndInsUp) {
        InsertOrUpdate insUp = checkAndInsUp.getInsUp();
        ObTableSingleOp singleOp = new ObTableSingleOp();

        ObTableOperation operation = ObTableOperation.getInstance(
            ObTableOperationType.INSERT_OR_UPDATE, insUp.getRowKey(), insUp.getColumns(),
            insUp.getValues());
        ObTableBatchOperation operations = new ObTableBatchOperation();
        operations.addTableOperation(operation);
        singleOp.setMutations(operations);

        ObTableQuery tableQuery = new ObTableQuery();
        ObNewRange range = new ObNewRange();
        range.setStartKey(ObRowKey.getInstance(insUp.getRowKey()));
        range.setEndKey(ObRowKey.getInstance(insUp.getRowKey()));
        tableQuery.addKeyRange(range);
        tableQuery.setFilterString(checkAndInsUp.getFilter().toString());
        singleOp.setTableQuery(tableQuery);
        singleOp.setIsCheckAndExecute(true);
        singleOp.setIsCheckNoExists(!checkAndInsUp.isCheckExists());
        singleOp.setSingleOpType(ObTableSingleOpType.QUERY_AND_MUTATE);
        addOperation(singleOp);
    }

    /*
     * Execute.
     */
    public List<Object> execute() throws Exception {
        List<Object> results = new ArrayList(batchOperation.size());
        for (ObTableOperationResult result : executeInternal().getResults()) {
            int errCode = result.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                results.add(result.getAffectedRows());
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
        List<Object> results = new ArrayList<Object>(batchOperation.size());
        for (ObTableOperationResult result : executeInternal().getResults()) {
            int errCode = result.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                results.add(new MutationResult(result));
            } else {
                results.add(ExceptionUtil.convertToObTableException(result.getExecuteHost(),
                    result.getExecutePort(), result.getSequence(), result.getUniqueId(), errCode,
                    result.getHeader().getErrMsg()));
            }
        }
        return results;
    }

    public Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> partitionPrepare()
            throws Exception {
        // TODO: currently, we only support tablet level operation aggregation
        List<ObTableSingleOp> operations = getSingleOperations();
        // map: <tablet_id, <idx in origin batch, table operation>>
        Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> partitionOperationsMap =
                new HashMap();

        // In ODP mode, client send the request to ODP directly without route
        if (obTableClient.isOdpMode()) {
            ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>> obTableOperations =
                    new ObPair(new ObTableParam(obTableClient.getOdpTable()),
                            new ArrayList<ObPair<Integer, ObTableSingleOp>>());
            for (int i = 0; i < operations.size(); i++) {
                ObTableSingleOp operation = operations.get(i);
                obTableOperations.getRight().add(new ObPair<Integer, ObTableSingleOp>(i, operation));
            }
            partitionOperationsMap.put(0L, obTableOperations);
            return partitionOperationsMap;
        }

        for (int i = 0; i < operations.size(); i++) {
            ObTableSingleOp operation = operations.get(i);
            ObRowKey rowKeyObject = operation.getTableQuery().getKeyRanges().get(0).getStartKey();
            int rowKeySize = rowKeyObject.getObjs().size();
            Object[] rowKey = new Object[rowKeySize];
            for (int j = 0; j < rowKeySize; j++) {
                rowKey[j] = rowKeyObject.getObj(j).getValue();
            }
            ObPair<Long, ObTableParam>  tableObPair= obTableClient.getTable(tableName, rowKey,
                    false, false, obTableClient.getRoute(false));
            ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>> obTableOperations =
                    partitionOperationsMap.get(tableObPair.getLeft());
            if (obTableOperations == null) {
                obTableOperations = new ObPair<>(tableObPair.getRight(), new ArrayList<>());
                partitionOperationsMap.put(tableObPair.getLeft(), obTableOperations);
            }
            obTableOperations.getRight().add(new ObPair(i, operation));
        }

        if (atomicOperation) {
            if (partitionOperationsMap.size() > 1) {
                throw new ObTablePartitionConsistentException(
                        "require atomic operation but found across partition may cause consistent problem ");
            }
        }
        return partitionOperationsMap;
    }

    /*
     * Partition execute.
     */
    public void partitionExecute(ObTableOperationResult[] results,
                                 Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> partitionOperation)
            throws Exception {
        ObTableParam tableParam = partitionOperation.getValue().getLeft();
        long tableId = tableParam.getTableId();
        long partId = tableParam.getPartitionId();
        ObTable subObTable = tableParam.getObTable();
        List<ObPair<Integer, ObTableSingleOp>> subOperationWithIndexList = partitionOperation
                .getValue().getRight();

        ObTableLSOpRequest subRequest = new ObTableLSOpRequest();
        List<ObTableSingleOp> subOperations = new ArrayList<>();
        for (ObPair<Integer, ObTableSingleOp> operationWithIndex : subOperationWithIndexList) {
            subOperations.add(operationWithIndex.getRight());
        }
        ObTableTabletOp tabletOp = new ObTableTabletOp();
        tabletOp.setSingleOperations(subOperations);
        tabletOp.setTableId(tableId);
        tabletOp.setTabletId(partId);
        subRequest.addTabletOperation(tabletOp);
        subRequest.setEntityType(entityType);
        subRequest.setTimeout(subObTable.getObTableOperationTimeout());

        ObTableLSOpResult subLSOpResult;
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
                logger.error("table name: {} partition id:{} it has tried " + tryTimes
                                + " times and it has waited " + costMillis + " ms"
                                + " which exceeds runtime max wait timeout "
                                + obTableClient.getRuntimeMaxWait() + " ms", tableName, partId);
                throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                        + " times and it has waited " + costMillis
                        + "ms which exceeds runtime max wait timeout "
                        + obTableClient.getRuntimeMaxWait() + "ms");
            }
            tryTimes++;
            try {
                if (obTableClient.isOdpMode()) {
                    subObTable = obTableClient.getOdpTable();
                } else {
                    if (tryTimes > 1) {
                        if (route == null) {
                            route = obTableClient.getRoute(false);
                        }
                        if (failedServerList != null) {
                            route.setBlackList(failedServerList);
                        }
                        subObTable = obTableClient.getTable(tableName, partId, needRefreshTableEntry,
                                        obTableClient.isTableEntryRefreshIntervalWait(), route).
                                            getRight().getObTable();
                    }
                }
                subLSOpResult = (ObTableLSOpResult) subObTable.execute(subRequest);
                obTableClient.resetExecuteContinuousFailureCount(tableName);
                break;
            } catch (Exception ex) {
                if (obTableClient.isOdpMode()) {
                    if ((tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger.warn("batch ops execute while meet Exception, tablename:{}, errorCode: {} , errorMsg: {}, try times {}",
                                     tableName, ((ObTableException) ex).getErrorCode(), ex.getMessage(), tryTimes);
                    } else {
                        throw ex;
                    }
                } else if (ex instanceof ObTableReplicaNotReadableException) {
                    if ((tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger.warn("tablename:{} partition id:{} retry when replica not readable: {}",
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
                    logger.warn("tablename:{} partition id:{} batch ops refresh table while meet ObTableMasterChangeException, errorCode: {}",
                                 tableName, partId, ((ObTableException) ex).getErrorCode(), ex);
                    if (obTableClient.isRetryOnChangeMasterTimes()
                            && (tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger.warn("tablename:{} partition id:{} batch ops retry while meet ObTableMasterChangeException, errorCode: {} , retry times {}",
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

        if (subLSOpResult == null) {
            RUNTIME.error("tablename:{} partition id:{} check batch operation result error: client get unexpected NULL result",
                           tableName, partId);
            throw new ObTableUnexpectedException("check batch operation result error: client get unexpected NULL result");
        }

        List<ObTableTabletOpResult> tabletOpResults = subLSOpResult.getResults();
        if (tabletOpResults.size() != 1) {
            throw new ObTableUnexpectedException("check batch result error: partition "
                    + partId + " expect tablet op result size 1"
                    + " actual result size is "
                    + tabletOpResults.size());
        }

        List<ObTableOperationResult> subObTableOperationResults = tabletOpResults.get(0).getResults();

        if (subObTableOperationResults.size() < subOperations.size()) {
            // only one result when it across failed
            // only one result when hkv puts
            if (subObTableOperationResults.size() == 1 && entityType == ObTableEntityType.HKV) {
                ObTableOperationResult subObTableOperationResult = subObTableOperationResults.get(0);
                subObTableOperationResult.setExecuteHost(subObTable.getIp());
                subObTableOperationResult.setExecutePort(subObTable.getPort());
                for (ObPair<Integer, ObTableSingleOp> SubOperationWithIndexList : subOperationWithIndexList) {
                    results[SubOperationWithIndexList.getLeft()] = subObTableOperationResult;
                }
            } else {
                throw new IllegalArgumentException(
                        "check batch operation result size error: operation size ["
                                + subOperations.size() + "] result size ["
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
                ObTableOperationResult subObTableOperationResult = subObTableOperationResults.get(i);
                subObTableOperationResult.setExecuteHost(subObTable.getIp());
                subObTableOperationResult.setExecutePort(subObTable.getPort());
                results[subOperationWithIndexList.get(i).getLeft()] = subObTableOperationResult;
            }
        }
        String endpoint = subObTable.getIp() + ":" + subObTable.getPort();
        MonitorUtil.info(subRequest, subObTable.getDatabase(), tableName,
                "BATCH-partitionExecute-", endpoint, tabletOp,
                subObTableOperationResults.size(), endExecute - startExecute,
                obTableClient.getslowQueryMonitorThreshold());
    }

    /*
     * Execute internal.
     */
    public ObTableBatchOperationResult executeInternal() throws Exception {

        long start = System.currentTimeMillis();
        final ObTableOperationResult[] obTableOperationResults = new ObTableOperationResult[batchOperation
            .size()];
        Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> partitions = partitionPrepare();
        long getTableTime = System.currentTimeMillis();
        final Map<Object, Object> context = ThreadLocalMap.getContextMap();
        if (executorService != null && !executorService.isShutdown() && partitions.size() > 1) {
            // execute sub-batch operation in parallel
            final ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(executorService,
                partitions.size());
            for (final Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> entry : partitions
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
            long timeoutTs = obTableClient.getRuntimeBatchMaxWait() * 1000L * 1000L
                             + System.nanoTime();
            try {
                while (timeoutTs > System.nanoTime()) {
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
                }
            } finally {
                executor.stop();
            }

            if (executor.getThrowableList().size() > 0) {
                throw new ObTableUnexpectedException("Batch Concurrent Execute Error", executor
                    .getThrowableList().get(0));
            }

            if (!executor.isComplete()) {
                throw new ObTableUnexpectedException(
                    "Batch Concurrent Execute Error, runtimeBatchMaxWait: "
                            + obTableClient.getRuntimeBatchMaxWait() + "ms");
            }

        } else {
            // Execute sub-batch operation one by one
            for (final Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> entry : partitions
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
        batchOperation = new ArrayList<>();
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

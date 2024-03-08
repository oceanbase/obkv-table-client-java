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
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.MonitorUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;
import static com.alipay.oceanbase.rpc.protocol.payload.Constants.*;

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

    /*
     * Put.
     */
    @Override
    public void put(Object[] rowkeys, String[] columns, Object[] values) {
        throw new FeatureNotSupportedException();
    }

    private void addOperation(ObTableSingleOp singleOp) {
        batchOperation.add(singleOp);
    }

    public void addOperation(CheckAndInsUp checkAndInsUp) {
        InsertOrUpdate insUp = checkAndInsUp.getInsUp();

        ObTableSingleOpQuery query = new ObTableSingleOpQuery();
        ObNewRange range = new ObNewRange();
        range.setStartKey(ObRowKey.getInstance(insUp.getRowKey()));
        range.setEndKey(ObRowKey.getInstance(insUp.getRowKey()));
        query.addScanRangeColumns(insUp.getRowKeyNames());
        query.addScanRange(range);
        query.setFilterString(checkAndInsUp.getFilter().toString());

        String[] rowKeyNames = checkAndInsUp.getInsUp().getRowKeyNames().toArray(new String[0]);
        Object[] rowKey = checkAndInsUp.getInsUp().getRowKey();
        String[] propertiesNames = checkAndInsUp.getInsUp().getColumns();
        Object[] propertiesValues = checkAndInsUp.getInsUp().getValues();
        ObTableSingleOpEntity entity = ObTableSingleOpEntity.getInstance(rowKeyNames, rowKey,
            propertiesNames, propertiesValues);

        ObTableSingleOp singleOp = new ObTableSingleOp();
        singleOp.setSingleOpType(ObTableOperationType.CHECK_AND_INSERT_UP);
        singleOp.setIsCheckNoExists(!checkAndInsUp.isCheckExists());
        singleOp.setQuery(query);
        singleOp.addEntity(entity);

        addOperation(singleOp);
    }

    public void addOperation(TableQuery query) throws Exception {
        // entity
        String[] rowKeyNames = query.getRowKey().getColumns();
        if (rowKeyNames == null || rowKeyNames.length == 0) {
            throw new IllegalArgumentException("rowKey is empty in get op");
        }
        Object[] rowKey = query.getRowKey().getValues();
        String[] propertiesNames = query.getSelectColumns().toArray(new String[0]);
        ObTableSingleOpEntity entity = ObTableSingleOpEntity.getInstance(rowKeyNames, rowKey,
            propertiesNames, null);

        ObTableSingleOp singleOp = new ObTableSingleOp();
        singleOp.setSingleOpType(ObTableOperationType.GET);
        singleOp.addEntity(entity);
        addOperation(singleOp);
    }

    public void addOperation(Mutation mutation) throws Exception {
        // entity
        String[] rowKeyNames = null;
        Object[] rowKey = null;
        String[] propertiesNames = null;
        Object[] propertiesValues = null;

        ObTableOperationType type = mutation.getOperationType();
        switch (type) {
            case GET:
                throw new IllegalArgumentException("Invalid type in batch operation, " + type);
            case INSERT:
                ((Insert) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Insert) mutation).getRowKeyNames().toArray(new String[0]);
                rowKey = mutation.getRowKey();
                propertiesNames = ((Insert) mutation).getColumns();
                propertiesValues = ((Insert) mutation).getValues();
                break;
            case DEL:
                rowKeyNames = ((Delete) mutation).getRowKeyNames().toArray(new String[0]);
                rowKey = mutation.getRowKey();
                break;
            case UPDATE:
                ((Update) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Update) mutation).getRowKeyNames().toArray(new String[0]);
                rowKey = mutation.getRowKey();
                propertiesNames = ((Update) mutation).getColumns();
                propertiesValues = ((Update) mutation).getValues();
                break;
            case INSERT_OR_UPDATE:
                ((InsertOrUpdate) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((InsertOrUpdate) mutation).getRowKeyNames().toArray(new String[0]);
                rowKey = mutation.getRowKey();
                propertiesNames = ((InsertOrUpdate) mutation).getColumns();
                propertiesValues = ((InsertOrUpdate) mutation).getValues();
                break;
            case REPLACE:
                ((Replace) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Replace) mutation).getRowKeyNames().toArray(new String[0]);
                rowKey = mutation.getRowKey();
                propertiesNames = ((Replace) mutation).getColumns();
                propertiesValues = ((Replace) mutation).getValues();
                break;
            case INCREMENT:
                ((Increment) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Increment) mutation).getRowKeyNames().toArray(new String[0]);
                rowKey = mutation.getRowKey();
                propertiesNames = ((Increment) mutation).getColumns();
                propertiesValues = ((Increment) mutation).getValues();
                break;
            case APPEND:
                ((Append) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Append) mutation).getRowKeyNames().toArray(new String[0]);
                rowKey = mutation.getRowKey();
                propertiesNames = ((Append) mutation).getColumns();
                propertiesValues = ((Append) mutation).getValues();
                break;
            case PUT:
                ((Put) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Put) mutation).getRowKeyNames().toArray(new String[0]);
                rowKey = mutation.getRowKey();
                propertiesNames = ((Put) mutation).getColumns();
                propertiesValues = ((Put) mutation).getValues();
                break;
            default:
                throw new ObTableException("unknown operation type " + type);
        }

        ObTableSingleOpEntity entity = ObTableSingleOpEntity.getInstance(rowKeyNames, rowKey,
            propertiesNames, propertiesValues);
        ObTableSingleOp singleOp = new ObTableSingleOp();
        singleOp.setSingleOpType(type);
        singleOp.addEntity(entity);
        addOperation(singleOp);
    }

    /*
     * Execute.
     */
    public List<Object> execute() throws Exception {
        List<Object> results = new ArrayList(batchOperation.size());
        for (ObTableSingleOpResult result : executeInternal()) {
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
        for (ObTableSingleOpResult result : executeInternal()) {
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

    public Map<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> partitionPrepare()
            throws Exception {
        List<ObTableSingleOp> operations = getSingleOperations();
        // map: <ls_id, map<tablet_id, <table param, <idx in origin batch, table operation>>>>
        Map<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> lsOperationsMap =
                new HashMap();

        // In ODP mode, client send the request to ODP directly without route
        if (obTableClient.isOdpMode()) {
            Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> tabletOperationsMap = new HashMap<>();
            ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>> obTableOperations =
                    new ObPair(new ObTableParam(obTableClient.getOdpTable()),
                            new ArrayList<ObPair<Integer, ObTableSingleOp>>());
            for (int i = 0; i < operations.size(); i++) {
                ObTableSingleOp operation = operations.get(i);
                obTableOperations.getRight().add(new ObPair<Integer, ObTableSingleOp>(i, operation));
            }
            tabletOperationsMap.put(INVALID_TABLET_ID, obTableOperations);
            lsOperationsMap.put(INVALID_LS_ID, tabletOperationsMap);
            return lsOperationsMap;
        }

        for (int i = 0; i < operations.size(); i++) {
            ObTableSingleOp operation = operations.get(i);
            List<ObObj> rowkeyObjs = operation.getRowkeyObjs();
            int rowKeySize = rowkeyObjs.size();
            Object[] rowKey = new Object[rowKeySize];
            for (int j = 0; j < rowKeySize; j++) {
                rowKey[j] = rowkeyObjs.get(j).getValue();
            }
            ObPair<Long, ObTableParam>  tableObPair= obTableClient.getTable(tableName, rowKey,
                    false, false, obTableClient.getRoute(false));
            long lsId = tableObPair.getRight().getLsId();

            Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> tabletOperations
                    = lsOperationsMap.get(lsId);
            // if ls id not exists
            if (tabletOperations == null) {
               tabletOperations = new HashMap<>();
               lsOperationsMap.put(lsId, tabletOperations);
            }

            ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>> singleOperations =
                    tabletOperations.get(tableObPair.getLeft());
            // if tablet id not exists
            if (singleOperations == null) {
                singleOperations = new ObPair<>(tableObPair.getRight(), new ArrayList<>());
                tabletOperations.put(tableObPair.getLeft(), singleOperations);
            }

            singleOperations.getRight().add(new ObPair(i, operation));
        }

        return lsOperationsMap;
    }

    /*
     * Partition execute.
     */
    public void partitionExecute(ObTableSingleOpResult[] results,
                                 Map.Entry<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> lsOperation)
            throws Exception {
        long lsId = lsOperation.getKey();
        Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> tabletOperationsMap = lsOperation.getValue();
        if (tabletOperationsMap.size() == 0) {
            logger.warn("the size of tablet operations in ls operation is zero");
            throw new ObTableUnexpectedException("the size of tablet operations in ls operation is zero");
        }

        ObTableLSOpRequest tableLsOpRequest = new ObTableLSOpRequest();
        ObTableLSOperation tableLsOp = new ObTableLSOperation();
        tableLsOp.setLsId(lsId);
        tableLsOp.setReturnOneResult(returnOneResult);
        // fetch the following parameters in first entry for routing
        long tableId = 0;
        long originPartId = 0;
        long operationTimeout = 0;
        ObTable subObTable = null;

        boolean isFirstEntry = true;
        // list ( index list for tablet op 1, index list for tablet op 2, ...)
        List<List<ObPair<Integer, ObTableSingleOp>>> lsOperationWithIndexList = new ArrayList<>();
        for (final Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> tabletOperation : tabletOperationsMap.entrySet()) {
            ObTableParam tableParam = tabletOperation.getValue().getLeft();
            long tabletId = tableParam.getPartitionId();
            List<ObPair<Integer, ObTableSingleOp>> tabletOperationWithIndexList = tabletOperation.getValue().getRight();
            lsOperationWithIndexList.add(tabletOperationWithIndexList);
            List<ObTableSingleOp> singleOps = new ArrayList<>();
            for (ObPair<Integer, ObTableSingleOp> operationWithIndex : tabletOperationWithIndexList) {
                singleOps.add(operationWithIndex.getRight());
            }
            ObTableTabletOp tableTabletOp = new ObTableTabletOp();
            tableTabletOp.setSingleOperations(singleOps);
            tableTabletOp.setTabletId(tabletId);

            tableLsOp.addTabletOperation(tableTabletOp);

            if (isFirstEntry) {
                tableId = tableParam.getTableId();
                originPartId = tableParam.getPartId();
                operationTimeout = tableParam.getObTable().getObTableOperationTimeout();
                subObTable = tableParam.getObTable();
                isFirstEntry = false;
            }
        }

        // Since we only have one tablet operation
        // We do the LS operation prepare here
       tableLsOp.prepare();

       tableLsOpRequest.setLsOperation(tableLsOp);
       tableLsOpRequest.setTableId(tableId);
       tableLsOpRequest.setEntityType(entityType);
       tableLsOpRequest.setTimeout(operationTimeout);

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
                logger.error("table name: {} ls id:{} it has tried " + tryTimes
                                + " times and it has waited " + costMillis + " ms"
                                + " which exceeds runtime max wait timeout "
                                + obTableClient.getRuntimeMaxWait() + " ms", tableName, lsId);
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
                        subObTable = obTableClient.getTable(tableName, originPartId, needRefreshTableEntry,
                                        obTableClient.isTableEntryRefreshIntervalWait(), route).
                                            getRight().getObTable();
                    }
                }
                subLSOpResult = (ObTableLSOpResult) subObTable.execute(tableLsOpRequest);
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
                        logger.warn("tablename:{} ls id:{} retry when replica not readable: {}",
                                tableName, lsId, ex.getMessage());
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
                    logger.warn("tablename:{} ls id:{} batch ops refresh table while meet ObTableMasterChangeException, errorCode: {}",
                                 tableName, lsId, ((ObTableException) ex).getErrorCode(), ex);
                    if (obTableClient.isRetryOnChangeMasterTimes()
                            && (tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger.warn("tablename:{} ls id:{} batch ops retry while meet ObTableMasterChangeException, errorCode: {} , retry times {}",
                                     tableName, lsId, ((ObTableException) ex).getErrorCode(),
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
            RUNTIME.error("tablename:{} ls id:{} check batch operation result error: client get unexpected NULL result",
                           tableName, lsId);
            throw new ObTableUnexpectedException("check batch operation result error: client get unexpected NULL result");
        }

        List<ObTableTabletOpResult> tabletOpResults = subLSOpResult.getResults();
        int affectedRows = 0;

        if (returnOneResult) {
            if (results[0] == null) {
                results[0] = new ObTableSingleOpResult();
            }

            ObTableSingleOpResult singleOpResult = tabletOpResults.get(0).getResults().get(0);
            if (singleOpResult.getHeader().getErrno() != ResultCodes.OB_SUCCESS.errorCode) {
                results[0].getHeader().setErrno(singleOpResult.getHeader().getErrno());
                results[0].getHeader().setMsg(singleOpResult.getHeader().getMsg());
            }
            results[0].setAffectedRows(results[0].getAffectedRows() +
                    tabletOpResults.get(0).getResults().get(0).getAffectedRows());
        } else {
            for (int i = 0; i < tabletOpResults.size(); i++) {
                List<ObTableSingleOpResult> singleOpResults = tabletOpResults.get(i).getResults();
                for (int j = 0; j < singleOpResults.size(); j++) {
                    affectedRows += singleOpResults.size();
                }
                List<ObPair<Integer, ObTableSingleOp>> singleOperationsWithIndexList = lsOperationWithIndexList.get(i);
                if (singleOpResults.size() < singleOperationsWithIndexList.size()) {
                    // only one result when it across failed
                    // only one result when hkv puts
                    if (singleOpResults.size() == 1 && entityType == ObTableEntityType.HKV) {
                        ObTableSingleOpResult subObTableSingleOpResult = singleOpResults.get(0);
                        subObTableSingleOpResult.setExecuteHost(subObTable.getIp());
                        subObTableSingleOpResult.setExecutePort(subObTable.getPort());
                        for (ObPair<Integer, ObTableSingleOp> SubOperationWithIndexList : singleOperationsWithIndexList) {
                            results[SubOperationWithIndexList.getLeft()] = subObTableSingleOpResult;
                        }
                    } else {
                        throw new IllegalArgumentException(
                                "check batch operation result size error: operation size ["
                                        + singleOperationsWithIndexList.size() + "] result size ["
                                        + singleOpResults.size() + "]");
                    }
                } else {
                    if (singleOpResults.size() != singleOperationsWithIndexList.size()) {
                        throw new ObTableUnexpectedException("check batch result error: ls "
                                + lsId + " expect result size "
                                + singleOperationsWithIndexList.size()
                                + " actual result size "
                                + singleOpResults.size()
                                + " for " + i + "th tablet operation");
                    }
                    for (int j = 0; j < singleOperationsWithIndexList.size(); j++) {
                        ObTableSingleOpResult subObTableSingleOpResult = singleOpResults.get(j);
                        subObTableSingleOpResult.setExecuteHost(subObTable.getIp());
                        subObTableSingleOpResult.setExecutePort(subObTable.getPort());
                        results[singleOperationsWithIndexList.get(j).getLeft()] = subObTableSingleOpResult;
                    }
                }
            }
        }


        String endpoint = subObTable.getIp() + ":" + subObTable.getPort();
        MonitorUtil.info(tableLsOpRequest, subObTable.getDatabase(), tableName,
                "LS_BATCH-Execute-", endpoint, tableLsOp,
                affectedRows, endExecute - startExecute,
                obTableClient.getslowQueryMonitorThreshold());
    }

    /*
     * Execute internal.
     */
    public ObTableSingleOpResult[] executeInternal() throws Exception {

        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        long start = System.currentTimeMillis();
        ObTableSingleOpResult[] obTableOperationResults = null;
        if (returnOneResult) {
            obTableOperationResults = new ObTableSingleOpResult[1];
        } else {
            obTableOperationResults = new ObTableSingleOpResult[batchOperation.size()];
        }
        Map<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> lsOperations = partitionPrepare();
        long getTableTime = System.currentTimeMillis();
        final Map<Object, Object> context = ThreadLocalMap.getContextMap();
        if (executorService != null && !executorService.isShutdown() && lsOperations.size() > 1) {
            // execute sub-batch operation in parallel
            final ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(executorService,
                lsOperations.size());
            for (final Map.Entry<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> entry : lsOperations
                .entrySet()) {
                ObTableSingleOpResult[] finalObTableOperationResults = obTableOperationResults;
                executor.execute(new ConcurrentTask() {
                    /*
                     * Do task.
                     */
                    @Override
                    public void doTask() {
                        try {
                            ThreadLocalMap.transmitContextMap(context);
                            partitionExecute(finalObTableOperationResults, entry);
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
            for (final Map.Entry<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> entry : lsOperations
                .entrySet()) {
                partitionExecute(obTableOperationResults, entry);
            }
        }

        if (obTableOperationResults.length <= 0) {
            throw new ObTableUnexpectedException(
                "Ls batch execute returns zero single operation results");
        }

        MonitorUtil
            .info(obTableOperationResults[0], obTableClient.getDatabase(), tableName, "LS_BATCH",
                "", obTableOperationResults.length, getTableTime - start,
                System.currentTimeMillis() - getTableTime,
                obTableClient.getslowQueryMonitorThreshold());

        return obTableOperationResults;
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

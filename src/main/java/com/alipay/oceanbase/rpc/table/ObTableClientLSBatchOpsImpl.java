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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.get.Get;
import com.alipay.oceanbase.rpc.get.result.GetResult;
import com.alipay.oceanbase.rpc.location.model.ObServerRoute;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
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
                                                              .getLogger(ObTableClientLSBatchOpsImpl.class);
    private final ObTableClient   obTableClient;
    private ExecutorService       executorService;
    private boolean               returningAffectedEntity = false;
    private boolean               needAllProp             = false;
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
        range.setStartKey(ObRowKey.getInstance(insUp.getRowKeyValues()));
        range.setEndKey(ObRowKey.getInstance(insUp.getRowKeyValues()));
        query.addScanRangeColumns(insUp.getRowKeyNames());
        query.addScanRange(range);
        query.setFilterString(checkAndInsUp.getFilter().toString());

        String[] rowKeyNames = checkAndInsUp.getInsUp().getRowKeyNames().toArray(new String[0]);
        Object[] rowKeyValues = checkAndInsUp.getInsUp().getRowKeyValues().toArray(new Object[0]);
        String[] propertiesNames = checkAndInsUp.getInsUp().getColumns();
        Object[] propertiesValues = checkAndInsUp.getInsUp().getValues();
        ObTableSingleOpEntity entity = ObTableSingleOpEntity.getInstance(rowKeyNames, rowKeyValues,
            propertiesNames, propertiesValues);

        ObTableSingleOp singleOp = new ObTableSingleOp();
        singleOp.setSingleOpType(ObTableOperationType.CHECK_AND_INSERT_UP);
        singleOp.setIsCheckNoExists(!checkAndInsUp.isCheckExists());
        singleOp.setIsRollbackWhenCheckFailed(checkAndInsUp.isRollbackWhenCheckFailed());
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
        if (propertiesNames.length == 0) {
            needAllProp = true;
        }
        ObTableSingleOp singleOp = new ObTableSingleOp();
        if (entityType == ObTableEntityType.HKV) {
            ObTableQuery obTableQuery = query.getObTableQuery();
            ObTableSingleOpQuery singleOpQuery = ObTableSingleOpQuery.getInstance(
                obTableQuery.getIndexName(), obTableQuery.getKeyRanges(),
                obTableQuery.getSelectColumns(), obTableQuery.getScanOrder(),
                obTableQuery.isHbaseQuery(), obTableQuery.gethTableFilter(),
                obTableQuery.getObKVParams(), obTableQuery.getFilterString());
            singleOp.setQuery(singleOpQuery);
            singleOp.setSingleOpType(ObTableOperationType.SCAN);
        } else {
            singleOp.setSingleOpType(ObTableOperationType.GET);
        }
        singleOp.addEntity(entity);
        addOperation(singleOp);
    }

    public void addOperation(Mutation mutation) throws Exception {
        // entity
        String[] rowKeyNames = null;
        Object[] rowKeyValues = null;
        String[] propertiesNames = null;
        Object[] propertiesValues = null;

        ObTableOperationType type = mutation.getOperationType();
        switch (type) {
            case GET:
                throw new IllegalArgumentException("Invalid type in batch operation, " + type);
            case INSERT:
                ((Insert) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Insert) mutation).getRowKeyNames().toArray(new String[0]);
                rowKeyValues = ((Insert) mutation).getRowKeyValues().toArray(new Object[0]);
                propertiesNames = ((Insert) mutation).getColumns();
                propertiesValues = ((Insert) mutation).getValues();
                break;
            case DEL:
                rowKeyNames = ((Delete) mutation).getRowKeyNames().toArray(new String[0]);
                rowKeyValues = ((Delete) mutation).getRowKeyValues().toArray(new Object[0]);
                break;
            case UPDATE:
                ((Update) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Update) mutation).getRowKeyNames().toArray(new String[0]);
                rowKeyValues = ((Update) mutation).getRowKeyValues().toArray(new Object[0]);
                propertiesNames = ((Update) mutation).getColumns();
                propertiesValues = ((Update) mutation).getValues();
                break;
            case INSERT_OR_UPDATE:
                ((InsertOrUpdate) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((InsertOrUpdate) mutation).getRowKeyNames().toArray(new String[0]);
                rowKeyValues = ((InsertOrUpdate) mutation).getRowKeyValues().toArray(new Object[0]);
                propertiesNames = ((InsertOrUpdate) mutation).getColumns();
                propertiesValues = ((InsertOrUpdate) mutation).getValues();
                break;
            case REPLACE:
                ((Replace) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Replace) mutation).getRowKeyNames().toArray(new String[0]);
                rowKeyValues = ((Replace) mutation).getRowKeyValues().toArray(new Object[0]);
                propertiesNames = ((Replace) mutation).getColumns();
                propertiesValues = ((Replace) mutation).getValues();
                break;
            case INCREMENT:
                ((Increment) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Increment) mutation).getRowKeyNames().toArray(new String[0]);
                rowKeyValues = ((Increment) mutation).getRowKeyValues().toArray(new Object[0]);
                propertiesNames = ((Increment) mutation).getColumns();
                propertiesValues = ((Increment) mutation).getValues();
                break;
            case APPEND:
                ((Append) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Append) mutation).getRowKeyNames().toArray(new String[0]);
                rowKeyValues = ((Append) mutation).getRowKeyValues().toArray(new Object[0]);
                propertiesNames = ((Append) mutation).getColumns();
                propertiesValues = ((Append) mutation).getValues();
                break;
            case PUT:
                ((Put) mutation).removeRowkeyFromMutateColval();
                rowKeyNames = ((Put) mutation).getRowKeyNames().toArray(new String[0]);
                rowKeyValues = ((Put) mutation).getRowKeyValues().toArray(new Object[0]);
                propertiesNames = ((Put) mutation).getColumns();
                propertiesValues = ((Put) mutation).getValues();
                break;
            default:
                throw new ObTableException("unknown operation type " + type);
        }

        ObTableSingleOpEntity entity = ObTableSingleOpEntity.getInstance(rowKeyNames, rowKeyValues,
            propertiesNames, propertiesValues);
        ObTableSingleOp singleOp = new ObTableSingleOp();
        singleOp.setSingleOpType(type);
        singleOp.addEntity(entity);
        addOperation(singleOp);
    }

    public void addOperation(Get get) throws Exception {
        if (get.getRowKey() == null) {
            throw new ObTableException("RowKey is null");
        }
        String[] rowKeyNames = get.getRowKey().getColumns();
        Object[] rowKeyValues = get.getRowKey().getValues();
        String[] propertiesNames = get.getSelectColumns();
        ObTableSingleOpEntity entity = ObTableSingleOpEntity.getInstance(rowKeyNames, rowKeyValues,
            propertiesNames, null);
        ObTableSingleOp singleOp = new ObTableSingleOp();
        singleOp.setSingleOpType(ObTableOperationType.GET);
        singleOp.addEntity(entity);
        addOperation(singleOp);
    }

    /*
     * Execute.
     */
    public List<Object> execute() throws Exception {
        List<Object> results = new ArrayList<>(batchOperation.size());
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
        ObTableSingleOpResult[] singleResults = executeInternal();
        for (int i = 0; i < singleResults.length; i++) {
            ObTableSingleOpResult result = singleResults[i];
            // Sometimes the server does not set the operation type，so we use request operation type
            ObTableOperationType opType = batchOperation.get(i).getSingleOpType();
            int errCode = result.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                if (opType == ObTableOperationType.GET) {
                    results.add(new GetResult(result));
                } else {
                    results.add(new MutationResult(result));
                }
            } else {
                results.add(ExceptionUtil.convertToObTableException(result.getExecuteHost(),
                    result.getExecutePort(), result.getSequence(), result.getUniqueId(), errCode,
                    result.getHeader().getErrMsg()));
            }
        }
        return results;
    }

    private Object[] calculateRowKey(ObTableSingleOp operation) {
        List<ObObj> rowKeyObject = operation.getRowkeyObjs();
        int rowKeySize = rowKeyObject.size();
        Object[] rowKey = new Object[rowKeySize];
        for (int j = 0; j < rowKeySize; j++) {
            rowKey[j] = rowKeyObject.get(j).getValue();
        }
        return rowKey;
    }

    private List<ObPair<Integer, ObTableSingleOp>> extractOperations(Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> tabletOperationsMap) {
        List<ObPair<Integer, ObTableSingleOp>> operationsWithIndex = new ArrayList<>();
        for (ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>> pair : tabletOperationsMap.values()) {
            operationsWithIndex.addAll(pair.getRight());
        }
        return operationsWithIndex;
    }

    public Map<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> prepareOperations(List<ObPair<Integer, ObTableSingleOp>> operationsWithIndex) throws Exception {
        Map<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> lsOperationsMap = new HashMap<>();

        if (obTableClient.isOdpMode()) {
            Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> tabletOperationsMap = new HashMap<>();
            ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>> obTableOperations =
                    new ObPair<>(new ObTableParam(obTableClient.getOdpTable()),
                            new ArrayList<ObPair<Integer, ObTableSingleOp>>());
            for (int i = 0; i < operationsWithIndex.size(); i++) {
                obTableOperations.getRight().add(operationsWithIndex.get(i));
            }
            tabletOperationsMap.put(INVALID_TABLET_ID, obTableOperations);
            lsOperationsMap.put(INVALID_LS_ID, tabletOperationsMap);
            return lsOperationsMap;
        }

        for (int i = 0; i < operationsWithIndex.size(); i++) {
            ObTableSingleOp operation = operationsWithIndex.get(i).getRight();
            Object[] rowKey = calculateRowKey(operation);

            String real_tableName = tableName;
            if (this.entityType == ObTableEntityType.HKV && obTableClient.isTableGroupName(tableName)) {
                real_tableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, false);
            }
            ObPair<Long, ObTableParam> tableObPair = null;
            long lsId = INVALID_LS_ID;
            try {
                tableObPair = obTableClient.getTable(real_tableName, rowKey,
                        false, false, obTableClient.getRoute(false));
                lsId = tableObPair.getRight().getLsId();
            } catch (ObTableNotExistException e) {
                if (this.entityType == ObTableEntityType.HKV
                        && obTableClient.isTableGroupName(tableName)
                        && obTableClient.getTableGroupInverted().get(real_tableName) != null) {
                    obTableClient.eraseTableGroupFromCache(tableName);
                    real_tableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, true);
                    tableObPair = obTableClient.getTable(real_tableName, rowKey,
                            false, false, obTableClient.getRoute(false));
                    lsId = tableObPair.getRight().getLsId();
                } else {
                    throw e;
                }
            }

            Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> tabletOperations
                    = lsOperationsMap.computeIfAbsent(lsId, k -> new HashMap<>());
            // if ls id not exists

            ObPair<Long, ObTableParam> finalTableObPair = tableObPair;
            ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>> singleOperations =
                    tabletOperations.computeIfAbsent(tableObPair.getLeft(), k -> new ObPair<>(finalTableObPair.getRight(), new ArrayList<>()));
            // if tablet id not exists
            singleOperations.getRight().add(operationsWithIndex.get(i));
        }

        return lsOperationsMap;
    }

    public Map<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> partitionPrepare()
            throws Exception {
        List<ObTableSingleOp> operations = getSingleOperations();
        List<ObPair<Integer, ObTableSingleOp>> operationsWithIndex = new LinkedList<>();
        for (int i = 0; i < operations.size(); i++) {
            operationsWithIndex.add(new ObPair<>(i, operations.get(i)));
        }
        return prepareOperations(operationsWithIndex);
    }

    /*
     * Partition execute.
     */
    public void partitionExecute(ObTableSingleOpResult[] results,
                                 Map.Entry<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> lsOperation)
            throws Exception {
        long lsId = lsOperation.getKey();
        Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>> tabletOperationsMap = lsOperation.getValue();
        if (tabletOperationsMap.isEmpty()) {
            logger.warn("the size of tablet operations in ls operation is zero");
            throw new ObTableUnexpectedException("the size of tablet operations in ls operation is zero");
        }

        ObTableLSOpRequest tableLsOpRequest = new ObTableLSOpRequest();
        ObTableLSOperation tableLsOp = new ObTableLSOperation();
        tableLsOp.setLsId(lsId);
        tableLsOp.setReturnOneResult(returnOneResult);
        tableLsOp.setNeedAllProp(needAllProp);
        tableLsOp.setTableName(tableName);
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
        // maybe get real table name
        String realTableName = obTableClient.getPhyTableNameFromTableGroup(tableLsOpRequest.getEntityType(), tableName);
        while (true) {
            obTableClient.checkStatus();
            long currentExecute = System.currentTimeMillis();
            long costMillis = currentExecute - startExecute;
            if (costMillis > obTableClient.getRuntimeMaxWait()) {
                logger.error("table name: {} ls id:{} it has tried " + tryTimes
                                + " times and it has waited " + costMillis + " ms"
                                + " which exceeds runtime max wait timeout "
                                + obTableClient.getRuntimeMaxWait() + " ms", realTableName, lsId);
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
                        TableEntry entry = obTableClient.getOrRefreshTableEntry(realTableName, false,
                                false, false);
                        if (ObGlobal.obVsnMajor() >= 4) {
                            obTableClient.refreshTableLocationByTabletId(entry, realTableName, obTableClient.getTabletIdByPartId(entry, originPartId));
                        } else { // 3.x
                            obTableClient.getOrRefreshTableEntry(realTableName, needRefreshTableEntry,
                                    obTableClient.isTableEntryRefreshIntervalWait(), false);
                        }
                        subObTable = obTableClient.getTableWithPartId(realTableName, originPartId, needRefreshTableEntry,
                                        obTableClient.isTableEntryRefreshIntervalWait(), false, route).
                                            getRight().getObTable();
                    }
                }
                ObPayload result = subObTable.execute(tableLsOpRequest);
                if (result != null && result.getPcode() == Pcodes.OB_TABLE_API_MOVE) {
                    ObTableApiMove moveResponse = (ObTableApiMove) result;
                    obTableClient.getRouteTableRefresher().addTableIfAbsent(realTableName, true);
                    obTableClient.getRouteTableRefresher().triggerRefreshTable();
                    subObTable = obTableClient.getTable(moveResponse);
                    result = subObTable.execute(tableLsOpRequest);
                    if (result instanceof ObTableApiMove) {
                        ObTableApiMove move = (ObTableApiMove) result;
                        logger.warn("The server has not yet completed the master switch, and returned an incorrect leader with an IP address of {}. " +
                                "Rerouting return IP is {}", moveResponse.getReplica().getServer().ipToString(), move .getReplica().getServer().ipToString());
                        throw new ObTableRoutingWrongException();
                    }
                }
                subLSOpResult = (ObTableLSOpResult) result;
                obTableClient.resetExecuteContinuousFailureCount(realTableName);
                break;
            } catch (Exception ex) {
                if (obTableClient.isOdpMode()) {
                    logger.warn("meet exception when execute ls batch in odp mode." +
                            "tablename: {}, errMsg: {}", realTableName, ex.getMessage());
                    throw ex;
                } else if (ex instanceof ObTableReplicaNotReadableException) {
                    if ((tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger.warn("tablename:{} ls id:{} retry when replica not readable: {}",
                                realTableName, lsId, ex.getMessage());
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
                    if (((ObTableException) ex).getErrorCode() == ResultCodes.OB_TABLE_NOT_EXIST.errorCode
                            && obTableClient.isTableGroupName(tableName)
                            && obTableClient.getTableGroupInverted().get(realTableName) != null) {
                        // TABLE_NOT_EXIST + tableName is tableGroup + TableGroup cache is not empty
                        // means tableGroupName cache need to refresh
                        obTableClient.eraseTableGroupFromCache(tableName);
                        realTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, true);
                    }
                    if (obTableClient.isRetryOnChangeMasterTimes()
                            && (tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        if (ex instanceof ObTableNeedFetchAllException) {
                            obTableClient.getOrRefreshTableEntry(realTableName, needRefreshTableEntry,
                                    obTableClient.isTableEntryRefreshIntervalWait(), true);
                            throw ex;
                        }
                    } else {
                        String logMessage = String.format(
                                "exhaust retry while meet NeedRefresh Exception, table name: %s, ls id: %d, batch ops refresh table, retry times: %d, errorCode: %d",
                                realTableName,
                                lsId,
                                obTableClient.getRuntimeRetryTimes(),
                                ((ObTableException) ex).getErrorCode()
                        );
                        logger.warn(logMessage, ex);
                        obTableClient.calculateContinuousFailure(realTableName, ex.getMessage());
                        throw new ObTableRetryExhaustedException(logMessage, ex);
                    }
                } else {
                    obTableClient.calculateContinuousFailure(realTableName, ex.getMessage());
                    throw ex;
                }
            }
            Thread.sleep(obTableClient.getRuntimeRetryInterval());
        }

        long endExecute = System.currentTimeMillis();

        if (subLSOpResult == null) {
            String logMessage = String.format(
                    "table name: %s ls id: %d check batch operation result error: client get unexpected NULL result",
                    realTableName, lsId);
            RUNTIME.error(logMessage);
            throw new ObTableUnexpectedException(logMessage);
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
        MonitorUtil.info(tableLsOpRequest, subObTable.getDatabase(), realTableName,
                "LS_BATCH-Execute-", endpoint, tableLsOp,
                affectedRows, endExecute - startExecute,
                obTableClient.getslowQueryMonitorThreshold());
    }

    private boolean shouldRetry(Throwable throwable) {
        return throwable instanceof ObTableNeedFetchAllException;
    }

    private void executeWithRetries(
            ObTableSingleOpResult[] results,
            Map.Entry<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> entry,
            int maxRetries) throws Exception {

        int retryCount = 0;
        boolean success = false;
        
        Map<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> currentPartitions = new HashMap<>();
        currentPartitions.put(entry.getKey(), entry.getValue());
        int errCode = ResultCodes.OB_SUCCESS.errorCode;
        String errMsg = null;
        while (retryCount <= maxRetries && !success) {
            boolean allPartitionsSuccess = true;

            for (Map.Entry<Long, Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableSingleOp>>>>> currentEntry : currentPartitions.entrySet()) {
                try {
                    partitionExecute(results, currentEntry);
                } catch (Exception e) {
                    if (shouldRetry(e)) {
                        retryCount++;
                        errCode = ((ObTableNeedFetchAllException)e).getErrorCode();
                        errMsg = e.getMessage();
                        List<ObPair<Integer, ObTableSingleOp>> failedOperations = extractOperations(currentEntry.getValue());
                        currentPartitions = prepareOperations(failedOperations);
                        allPartitionsSuccess = false;
                        break;
                    } else {
                        throw e;
                    }
                }
            }

            if (allPartitionsSuccess) {
                success = true;
            }
        }

        if (!success) {
            errMsg = "Failed to execute operation after retrying " + maxRetries + " times. Last error Msg:" +
                    "[errCode="+ errCode +"] " + errMsg;
            throw new ObTableUnexpectedException(errMsg);
        }
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
        final int maxRetries = obTableClient.getRuntimeRetryTimes();

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
                            executeWithRetries(finalObTableOperationResults, entry, maxRetries);
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

                    if (!executor.getThrowableList().isEmpty()) {
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

            if (!executor.getThrowableList().isEmpty()) {
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
                executeWithRetries(obTableOperationResults, entry, maxRetries);
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

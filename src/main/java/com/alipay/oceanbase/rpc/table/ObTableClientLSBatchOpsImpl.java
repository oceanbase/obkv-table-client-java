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
import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
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
import com.alipay.oceanbase.rpc.queryandmutate.QueryAndMutate;
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

class BatchIdxOperationPairList extends ArrayList<ObPair<Integer, ObTableSingleOp>> {
};

class TabletOperationsMap extends HashMap<Long, ObPair<ObTableParam, BatchIdxOperationPairList>> {
};

class LsOperationsMap extends HashMap<Long, TabletOperationsMap> {
};

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

    public void addOperation(QueryAndMutate queryAndMutate) {

        ObTableSingleOp singleOp = new ObTableSingleOp();
        ObTableQuery obTableQuery = queryAndMutate.getQuery();
        if (queryAndMutate.getMutation() instanceof Delete) {
            Delete delete = (Delete) queryAndMutate.getMutation();
            ObTableSingleOpQuery singleOpQuery = ObTableSingleOpQuery.getInstance(
                obTableQuery.getIndexName(), obTableQuery.getKeyRanges(),
                obTableQuery.getSelectColumns(), obTableQuery.getScanOrder(),
                obTableQuery.isHbaseQuery(), obTableQuery.gethTableFilter(),
                obTableQuery.getObKVParams(), obTableQuery.getFilterString());
            singleOp.setQuery(singleOpQuery);
            singleOp.setQuery(singleOpQuery);
            singleOp.setSingleOpType(ObTableOperationType.QUERY_AND_MUTATE);
            String[] rowKeyNames = delete.getRowKey().getColumns();
            Object[] rowKeyValues = delete.getRowKey().getValues();
            ObTableSingleOpEntity entity = ObTableSingleOpEntity.getInstance(rowKeyNames,
                rowKeyValues, null, null);
            singleOp.addEntity(entity);
            addOperation(singleOp);
        } else {
            throw new ObTableException("invalid operation type "
                                       + queryAndMutate.getMutation().getOperationType());
        }
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
        if (ObTableClient.RunningMode.HBASE == obTableClient.getRunningMode()) {
            long ts = (long) ObTableClient.getRowKeyValue(mutation, 2);
            if (ts != -Long.MAX_VALUE) {
                singleOp.setIsUserSpecifiedT(true);
            }
        }
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

    private Row calculateRowKey(ObPair<Integer, ObTableSingleOp> operationPair) {
        ObTableSingleOp operation = operationPair.getRight();
        Row rowKey = new Row();
        List<ObObj> rowKeyObject = operation.getRowkeyObjs();
        List<String> rowKeyNames = operation.getRowKeyNames();
        int rowKeySize = rowKeyObject.size();
        if (rowKeySize != rowKeyNames.size()) {
            throw new ObTableUnexpectedException(
                "the length of rowKey value and rowKey name of the No." + operationPair.getLeft()
                        + " operation is not matched");
        }
        for (int j = 0; j < rowKeySize; j++) {
            Object rowKeyObj = rowKeyObject.get(j).getValue();
            String rowKeyName = rowKeyNames.get(j);
            rowKey.add(rowKeyName, rowKeyObj);
        }
        return rowKey;
    }

    private BatchIdxOperationPairList extractOperations(TabletOperationsMap tabletOperationsMap) {
        BatchIdxOperationPairList operationsWithIndex = new BatchIdxOperationPairList();
        for (ObPair<ObTableParam, BatchIdxOperationPairList> pair : tabletOperationsMap.values()) {
            operationsWithIndex.addAll(pair.getRight());
        }
        return operationsWithIndex;
    }

    public LsOperationsMap prepareOperations(BatchIdxOperationPairList operationsWithIndex) throws Exception {
        LsOperationsMap lsOperationsMap = new LsOperationsMap();

        if (obTableClient.isOdpMode()) {
            TabletOperationsMap tabletOperationsMap = new TabletOperationsMap();
            ObPair<ObTableParam, BatchIdxOperationPairList> obTableOperations =
                    new ObPair<>(new ObTableParam(obTableClient.getOdpTable()), operationsWithIndex);
            tabletOperationsMap.put(INVALID_TABLET_ID, obTableOperations);
            lsOperationsMap.put(INVALID_LS_ID, tabletOperationsMap);
            return lsOperationsMap;
        } else if (!obTableClient.getServerCapacity().isSupportDistributedExecute()) {
            return prepareByEachOperation(lsOperationsMap, operationsWithIndex);
        } else {
            return prepareByFirstOperation(lsOperationsMap, operationsWithIndex);
        }
    }

    private LsOperationsMap prepareByFirstOperation(LsOperationsMap lsOperationsMap,
                        BatchIdxOperationPairList operationsWithIndex) throws Exception {
        if (operationsWithIndex.isEmpty()) {
            throw new IllegalArgumentException("batch operations is empty");
        } else {
            String realTableName = tableName;
            // 1. get the ObTableParam of the first op
            ObPair<Integer, ObTableSingleOp> operationPair = operationsWithIndex.get(0);
            Row rowKey = calculateRowKey(operationPair);
            if (this.entityType == ObTableEntityType.HKV && obTableClient.isTableGroupName(tableName)) {
                realTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, false);
            }
            ObTableParam obTableParam = null;
            try {
                obTableParam = obTableClient.getTableParamWithRoute(realTableName, rowKey, obTableClient.getRoute(false));
            } catch (ObTableNotExistException e) {
                logger.warn("LSBatch meet TableNotExist Exception, realTableName: {}, errMsg: {}", realTableName, e.getMessage());
                // if it is HKV and is tableGroup request, TableNotExist and tableGroup cache not empty mean that the table cached had been dropped
                // not to refresh tableGroup cache
                if (this.entityType == ObTableEntityType.HKV
                        && obTableClient.isTableGroupName(tableName)
                        && obTableClient.getTableGroupInverted().get(realTableName) != null) {
                    obTableClient.eraseTableGroupFromCache(tableName);
                    realTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, true);
                    obTableParam = obTableClient.getTableParamWithRoute(realTableName, rowKey, obTableClient.getRoute(false));
                } else {
                    throw e;
                }
            }
            // 2. construct the TabletOperationsMap with invalid tabletId
            TabletOperationsMap tabletOperationsMap = new TabletOperationsMap();
            ObPair<ObTableParam, BatchIdxOperationPairList> obTableOperations =
                    new ObPair<>(obTableParam, operationsWithIndex);
            tabletOperationsMap.put(INVALID_TABLET_ID, obTableOperations);
            // 3. construct the LsOperationsMap with invalid lsId
            lsOperationsMap.put(INVALID_LS_ID, tabletOperationsMap);
            return lsOperationsMap;
        }
    }

    private LsOperationsMap prepareByEachOperation(LsOperationsMap lsOperationsMap,
                         BatchIdxOperationPairList operationsWithIndex) throws Exception {
        for (int i = 0; i < operationsWithIndex.size(); i++) {
            ObPair<Integer, ObTableSingleOp> operation = operationsWithIndex.get(i);
            Row rowKey = calculateRowKey(operation);

            String realTableName = tableName;
            if (this.entityType == ObTableEntityType.HKV && obTableClient.isTableGroupName(tableName)) {
                realTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, false);
            }
            ObTableParam tableParam = null;
            try {
                tableParam = obTableClient.getTableParamWithRoute(realTableName, rowKey, obTableClient.getRoute(false));
            } catch (ObTableNotExistException e) {
                logger.warn("LSBatch meet TableNotExist Exception, realTableName: {}, errMsg: {}", realTableName, e.getMessage());
                // if it is HKV and is tableGroup request, TableNotExist and tableGroup cache not empty mean that the table cached had been dropped
                // not to refresh tableGroup cache
                if (this.entityType == ObTableEntityType.HKV
                        && obTableClient.isTableGroupName(tableName)
                        && obTableClient.getTableGroupInverted().get(realTableName) != null) {
                    obTableClient.eraseTableGroupFromCache(tableName);
                    realTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, true);
                    tableParam = obTableClient.getTableParamWithRoute(realTableName, rowKey, obTableClient.getRoute(false));
                } else {
                    throw e;
                }
            }
            final ObTableParam finalTableParam = tableParam;
            long lsId = tableParam.getLsId();

            TabletOperationsMap tabletOperations
                    = lsOperationsMap.computeIfAbsent(lsId, k -> new TabletOperationsMap());
            ObPair<ObTableParam, BatchIdxOperationPairList> singleOperations =
                    tabletOperations.computeIfAbsent(finalTableParam.getPartId(), k -> new ObPair<>(finalTableParam, new BatchIdxOperationPairList()));
            // if tablet id not exists
            singleOperations.getRight().add(operationsWithIndex.get(i));
        }
        return lsOperationsMap;
    }

    public LsOperationsMap partitionPrepare()
            throws Exception {
        List<ObTableSingleOp> operations = getSingleOperations();
        BatchIdxOperationPairList operationsWithIndex = new BatchIdxOperationPairList();
        for (int i = 0; i < operations.size(); i++) {
            operationsWithIndex.add(new ObPair<>(i, operations.get(i)));
        }
        return prepareOperations(operationsWithIndex);
    }

    /*
     * Partition execute.
     */
    public void partitionExecute(ObTableSingleOpResult[] results,
                                 Map.Entry<Long, TabletOperationsMap> lsOperation)
            throws Exception {
        long lsId = lsOperation.getKey();
        TabletOperationsMap tabletOperationsMap = lsOperation.getValue();
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
        List<Long> tabletIds = new ArrayList<>();
        ObTable subObTable = null;

        boolean isFirstEntry = true;
        // list ( index list for tablet op 1, index list for tablet op 2, ...)
        List<List<ObPair<Integer, ObTableSingleOp>>> lsOperationWithIndexList = new ArrayList<>();
        for (final Map.Entry<Long, ObPair<ObTableParam, BatchIdxOperationPairList>> tabletOperation : tabletOperationsMap.entrySet()) {
            ObTableParam tableParam = tabletOperation.getValue().getLeft();
            long tabletId = obTableClient.getServerCapacity().isSupportDistributedExecute() ?
                    INVALID_TABLET_ID : tableParam.getPartitionId();
            List<ObPair<Integer, ObTableSingleOp>> tabletOperationWithIndexList = tabletOperation.getValue().getRight();
            lsOperationWithIndexList.add(tabletOperationWithIndexList);
            List<ObTableSingleOp> singleOps = new ArrayList<>();
            for (ObPair<Integer, ObTableSingleOp> operationWithIndex : tabletOperationWithIndexList) {
                singleOps.add(operationWithIndex.getRight());
            }
            ObTableTabletOp tableTabletOp = new ObTableTabletOp();
            tableTabletOp.setSingleOperations(singleOps);
            tableTabletOp.setTabletId(tabletId);
            tabletIds.add(tabletId);

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
        boolean needRefreshPartitionLocation = false;
        int tryTimes = 0;
        Set<String> failedServerList = null;
        ObServerRoute route = null;
        // maybe get real table name
        String realTableName = obTableClient.getPhyTableNameFromTableGroup(tableLsOpRequest.getEntityType(), tableName);
        long startExecute = System.currentTimeMillis();
        while (true) {
            obTableClient.checkStatus();
            long costMillis = System.currentTimeMillis() - startExecute;
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
                        if (needRefreshPartitionLocation) {
                            // refresh partition location
                            TableEntry entry = obTableClient.getOrRefreshTableEntry(realTableName, false);
                            obTableClient.refreshTableLocationByTabletId(realTableName, obTableClient.getTabletIdByPartId(entry, originPartId));
                            ObTableParam param = obTableClient.getTableParamWithPartId(realTableName, originPartId, route);
                            subObTable = param.getObTable();
                        }
                    }
                }
                ObPayload result = subObTable.execute(tableLsOpRequest);
                if (result != null && result.getPcode() == Pcodes.OB_TABLE_API_MOVE) {
                    ObTableApiMove moveResponse = (ObTableApiMove) result;
                    subObTable = obTableClient.getTable(moveResponse);
                    result = subObTable.execute(tableLsOpRequest);
                    if (result instanceof ObTableApiMove) {
                        ObTableApiMove move = (ObTableApiMove) result;
                        logger.warn("The server has not yet completed the master switch, and returned an incorrect leader with an IP address of {}. " +
                                "Rerouting return IP is {}", moveResponse.getReplica().getServer().ipToString(), move.getReplica().getServer().ipToString());
                        throw new ObTableRoutingWrongException();
                    }
                }
                subLSOpResult = (ObTableLSOpResult) result;
                obTableClient.resetExecuteContinuousFailureCount(realTableName);
                break;
            } catch (Exception ex) {
                needRefreshPartitionLocation = true;
                if (obTableClient.isOdpMode()) {
                    // if exceptions need to retry, retry to timeout
                    if (ex instanceof ObTableException && ((ObTableException) ex).isNeedRetryServerError()) {
                        logger.warn("meet need retry exception when execute ls batch in odp mode." +
                                "tableName: {}, errMsg: {}", realTableName, ex.getMessage());
                    } else {
                        logger.warn("meet exception when execute ls batch in odp mode." +
                                "tablename: {}, errMsg: {}", realTableName, ex.getMessage());
                        throw ex;
                    }
                } else if (ex instanceof ObTableReplicaNotReadableException) {
                    if (System.currentTimeMillis() - startExecute < obTableClient.getRuntimeMaxWait()) {
                        logger.warn("tablename:{} ls id:{} retry when replica not readable: {}",
                                realTableName, lsId, ex.getMessage());
                        if (failedServerList == null) {
                            failedServerList = new HashSet<String>();
                        }
                        failedServerList.add(subObTable.getIp());
                    } else {
                        logger.warn("retry to timeout when replica not readable: {}", ex.getMessage());
                        throw ex;
                    }
                } else if (ex instanceof ObTableException) {
                    if (((ObTableException) ex).isNeedRefreshTableEntry()) {
                        logger.warn("meet need refresh exception, errCode: {}, ls id: {}", ((ObTableException) ex).getErrorCode(), lsId);
                        if ((((ObTableException) ex).getErrorCode() == ResultCodes.OB_TABLE_NOT_EXIST.errorCode ||
                                ((ObTableException) ex).getErrorCode() == ResultCodes.OB_SCHEMA_ERROR.errorCode)
                                && obTableClient.isTableGroupName(tableName)
                                && obTableClient.getTableGroupInverted().get(realTableName) != null) {
                            // (TABLE_NOT_EXIST or OB_SCHEMA_ERROR) + tableName is tableGroup + TableGroup cache is not empty
                            // means tableGroupName cache need to refresh
                            obTableClient.eraseTableGroupFromCache(tableName);
                            realTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, true);
                        }
                        // if exceptions need to retry, retry to timeout
                        if (obTableClient.isRetryOnChangeMasterTimes()) {
                            if (ex instanceof ObTableNeedFetchMetaException) {
                                obTableClient.getOrRefreshTableEntry(realTableName, true);
                                if (((ObTableNeedFetchMetaException) ex).isNeedRefreshMetaAndLocation()) {
                                    obTableClient.refreshTabletLocationBatch(realTableName);
                                }
                                throw ex;
                            }
                        } else {
                            String logMessage = String.format(
                                    "retry is disabled while meet NeedRefresh Exception, table name: %s, ls id: %d, batch ops refresh table, retry times: %d, errorCode: %d",
                                    realTableName,
                                    lsId,
                                    tryTimes,
                                    ((ObTableException) ex).getErrorCode()
                            );
                            logger.warn(logMessage, ex);
                            obTableClient.calculateContinuousFailure(realTableName, ex.getMessage());
                            throw new ObTableRetryExhaustedException(logMessage, ex);
                        }
                    } else if (((ObTableException) ex).isNeedRetryServerError()) {
                        // retry server errors, no need to refresh partition location
                        needRefreshPartitionLocation = false;
                        if (obTableClient.isRetryOnChangeMasterTimes()) {
                            logger.warn(
                                    "execute while meet server error, need to retry, errorCode: {}, ls id: {}, errorMsg: {}, try times {}",
                                    ((ObTableException) ex).getErrorCode(), lsId, ex.getMessage(),
                                    tryTimes);
                        } else {
                            String logMessage = String.format(
                                    "retry is disabled while meet NeedRefresh Exception, table name: %s, ls id: %d, batch ops refresh table, retry times: %d, errorCode: %d",
                                    realTableName,
                                    lsId,
                                    tryTimes,
                                    ((ObTableException) ex).getErrorCode()
                            );
                            logger.warn(logMessage, ex);
                            obTableClient.calculateContinuousFailure(realTableName, ex.getMessage());
                            throw new ObTableRetryExhaustedException(logMessage, ex);
                        }
                    } else {
                        if (ex instanceof ObTableTransportException &&
                                ((ObTableTransportException) ex).getErrorCode() == TransportCodes.BOLT_TIMEOUT) {
                            obTableClient.syncRefreshMetadata(true);
                        }
                        obTableClient.calculateContinuousFailure(realTableName, ex.getMessage());
                        throw ex;
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
        return throwable instanceof ObTableNeedFetchMetaException;
    }

    private void executeWithRetries(ObTableSingleOpResult[] results,
                                    Map.Entry<Long, TabletOperationsMap> entry) throws Exception {

        int retryCount = 0;
        boolean success = false;

        LsOperationsMap currentPartitions = new LsOperationsMap();
        currentPartitions.put(entry.getKey(), entry.getValue());
        int errCode = ResultCodes.OB_SUCCESS.errorCode;
        String errMsg = null;
        int maxRetryTimes = obTableClient.getRuntimeRetryTimes();
        while (retryCount < maxRetryTimes && !success) {
            boolean allPartitionsSuccess = true;

            for (Map.Entry<Long, TabletOperationsMap> currentEntry : currentPartitions.entrySet()) {
                try {
                    partitionExecute(results, currentEntry);
                } catch (Exception e) {
                    if (shouldRetry(e)) {
                        logger.warn("ls batch meet should retry exception", e);
                        retryCount++;
                        errCode = ((ObTableNeedFetchMetaException) e).getErrorCode();
                        errMsg = e.getMessage();
                        BatchIdxOperationPairList failedOperations = extractOperations(currentEntry
                            .getValue());
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
            errMsg = "Failed to execute operation after retrying " + retryCount
                     + " times. Last error Msg:" + "[errCode=" + errCode + "] " + errMsg;
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
        LsOperationsMap lsOperations = partitionPrepare();
        long getTableTime = System.currentTimeMillis();
        final Map<Object, Object> context = ThreadLocalMap.getContextMap();

        if (executorService != null && !executorService.isShutdown() && lsOperations.size() > 1) {
            // execute sub-batch operation in parallel
            final ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(executorService,
                lsOperations.size());
            for (final Map.Entry<Long, TabletOperationsMap> entry : lsOperations.entrySet()) {
                ObTableSingleOpResult[] finalObTableOperationResults = obTableOperationResults;
                executor.execute(new ConcurrentTask() {
                    /*
                     * Do task.
                     */
                    @Override
                    public void doTask() {
                        try {
                            ThreadLocalMap.transmitContextMap(context);
                            executeWithRetries(finalObTableOperationResults, entry);
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
            for (final Map.Entry<Long, TabletOperationsMap> entry : lsOperations.entrySet()) {
                executeWithRetries(obTableOperationResults, entry);
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

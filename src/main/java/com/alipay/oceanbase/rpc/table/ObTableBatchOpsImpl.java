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

import com.alipay.oceanbase.rpc.exception.ExceptionUtil;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.*;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.remoting.exception.RemotingException;

import java.util.ArrayList;
import java.util.List;

public class ObTableBatchOpsImpl extends AbstractTableBatchOps {

    private final ObTableBatchOperationRequest request;
    private final ObTableBatchOperation        operations;

    private ObTable                            obTable;

    /*
     * Ob table batch ops impl.
     */
    public ObTableBatchOpsImpl(String tableName, ObTable obTable) {
        super.setTableName(tableName);
        this.request = new ObTableBatchOperationRequest();
        this.operations = new ObTableBatchOperation();
        this.obTable = obTable;

        request.setTimeout(obTable.getObTableOperationTimeout());
        request.setBatchOperation(operations);
        request.setTableName(tableName);
        request.setReturningAffectedRows(true);
    }

    /*
     * Get.
     */
    @Override
    public void get(Object[] rowkeys, String[] columns) {
        addObTableOperation(ObTableOperationType.GET, rowkeys, columns, null);
    }

    /*
     * Update.
     */
    @Override
    public void update(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableOperation(ObTableOperationType.UPDATE, rowkeys, columns, values);
    }

    /*
     * Delete.
     */
    @Override
    public void delete(Object[] rowkeys) {
        addObTableOperation(ObTableOperationType.DEL, rowkeys, null, null);
    }

    /*
     * Insert.
     */
    @Override
    public void insert(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableOperation(ObTableOperationType.INSERT, rowkeys, columns, values);
    }

    /*
     * Put.
     */
    @Override
    public void put(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableOperation(ObTableOperationType.PUT, rowkeys, columns, values);
    }

    /*
     * Replace.
     */
    @Override
    public void replace(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableOperation(ObTableOperationType.REPLACE, rowkeys, columns, values);
    }

    /*
     * Insert or update.
     */
    @Override
    public void insertOrUpdate(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableOperation(ObTableOperationType.INSERT_OR_UPDATE, rowkeys, columns, values);
    }

    /*
     * Increment.
     */
    @Override
    public void increment(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        request.setReturningAffectedEntity(withResult);
        addObTableOperation(ObTableOperationType.INCREMENT, rowkeys, columns, values);
    }

    /*
     * Append.
     */
    @Override
    public void append(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        request.setReturningAffectedEntity(withResult);
        addObTableOperation(ObTableOperationType.APPEND, rowkeys, columns, values);
    }

    /*
     * Add ob table operation.
     */
    public void addObTableOperation(ObTableOperationType type, Object[] rowkeys, String[] columns,
                                    Object[] values) {
        ObTableOperation instance = ObTableOperation.getInstance(type, rowkeys, columns, values);

        operations.addTableOperation(instance);
    }

    /*
     * Execute.
     */
    public List<Object> execute() throws RemotingException, InterruptedException {

        request.setBatchOperationAsAtomic(isAtomicOperation());
        Object result = obTable.execute(request);
        checkObTableOperationResult(result);

        ObTableBatchOperationResult obTableOperationResult = (ObTableBatchOperationResult) result;
        List<ObTableOperationResult> realResults = obTableOperationResult.getResults();
        List<Object> results = new ArrayList<Object>(realResults.size());
        for (ObTableOperationResult realResult : realResults) {
            int errCode = realResult.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                switch (realResult.getOperationType()) {
                    case GET:
                    case INCREMENT:
                    case APPEND:
                        results.add(realResult.getEntity().getSimpleProperties());
                        break;
                    default:
                        results.add(realResult.getAffectedRows());
                }
            } else {
                results.add(ExceptionUtil.convertToObTableException(obTable.getIp(),
                    obTable.getPort(), realResult.getSequence(), realResult.getUniqueId(), errCode,
                    realResult.getHeader().getErrMsg()));
            }
        }
        return results;
    }

    /*
     * Execute with result
     */
    public List<Object> executeWithResult() throws Exception {

        request.setBatchOperationAsAtomic(isAtomicOperation());
        Object result = obTable.execute(request);
        checkObTableOperationResult(result);

        ObTableBatchOperationResult obTableOperationResult = (ObTableBatchOperationResult) result;
        List<ObTableOperationResult> realResults = obTableOperationResult.getResults();
        List<Object> results = new ArrayList<Object>(realResults.size());
        for (ObTableOperationResult realResult : realResults) {
            int errCode = realResult.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                switch (realResult.getOperationType()) {
                    case GET:
                        throw new ObTableException("Get is not a mutation");
                    case INSERT:
                    case DEL:
                    case UPDATE:
                    case INSERT_OR_UPDATE:
                    case REPLACE:
                    case INCREMENT:
                    case APPEND:
                        results.add(new MutationResult(realResult));
                        break;
                    default:
                        throw new ObTableException("unknown operation type "
                                                   + realResult.getOperationType());
                }
            } else {
                results.add(ExceptionUtil.convertToObTableException(obTable.getIp(),
                    obTable.getPort(), realResult.getSequence(), realResult.getUniqueId(), errCode,
                    realResult.getHeader().getErrMsg()));
            }
        }
        return results;
    }

    /*
     * clear batch operations
     */
    public void clear() {
        operations.getTableOperations().clear();
    }

    private void checkObTableOperationResult(Object result) {
        if (result == null) {
            throw new ObTableException("client get unexpected NULL result");
        }

        if (!(result instanceof ObTableBatchOperationResult)) {
            throw new ObTableException("client get unexpected result: "
                                       + result.getClass().getName());
        }

        // ObTableBatchOperationResult obTableOperationResult = (ObTableBatchOperationResult) result;
        // ExceptionUtil.throwObTableException(obTableOperationResult.getHeader().getErrno());
    }

    /*
     * Reset ob table.
     */
    public void resetObTable(ObTable obTable) {
        this.obTable = obTable;
    }

    /*
     * Get ob table batch operation.
     */
    @Override
    public ObTableBatchOperation getObTableBatchOperation() {
        return operations;
    }
}

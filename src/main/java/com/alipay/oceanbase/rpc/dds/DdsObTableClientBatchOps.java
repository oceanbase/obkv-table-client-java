/*-
* #%L
 * * OceanBase Table Client Framework
 * *
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
 * *
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

package com.alipay.oceanbase.rpc.dds;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.dds.rule.DatabaseAndTable;
import com.alipay.oceanbase.rpc.dds.util.VersionedConfigSnapshot;
import com.alipay.oceanbase.rpc.table.AbstractTableBatchOps;
import com.alipay.oceanbase.rpc.table.ObTableClientBatchOpsImpl;

import java.util.List;

/**
* @author zhiqi.zzq
* @since 2021/7/14 下午7:20
*/
public class DdsObTableClientBatchOps extends AbstractTableBatchOps {

    private final DdsObTableClient ddsObTableClient;
    private final String           tableName;

    private ObTableBatchOperation  batchOperation;
    private boolean                returningAffectedEntity = false;

    /**
    * DdsObTableClientBatchOps
    * @param tableName String
    * @param ddsObTableClient DdsObTableClient
    */
    public DdsObTableClientBatchOps(String tableName, DdsObTableClient ddsObTableClient) {
        this.tableName = tableName;
        this.ddsObTableClient = ddsObTableClient;
        this.batchOperation = new ObTableBatchOperation();
    }

    /**
    *
    * @return
    */
    @Override
    public ObTableBatchOperation getObTableBatchOperation() {
        return batchOperation;
    }

    /**
    * Get.
    */
    @Override
    public void get(Object[] rowkeys, String[] columns) {
        addObTableClientOperation(ObTableOperationType.GET, rowkeys, columns, null);
    }

    /**
    * Update.
    */
    @Override
    public void update(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.UPDATE, rowkeys, columns, values);
    }

    /**
    * Delete.
    */
    @Override
    public void delete(Object[] rowkeys) {
        addObTableClientOperation(ObTableOperationType.DEL, rowkeys, null, null);
    }

    /**
    * Insert.
    */
    @Override
    public void insert(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.INSERT, rowkeys, columns, values);
    }

    /**
    * Replace.
    */
    @Override
    public void replace(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.REPLACE, rowkeys, columns, values);
    }

    /**
    * Insert or update.
    */
    @Override
    public void insertOrUpdate(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.INSERT_OR_UPDATE, rowkeys, columns, values);
    }

    /**
    * Increment.
    */
    @Override
    public void increment(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        returningAffectedEntity = withResult;
        addObTableClientOperation(ObTableOperationType.INCREMENT, rowkeys, columns, values);
    }

    /**
    * Append.
    */
    @Override
    public void append(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        returningAffectedEntity = withResult;
        addObTableClientOperation(ObTableOperationType.APPEND, rowkeys, columns, values);
    }

    private void addObTableClientOperation(ObTableOperationType type, Object[] rowkeys,
                                           String[] columns, Object[] values) {
        ObTableOperation instance = ObTableOperation.getInstance(type, rowkeys, columns, values);
        batchOperation.addTableOperation((instance));
    }

    @Override
    public List<Object> execute() throws Exception {
        VersionedConfigSnapshot snapshot = ddsObTableClient.getCurrentConfigSnapshot();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        
        DatabaseAndTable databaseAndTable = ddsObTableClient.calculateDatabaseAndTable(tableName,
            batchOperation.getTableOperations(), snapshot);

        ObTableClient client = ddsObTableClient.getObTableWithSnapshot(databaseAndTable, snapshot);
        ObTableClientBatchOpsImpl obTableClientBatchOpsImpl = new ObTableClientBatchOpsImpl(
            this.ddsObTableClient.getTargetTableName(databaseAndTable.getTableName()),
            batchOperation, client);
        obTableClientBatchOpsImpl.setReturningAffectedEntity(returningAffectedEntity);
        return obTableClientBatchOpsImpl.execute();
    }

    @Override
    public void clear() {
        batchOperation = new ObTableBatchOperation();
    }

    @Override
    public void put(Object[] rowkeys, String[] columns, Object[] values) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'put'");
    }

    @Override
    public List<Object> executeWithResult() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'executeWithResult'");
    }

    public boolean isReturningAffectedEntity() {
        return returningAffectedEntity;
    }

    public void setReturningAffectedEntity(boolean returningAffectedEntity) {
        this.returningAffectedEntity = returningAffectedEntity;
    }
}

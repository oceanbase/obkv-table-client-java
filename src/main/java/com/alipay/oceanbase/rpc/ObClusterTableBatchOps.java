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

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.table.AbstractTableBatchOps;
import com.alipay.oceanbase.rpc.table.ObTableClientBatchOpsImpl;

import java.util.List;
import java.util.concurrent.ExecutorService;

// TODO rename it
public class ObClusterTableBatchOps extends AbstractTableBatchOps {

    private final ObTableClientBatchOpsImpl tableBatchOps;

    ObClusterTableBatchOps(ObTableClientBatchOpsImpl tableBatchOps) {
        this.tableBatchOps = tableBatchOps;
    }

    ObClusterTableBatchOps(ExecutorService executorService, ObTableClientBatchOpsImpl tableBatchOps) {
        this.tableBatchOps = tableBatchOps;
        this.tableBatchOps.setExecutorService(executorService);
    }

    /*
     * Get.
     */
    @Override
    public void get(Object[] rowkeys, String[] columns) {
        tableBatchOps.get(rowkeys, columns);
    }

    /*
     * Update.
     */
    @Override
    public void update(Object[] rowkeys, String[] columns, Object[] values) {
        tableBatchOps.update(rowkeys, columns, values);
    }

    /*
     * Delete.
     */
    @Override
    public void delete(Object[] rowkeys) {
        tableBatchOps.delete(rowkeys);
    }

    /*
     * Insert.
     */
    @Override
    public void insert(Object[] rowkeys, String[] columns, Object[] values) {
        tableBatchOps.insert(rowkeys, columns, values);
    }

    /*
     * Replace.
     */
    @Override
    public void replace(Object[] rowkeys, String[] columns, Object[] values) {
        tableBatchOps.replace(rowkeys, columns, values);
    }

    /*
     * Insert or update.
     */
    @Override
    public void insertOrUpdate(Object[] rowkeys, String[] columns, Object[] values) {
        tableBatchOps.insertOrUpdate(rowkeys, columns, values);
    }

    /*
     * Increment.
     */
    @Override
    public void increment(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        tableBatchOps.increment(rowkeys, columns, values, withResult);
    }

    /*
     * Append.
     */
    @Override
    public void append(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        tableBatchOps.append(rowkeys, columns, values, withResult);
    }

    /*
     * Execute.
     */
    @Override
    public List<Object> execute() throws Exception {
        return tableBatchOps.execute();
    }

    /*
     * Execute internal.
     */
    public ObTableBatchOperationResult executeInternal() throws Exception {
        return tableBatchOps.executeInternal();
    }

    /*
     * clear batch operations
     */
    public void clear() {
        tableBatchOps.clear();
    }

    /*
     * Get ob table batch operation.
     */
    @Override
    public ObTableBatchOperation getObTableBatchOperation() {
        return tableBatchOps.getObTableBatchOperation();
    }

    /*
     * Get table name.
     */
    @Override
    public String getTableName() {
        return tableBatchOps.getTableName();
    }

    /*
     * Set entity type.
     */
    @Override
    public void setEntityType(ObTableEntityType entityType) {
        super.setEntityType(entityType);
        tableBatchOps.setEntityType(entityType);
    }

    /*
     * Set atomic operation.
     */
    @Override
    public void setAtomicOperation(boolean atomicOperation) {
        super.setAtomicOperation(atomicOperation);
        tableBatchOps.setAtomicOperation(atomicOperation);
    }
}

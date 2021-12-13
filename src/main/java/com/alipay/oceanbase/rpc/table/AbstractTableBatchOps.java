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

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;

public abstract class AbstractTableBatchOps implements TableBatchOps {

    protected String            tableName;

    protected boolean           atomicOperation;

    protected ObTableEntityType entityType = ObTableEntityType.DYNAMIC;

    /**
     * Get.
     */
    @Override
    public void get(Object rowkey, String[] columns) {
        get(new Object[] { rowkey }, columns);
    }

    /**
     * Update.
     */
    @Override
    public void update(Object rowkey, String[] columns, Object[] values) {
        update(new Object[] { rowkey }, columns, values);
    }

    /**
     * Delete.
     */
    @Override
    public void delete(Object rowkey) {
        delete(new Object[] { rowkey });
    }

    /**
     * Insert.
     */
    @Override
    public void insert(Object rowkey, String[] columns, Object[] values) {
        insert(new Object[] { rowkey }, columns, values);
    }

    /**
     * Replace.
     */
    @Override
    public void replace(Object rowkey, String[] columns, Object[] values) {
        replace(new Object[] { rowkey }, columns, values);
    }

    /**
     * Insert or update.
     */
    @Override
    public void insertOrUpdate(Object rowkey, String[] columns, Object[] values) {
        insertOrUpdate(new Object[] { rowkey }, columns, values);
    }

    /**
     * Increment.
     */
    @Override
    public void increment(Object rowkey, String[] columns, Object[] values, boolean withResult) {
        increment(new Object[] { rowkey }, columns, values, withResult);
    }

    /**
     * Append.
     */
    @Override
    public void append(Object rowkey, String[] columns, Object[] values, boolean withResult) {
        append(new Object[] { rowkey }, columns, values, withResult);
    }

    /**
     * Get table name.
     */
    @Override
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
     * Set atomic operation.
     */
    @Override
    public void setAtomicOperation(boolean atomicOperation) {
        this.atomicOperation = atomicOperation;
    }

    /*
     * Is atomic operation.
     */
    @Override
    public boolean isAtomicOperation() {
        return atomicOperation;
    }

    /*
     * Set entity type.
     */
    @Override
    public void setEntityType(ObTableEntityType entityType) {
        this.entityType = entityType;
    }

    /*
     * Get entity type.
     */
    @Override
    public ObTableEntityType getEntityType() {
        return entityType;
    }
}

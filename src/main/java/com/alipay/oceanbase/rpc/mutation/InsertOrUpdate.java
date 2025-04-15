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

package com.alipay.oceanbase.rpc.mutation;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.table.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InsertOrUpdate extends Mutation<InsertOrUpdate> {
    private boolean usePut;

    /*
     * default constructor
     */
    public InsertOrUpdate() {
        super();
        usePut = false;
        columns = new ArrayList<String>();
        values = new ArrayList<Object>();
    }

    /*
     * construct with ObTableClient and String
     */
    public InsertOrUpdate(Table client, String tableName) {
        super(client, tableName);
        usePut = false;
        columns = new ArrayList<String>();
        values = new ArrayList<Object>();
    }

    /*
     * set use put
     */
    protected InsertOrUpdate usePut() {
        usePut = true;
        return this;
    }

    /*
     * get operation type
     */
    public ObTableOperationType getOperationType() {
        return ObTableOperationType.INSERT_OR_UPDATE;
    }

    /*
     * get the mutated columns' name
     */
    public String[] getColumns() {
        return columns.toArray(new String[0]);
    }

    /*
     * get the mutated columns' value
     */
    public Object[] getValues() {
        return values.toArray();
    }

    /*
     * add mutated Row
     */
    public InsertOrUpdate addMutateRow(Row rows) {
        if (null == rows) {
            throw new IllegalArgumentException("Invalid null rowKey set into Insert");
        } else if (0 == rows.getMap().size()) {
            throw new IllegalArgumentException("input row key should not be empty");
        }

        // set mutate row into Insert
        for (Map.Entry<String, Object> entry : rows.getMap().entrySet()) {
            columns.add(entry.getKey());
            values.add(entry.getValue());
        }

        return this;
    }

    /*
     * add mutated ColumnValues
     */
    public InsertOrUpdate addMutateColVal(ColumnValue... columnValues) {
        if (null == columnValues) {
            throw new IllegalArgumentException("Invalid null columnValues set into Insert");
        }

        // set mutate row into Insert
        for (ColumnValue columnValue : columnValues) {
            if (columns.contains(columnValue.getColumnName())) {
                throw new ObTableException("Duplicate column in Row Key");
            }
            columns.add(columnValue.getColumnName());
            values.add(columnValue.getValue());
        }

        return this;
    }

    /*
     * Remove rowkey from mutateColval
     */
    public InsertOrUpdate removeRowkeyFromMutateColval() {
        removeRowkeyFromMutateColval(this.columns, this.values, this.rowKeyNames);
        return this;
    }

    /*
     * execute
     */
    public MutationResult execute() throws Exception {
        if (null == getTableName() || getTableName().isEmpty()) {
            throw new ObTableException("table name is null");
        } else if (null == getClient()) {
            throw new ObTableException("client is null");
        }
        removeRowkeyFromMutateColval(this.columns, this.values, this.rowKeyNames);
        if (null == getQuery()) {
            // simple InsertOrUpdate, without filter
            return new MutationResult(((ObTableClient) getClient()).insertOrUpdateWithResult(
                getTableName(), getRowKey(), columns.toArray(new String[0]), values.toArray(),
                usePut));
        } else {
            throw new ObTableException("InsertOrUpdate with query(filter) is not supported yet");
        }
    }
}

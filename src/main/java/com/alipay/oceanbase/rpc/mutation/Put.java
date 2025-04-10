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
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.table.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Put extends Mutation<Put> {
    /*
     * default constructor
     */
    public Put() {
        super();
        columns = new ArrayList<String>();
        values = new ArrayList<Object>();
    }

    /*
     * construct with ObTableClient and String
     */
    public Put(Table client, String tableName) {
        super(client, tableName);
        columns = new ArrayList<String>();
        values = new ArrayList<Object>();
    }

    /*
     * set the Row Key of mutation with Row and keep scan range
     */
    @Override
    public Put setRowKey(Row rowKey) {
        return setRowKeyOnly(rowKey);
    }

    /*
     * set the Row Key of mutation with ColumnValues and keep scan range
     */
    @Override
    public Put setRowKey(ColumnValue... rowKey) {
        return setRowKeyOnly(rowKey);
    }

    /*
     * add filter into mutation (use QueryAndMutate) and scan range
     */
    public Put setFilter(ObTableFilter filter) throws Exception {
        return setFilterOnly(filter);
    }

    /*
     * get operation type
     */
    public ObTableOperationType getOperationType() {
        return ObTableOperationType.PUT;
    }

    /*
     * add mutated Row
     */
    public Put addMutateRow(Row rows) {
        if (null == rows) {
            throw new IllegalArgumentException("Invalid null rowKey set into Put");
        } else if (0 == rows.getMap().size()) {
            throw new IllegalArgumentException("input row key should not be empty");
        }

        // set mutate row into Put
        for (Map.Entry<String, Object> entry : rows.getMap().entrySet()) {
            columns.add(entry.getKey());
            values.add(entry.getValue());
        }

        return this;
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
     * add mutated ColumnValues
     */
    public Put addMutateColVal(ColumnValue... columnValues) throws Exception {
        if (null == columnValues) {
            throw new IllegalArgumentException("Invalid null columnValues set into Put");
        }

        // set mutate row into Put
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
    public Put removeRowkeyFromMutateColval() {
        removeRowkeyFromMutateColval(this.columns, this.values, this.rowKeyNames);
        return this;
    }

    /*
     * execute
     */
    public MutationResult execute() throws Exception {
        if (null == getTableName()) {
            throw new ObTableException("table name is null");
        } else if (null == getClient()) {
            throw new ObTableException("client is null");
        }
        removeRowkeyFromMutateColval(this.columns, this.values, this.rowKeyNames);
        if (null == getQuery()) {
            // simple Put, without filter
            return new MutationResult(((ObTableClient) getClient()).putWithResult(getTableName(),
                getRowKey(), columns.toArray(new String[0]), values.toArray()));
        } else {
            if (checkMutationWithFilter()) {
                // QueryAndPut
                ObTableOperation operation = ObTableOperation.getInstance(ObTableOperationType.PUT,
                    getRowKeyValues().toArray(), columns.toArray(new String[0]), values.toArray());
                return new MutationResult(((ObTableClient) getClient()).mutationWithFilter(
                    getQuery(), getRowKey(), operation, true));
            } else {
                throw new ObTableUnexpectedException("should set filter and scan range both");
            }
        }
    }
}

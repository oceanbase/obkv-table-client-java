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

public class Append extends Mutation<Append> {
    private List<String> columns;
    private List<Object> values;
    boolean              withResult;

    /*
     * default constructor
     */
    public Append() {
        super();
        columns = new ArrayList<String>();
        values = new ArrayList<Object>();
        withResult = false;
    }

    /*
     * construct with ObTableClient and String
     */
    public Append(Table client, String tableName) {
        super(client, tableName);
        columns = new ArrayList<String>();
        values = new ArrayList<Object>();
        withResult = false;
    }

    /*
     * get operation type
     */
    public ObTableOperationType getOperationType() {
        return ObTableOperationType.APPEND;
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
    public Append addMutateRow(Row rows) {
        if (null == rows) {
            throw new IllegalArgumentException("Invalid null rowKey set into Update");
        } else if (0 == rows.getMap().size()) {
            throw new IllegalArgumentException("input row key should not be empty");
        }

        // set mutate row into Update
        for (Map.Entry<String, Object> entry : rows.getMap().entrySet()) {
            columns.add(entry.getKey());
            values.add(entry.getValue());
        }

        return this;
    }

    /*
     * add mutated ColumnValues
     */
    public Append addMutateColVal(ColumnValue... columnValues) {
        if (null == columnValues) {
            throw new IllegalArgumentException("Invalid null columnValues set into Update");
        }

        // set mutate ColumnValue into Update
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
     * only using by execute()
     * get the selected columns of this mutation
     * TODO: can be removed after implement schema
     */
    protected String[] getSelectedColumns() throws Exception {
        if (null == getFilter()) {
            throw new ObTableException("filter is empty, only QueryAndMutate need selected columns");
        }

        // add name of row key
        List<String> selectedColumns = new ArrayList<>(getRowKeyName());
        // add name from filter
        addSelectedColumn(selectedColumns, getFilter());
        // add name from mutated row
        for (String column : columns) {
            if (!selectedColumns.contains(column)) {
                selectedColumns.add(column);
            }
        }

        return selectedColumns.toArray(new String[0]);
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

        if (null == getQuery()) {
            // simple update, without filter
            return new MutationResult(((ObTableClient) getClient()).appendWithResult(
                    getTableName(), getRowKey(), getKeyRanges(),
                    columns.toArray(new String[0]), values.toArray(), withResult));
        } else {
            // QueryAndAppend
            // getQuery().select(getSelectedColumns());
            return new MutationResult(((ObTableClient) getClient()).mutationWithFilter(getQuery(),
                    getRowKey(), getKeyRanges(), ObTableOperationType.APPEND,
                    columns.toArray(new String[0]), values.toArray(), withResult));

        }
    }
}

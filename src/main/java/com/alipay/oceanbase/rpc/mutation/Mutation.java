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
import com.alipay.oceanbase.rpc.filter.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Mutation<T> {
    private String        tableName;
    private Table         client;
    private Object[]      rowKey;
    private TableQuery    query;

    // TODO: remove rowKeysName and filter after implement schema
    private List<String>  rowKeyName;
    private ObTableFilter filter;

    /*
     * default constructor
     * recommend for batch operation
     */
    public Mutation() {
        tableName = null;
        client = null;
        rowKey = null;
        rowKeyName = null;
        query = null;
        filter = null;
    }

    /*
     * construct Mutation with client and tableName
     */
    public Mutation(Table client, String tableName) {
        if (null == client || null == tableName || tableName.isEmpty()) {
            throw new IllegalArgumentException("Invalid input to create Mutation in table"
                                               + tableName);
        }

        this.client = client;
        this.tableName = tableName;
        this.rowKey = null;
        this.rowKeyName = null;
        this.query = null;
    }

    /*
     * get client
     */
    protected Table getClient() {
        return client;
    }

    /*
     * get tableName
     */
    protected String getTableName() {
        return tableName;
    }

    /*
     * get query
     */
    protected TableQuery getQuery() {
        return query;
    }

    /*
     * get row key
     */
    protected Object[] getRowKey() {
        return rowKey;
    }

    /*
     * get row key name
     */
    protected List<String> getRowKeyName() {
        return rowKeyName;
    }

    /*
     * get filter
     */
    protected ObTableFilter getFilter() {
        return filter;
    }

    /*
     * get operation type
     */
    public ObTableOperationType getOperationType() {
        return null;
    }

    /*
     * add selected Column from filter
     * TODO: remove this function after implement table schema
     */
    protected void addSelectedColumn(List<String> selectedColumns, ObTableFilter filter)
                                                                                        throws Exception {
        if (filter instanceof ObTableFilterList) {
            for (int i = 0; i < ((ObTableFilterList) filter).size(); ++i) {
                addSelectedColumn(selectedColumns, ((ObTableFilterList) filter).get(i));
            }
        } else if (filter instanceof ObTableNotInFilter) {
            if (!selectedColumns.contains(((ObTableNotInFilter) filter).getColumnName())) {
                selectedColumns.add(((ObTableNotInFilter) filter).getColumnName());
            }
        } else if (filter instanceof ObTableInFilter) {
            if (!selectedColumns.contains(((ObTableInFilter) filter).getColumnName())) {
                selectedColumns.add(((ObTableInFilter) filter).getColumnName());
            }
        } else if (filter instanceof ObTableValueFilter) {
            if (!selectedColumns.contains(((ObTableValueFilter) filter).getColumnName())) {
                selectedColumns.add(((ObTableValueFilter) filter).getColumnName());
            }
        } else {
            throw new ObTableException("unknown filter type " + filter.toString());
        }
    }

    /*
     * only using by execute()
     * get the selected columns of this mutation
     * TODO: can be removed after implement schema
     */
    protected String[] getSelectedColumns() throws Exception {
        if (null == filter) {
            throw new ObTableException("filter is empty, only QueryAndMutate need selected columns");
        }

        // add name of row key
        List<String> selectedColumns = new ArrayList<>(rowKeyName);
        // add name from filter
        addSelectedColumn(selectedColumns, filter);

        return selectedColumns.toArray(new String[0]);
    }

    /*
     * set client
     */
    @SuppressWarnings("unchecked")
    public T setClient(ObTableClient client) {
        if (null == client) {
            throw new IllegalArgumentException("Invalid client to create Mutation");
        }

        this.client = client;

        return (T) this;
    }

    /*
     * set table
     */
    @SuppressWarnings("unchecked")
    public T setTable(String tableName) {
        if (null == tableName || tableName.isEmpty()) {
            throw new IllegalArgumentException("Invalid table name to create Mutation in table"
                                               + tableName);
        }

        this.tableName = tableName;

        return (T) this;
    }

    /*
     * set the Row Key of mutation with Row
     */
    @SuppressWarnings("unchecked")
    public T setRowKey(Row rowKey) {
        if (null == rowKey) {
            throw new IllegalArgumentException("Invalid null rowKey set into Mutation");
        } else if (0 == rowKey.getMap().size()) {
            throw new IllegalArgumentException("input row key should not be empty");
        }

        // set row key name into client and set rowKeys
        List<String> columnNames = new ArrayList<String>();
        List<Object> Keys = new ArrayList<Object>();
        for (Map.Entry<String, Object> entry : rowKey.getMap().entrySet()) {
            columnNames.add(entry.getKey());
            Keys.add(entry.getValue());
        }
        this.rowKeyName = columnNames;
        this.rowKey = Keys.toArray();

        // set row key in table
        if (null != tableName) {
            ((ObTableClient) client)
                .addRowKeyElement(tableName, columnNames.toArray(new String[0]));
        }

        // renew scan range of QueryAndMutate
        if (null != query) {
            query.addScanRange(this.rowKey, this.rowKey);
        }

        return (T) this;
    }

    /*
     * set the Row Key of mutation with ColumnValues
     */
    @SuppressWarnings("unchecked")
    public T setRowKey(ColumnValue... rowKey) {
        if (null == rowKey) {
            throw new IllegalArgumentException("Invalid null rowKey set into Mutation");
        }

        // set row key name into client and set rowKey
        List<String> columnNames = new ArrayList<String>();
        List<Object> Keys = new ArrayList<Object>();
        for (ColumnValue columnValue : rowKey) {
            if (columnNames.contains(columnValue.getColumnName())) {
                throw new ObTableException("Duplicate column in Row Key");
            }
            columnNames.add(columnValue.getColumnName());
            Keys.add(columnValue.getValue());
        }
        this.rowKeyName = columnNames;
        this.rowKey = Keys.toArray();

        // set row key in table
        if (null != tableName) {
            ((ObTableClient) client)
                .addRowKeyElement(tableName, columnNames.toArray(new String[0]));
        }

        // renew scan range of QueryAndMutate
        if (null != query) {
            query.addScanRange(rowKey, rowKey);
        }

        return (T) this;
    }

    /*
     * add filter into mutation (use QueryAndMutate)
     */
    @SuppressWarnings("unchecked")
    public T setFilter(ObTableFilter filter) throws Exception {
        if (null == filter) {
            throw new IllegalArgumentException("Invalid null filter set into Mutation");
        }

        if (null == query) {
            query = client.query(tableName);
            // set scan range if rowKey exist
            if (null != rowKey) {
                query.addScanRange(rowKey, rowKey);
            }
        }

        // only filter string in query works
        this.filter = filter;
        query.setFilter(filter);

        return (T) this;
    }
}

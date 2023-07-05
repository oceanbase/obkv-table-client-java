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
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Mutation<T> {
    private String         tableName;
    private Table          client;
    protected Object[]     rowKey;
    private TableQuery     query;
    private boolean        hasSetRowKey = false;
    protected List<String> rowKeyNames  = null;
    private boolean        isInsert     = false;
    /*
     * default constructor
     * recommend for batch operation
     */
    public Mutation() {
        tableName = null;
        client = null;
        rowKey = null;
        query = null;
        rowKeyNames = null;
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
        this.query = null;
        this.rowKeyNames = null;
    }

    /*
     * set is insert
     */
    protected void setInsert() {
        isInsert = true;
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
     * get key ranges
     */
    protected List<ObNewRange> getKeyRanges() {
        if (null != query) {
            return query.getObTableQuery().getKeyRanges();
        }
        return null;
    }

    /*
     * get operation type
     */
    public ObTableOperationType getOperationType() {
        return null;
    }

    /*
     * get rowkey names
     */
    public List<String> getRowKeyNames() {
        return rowKeyNames;
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
        if (hasSetRowKey) {
            throw new IllegalArgumentException("Could not set row key (scan range) twice");
        } else if (null == rowKey) {
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
        this.rowKey = Keys.toArray();
        this.rowKeyNames = columnNames;

        // set row key in table
        if (null != tableName) {
            ((ObTableClient) client)
                .addRowKeyElement(tableName, columnNames.toArray(new String[0]));
        }

        // renew scan range of QueryAndMutate
        if (null != query) {
            if (!isInsert) { // insert do not renew
                query.addScanRange(this.rowKey, this.rowKey);
            }
        }
        hasSetRowKey = true;
        return (T) this;
    }

    /*
     * set the Row Key of mutation with ColumnValues
     */
    @SuppressWarnings("unchecked")
    public T setRowKey(ColumnValue... rowKey) {
        if (hasSetRowKey) {
            throw new IllegalArgumentException("Could not set row key (scan range) twice");
        } else if (null == rowKey) {
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
        this.rowKey = Keys.toArray();
        this.rowKeyNames = columnNames;

        // set row key in table
        if (null != tableName) {
            ((ObTableClient) client)
                .addRowKeyElement(tableName, columnNames.toArray(new String[0]));
        }

        // renew scan range of QueryAndMutate
        if (null != query) {
            if (!isInsert) { // insert do not renew
                query.addScanRange(this.rowKey, this.rowKey);
            }
        }
        hasSetRowKey = true;
        return (T) this;
    }

    /*
     * add filter into mutation (use QueryAndMutate)
     */
    @SuppressWarnings("unchecked")
    public T setFilter(ObTableFilter filter) throws Exception {
        if (null == filter) {
            throw new IllegalArgumentException("Invalid null filter set into Mutation");
        } else if (null == client) {
            // do nothing
        } else {
            if (null == query) {
                query = client.query(tableName);
                // set scan range if rowKey exist
                if (null != rowKey) {
                    if (!isInsert) { // insert do not renew
                        query.addScanRange(this.rowKey, this.rowKey);
                    }
                }
            }
            // only filter string in query works
            query.setFilter(filter);
        }
        return (T) this;
    }

    /*
     * used for scan range (not ODP)
     */
    @SuppressWarnings("unchecked")
    public T setScanRangeColumns(String... columnNames) throws Exception {
        if (null == columnNames) {
            throw new IllegalArgumentException("Invalid null column names set into Mutation");
        }

        if (null == query) {
            query = client.query(tableName);
        }

        query.setScanRangeColumns(columnNames);

        // set row key in table
        if (null != tableName && null != client) {
            if (!((ObTableClient) client).isOdpMode()) {
                // TODO: adapt OCP
                //      OCP must conclude all rowkey now
                ((ObTableClient) client).addRowKeyElement(tableName, columnNames);
            }
        } else {
            throw new ObTableException("invalid table name: " + tableName + ", or invalid client: "
                                       + client + " while setting scan range columns");
        }

        return (T) this;
    }

    /*
     * add scan range
     */
    @SuppressWarnings("unchecked")
    public T addScanRange(Object start, Object end) throws Exception {
        if (null == start || null == end) {
            throw new IllegalArgumentException("Invalid null range set into Mutation");
        }

        return addScanRange(new Object[] { start }, true, new Object[] { end }, true);
    }

    /*
     * add list of scan range
     */
    @SuppressWarnings("unchecked")
    public T addScanRange(Object[] start, Object[] end) throws Exception {
        if (null == start || null == end) {
            throw new IllegalArgumentException("Invalid null range set into Mutation");
        }

        return addScanRange(start, true, end, true);
    }

    /*
     * add scan range with boundary
     */
    @SuppressWarnings("unchecked")
    public T addScanRange(Object start, boolean startEquals, Object end, boolean endEquals)
                                                                                           throws Exception {
        if (null == start || null == end) {
            throw new IllegalArgumentException("Invalid null range set into Mutation");
        }

        return addScanRange(new Object[] { start }, startEquals, new Object[] { end }, endEquals);
    }

    /*
     * add list of scan range with boundary
     */
    @SuppressWarnings("unchecked")
    public T addScanRange(Object[] start, boolean startEquals, Object[] end, boolean endEquals)
                                                                                               throws Exception {
        if (this.rowKey != null) {
            throw new IllegalArgumentException("Invalid scan range with row key");
        } else if (null == start || null == end) {
            throw new IllegalArgumentException("Invalid null range set into Mutation");
        }

        if (null == query) {
            query = client.query(tableName);
        }

        rowKey = null;
        query.addScanRange(start, startEquals, end, endEquals);

        hasSetRowKey = true;
        return (T) this;
    }

    static void removeRowkeyFromMutateColval(List<String> columns, List<Object> values,
                                             List<String> rowKeyNames) {
        if (null == columns || null == rowKeyNames || columns.size() != values.size()) {
            return;
        }
        for (int i = values.size() - 1; i >= 0; --i) {
            if (rowKeyNames.contains(columns.get(i))) {
                columns.remove(i);
                values.remove(i);
            }
        }
    }
}

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

import com.alipay.oceanbase.rpc.exception.ObTableException;

import java.util.*;

public class Row {
    private Map<String, Object> values;

    /*
     * default constructor
     */
    public Row() {
        values = new LinkedHashMap<String, Object>();
    }

    /*
     * construct with map
     */
    public Row(Map<String, Object> values) {
        if (null == values) {
            throw new ObTableException("input of Row is null");
        }
        this.values = values;
    }

    /*
     * construct with columnName and value
     */
    public Row(String columnName, Object value) {
        if (null == columnName || columnName.isEmpty()) {
            throw new ObTableException("column name is null");
        }

        values = new LinkedHashMap<String, Object>();
        values.put(columnName, value);
    }

    /*
     * construct with ColumnValue
     */
    public Row(ColumnValue... columnValues) {
        values = new LinkedHashMap<String, Object>();

        for (ColumnValue columnValue : columnValues) {
            if (values.containsKey(columnValue.getColumnName())) {
                throw new ObTableException("Duplicate column in row");
            }

            values.put(columnValue.getColumnName(), columnValue.getValue());
        }
    }

    /*
     * add column with String and Object
     */
    public Row add(String columnName, Object value) {
        if (null == columnName || columnName.isEmpty()) {
            throw new ObTableException("column name is null");
        }

        values.put(columnName, value);

        return this;
    }

    /*
     * add column with ColumnValue
     */
    public Row add(ColumnValue... columnValues) {
        for (ColumnValue columnValue : columnValues) {
            if (values.containsKey(columnValue.getColumnName())) {
                throw new ObTableException("Duplicate column in row");
            }

            values.put(columnValue.getColumnName(), columnValue.getValue());
        }

        return this;
    }

    /*
     * get columns' name
     */
    public String[] getColumns() {
        List<String> keys = new ArrayList<String>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            keys.add((entry.getKey()));
        }
        return keys.toArray(new String[0]);
    }

    /*
     * get values
     */
    public Object[] getValues() {
        List<Object> keys = new ArrayList<Object>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            keys.add((entry.getValue()));
        }
        return keys.toArray();
    }

    /*
     * get the column-value map
     */
    public Map<String, Object> getMap() {
        return values;
    }

    /*
     * get value from key
     */
    public Object get(String key) {
        return values.get(key);
    }

    /*
     * size of row
     */
    public long size() {
        return values.size();
    }
}

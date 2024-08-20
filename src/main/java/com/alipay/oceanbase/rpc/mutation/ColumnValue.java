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

public class ColumnValue {
    private String columnName;
    private Object value;

    /*
     * default constructor, columnName should not be empty
     */
    public ColumnValue(String columnName, Object value) {
        if (null == columnName || columnName.isEmpty()) {
            throw new ObTableException("column name is null");
        }

        this.columnName = columnName;
        this.value = value;
    }

    /*
     * get the columnName
     */
    public String getColumnName() {
        return columnName;
    }

    /*
     * get the value
     */
    public Object getValue() {
        return value;
    }
}

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

package com.alipay.oceanbase.rpc.filter;

import com.alipay.oceanbase.rpc.exception.ObTableException;

public class ObTableValueFilter extends ObTableFilter {
    private ObCompareOp op;
    private String      columnName;
    private Object      value;

    /*
     * construct with ObCompareOp / String / Object
     */
    public ObTableValueFilter(ObCompareOp op, String columnName, Object value)
                                                                              throws ObTableException {
        if (op == ObCompareOp.IS || op == ObCompareOp.IS_NOT) {
            if (value != null) {
                throw new ObTableException(String.format("the value of compare op %s must be null",
                    op.toString()));
            }
        } else if (value == null) {
            throw new ObTableException(String.format(
                "the value of comparer op %s must not be null", op.toString()));
        }

        this.op = op;
        this.columnName = columnName;
        this.value = value;
    }

    /*
     * set filter
     */
    public void set(ObCompareOp op, String columnName, Object value) throws ObTableException {
        if (op == ObCompareOp.IS || op == ObCompareOp.IS_NOT) {
            if (value != null) {
                throw new ObTableException(String.format("the value of %s must be null",
                    op.toString()));
            }
        } else if (value == null) {
            throw new ObTableException(String.format("the value of %s must not be null",
                op.toString()));
        }
        this.op = op;
        this.columnName = columnName;
        this.value = value;
    }

    /*
     * get column name
     */
    public String getColumnName() {
        return columnName;
    }

    /*
     * to string
     */
    public String toString() {
        StringBuilder filterString = new StringBuilder();

        // handle empty op / columnName
        if (null == op || null == columnName || columnName.isEmpty()) {
            return null;
        }

        filterString.append(TABLE_COMPARE_FILTER);
        filterString.append("(");
        filterString.append(op.toString());
        filterString.append(", '");
        filterString.append(columnName);
        filterString.append(":");
        if (value != null) {
            filterString.append(String.valueOf(value));
        }
        filterString.append("')");

        return filterString.toString();
    }
}
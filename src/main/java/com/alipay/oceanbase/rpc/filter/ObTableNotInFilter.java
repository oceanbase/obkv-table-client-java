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

import java.util.List;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.andList;
import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.compareVal;

public class ObTableNotInFilter extends ObTableFilter {
    private String columnName;
    private Object[] values;

    /**
     * construct with String / Object[]
     * @param columnName columnName
     * @param values values
     */
    public ObTableNotInFilter(String columnName, Object... values) {
        if (null == columnName || columnName.isEmpty()) {
            throw new ObTableException("column name is null");
        }

        if (null == values || values.length == 0){
            throw new ObTableException("not in filter values should not be empty");
        }

        this.columnName = columnName;
        this.values = values;
    }

    /**
     * construct with String / List<Object>
     * @param columnName column name
     * @param values values
     */
    public ObTableNotInFilter(String columnName, List<Object> values) {
        if (null == columnName || columnName.isEmpty()) {
            throw new ObTableException("column name is null");
        }

        if (null == values || values.isEmpty()){
            throw new ObTableException("not in filter values should not be empty");
        }

        this.columnName = columnName;
        this.values = values.toArray();
    }

    public String toString() {
        ObTableFilterList filterList = andList();

        for (Object value : values) {
            filterList.addFilter(compareVal(ObCompareOp.NE, columnName, value));
        }

        return filterList.toString();
    }
}

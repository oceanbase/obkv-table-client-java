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

import java.util.List;

/**
 * ObTableFilterFactory is used to create filter without complex procedure
 * Suggest all filter should create by this util class
 */
public class ObTableFilterFactory {
    public static ObTableFilterList andList(ObTableFilter... filters) {
        return new ObTableFilterList(ObTableFilterList.operator.AND, filters);
    }

    public static ObTableFilterList orList(ObTableFilter... filters) {
        return new ObTableFilterList(ObTableFilterList.operator.OR, filters);
    }

    public static ObTableValueFilter compareVal(ObCompareOp op, String columnName, Object value) {
        return new ObTableValueFilter(op, columnName, value);
    }

    public static ObTableInFilter in(String columnName, Object... values) {
        return new ObTableInFilter(columnName, values);
    }

    public static ObTableInFilter in(String columnName, List<Object> values) {
        return new ObTableInFilter(columnName, values);
    }

    public static ObTableNotInFilter notIn(String columnName, Object... values) {
        return new ObTableNotInFilter(columnName, values);
    }

    public static ObTableNotInFilter notIn(String columnName, List<Object> values) {
        return new ObTableNotInFilter(columnName, values);
    }
}

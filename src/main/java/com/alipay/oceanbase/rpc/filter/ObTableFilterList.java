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

import java.util.ArrayList;
import java.util.List;

public class ObTableFilterList extends ObTableFilter {
    public enum operator {
        AND, OR;
    };

    private operator            op;
    private List<ObTableFilter> filters;

    /*
     * default constructor with op = AND
     */
    public ObTableFilterList() {
        op = operator.AND;
        filters = new ArrayList<ObTableFilter>();
    }

    /*
     * construct with ObCompareOp
     */
    public ObTableFilterList(operator op) {
        if (null == op) {
            throw new ObTableException("operator is null");
        }
        this.op = op;
        this.filters = new ArrayList<ObTableFilter>();
    }

    /**
     * construct with ObCompareOp / ObTableFilter
     * @param op filter operation
     * @param filters table filters
     */
    public ObTableFilterList(operator op, ObTableFilter... filters) {
        if (null == op) {
            throw new ObTableException("operator is null");
        }
        this.op = op;
        this.filters = new ArrayList<ObTableFilter>();

        for (int i = 0; i < filters.length; ++i) {
            if (null == filters[i]) {
                throw new ObTableException(i + "-th filter is null");
            }
            this.filters.add(filters[i]);
        }
    }

    /*
     * add filter
     */
    public void addFilter(ObTableFilter... filters) {
        for (int i = 0; i < filters.length; ++i) {
            if (null == filters[i] || this == filters[i]) {
                throw new ObTableException(i + "-th input filter is illegal ");
            }
            this.filters.add(filters[i]);
        }
    }

    /*
     * get the size of filter list
     */
    public int size() {
        return filters.size();
    }

    /*
     * get the pos-th filter of filter list
     */
    public ObTableFilter get(int pos) {
        if (pos >= size()) {
            throw new ObTableException("pos:" + pos + " is out of range: " + size());
        }
        return filters.get(pos);
    }

    /*
     * to string
     */
    public String toString() {
        StringBuilder filtersString = new StringBuilder();
        String stringOp;

        if (operator.AND == op) {
            stringOp = " && ";
        } else {
            stringOp = " || ";
        }

        for (int i = 0; i < filters.size(); ++i) {
            String filterString = filters.get(i).toString();
            if (null == filterString || filterString.isEmpty()) {
                continue;
            } else {
                if (0 != i) {
                    filtersString.append(stringOp);
                }
                if (filters.get(i) instanceof ObTableValueFilter) {
                    filtersString.append(filterString);
                } else {
                    filtersString.append("(");
                    filtersString.append(filterString);
                    filtersString.append(")");
                }
            }
        }
        // if filtersString is empty then return ""
        return filtersString.toString();
    }
}
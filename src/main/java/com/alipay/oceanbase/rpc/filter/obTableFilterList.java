package com.alipay.oceanbase.rpc.filter;

import com.alipay.oceanbase.rpc.exception.ObTableException;

import java.util.ArrayList;
import java.util.List;

public class obTableFilterList extends obTableFilter {
    public enum operator {
        AND, OR;
    };

    private operator op;
    private List<obTableFilter> filters;

    /*
     * default constructor with op = AND
     */
    public obTableFilterList() {
        op = operator.AND;
        filters = new ArrayList<obTableFilter>();
    }

    /*
     * construct with obCompareOp
     */
    public obTableFilterList(operator op) {
        if (null == op) {
            throw new ObTableException("operator is null");
        }
        this.op = op;
        this.filters = new ArrayList<obTableFilter>();
    }

    /*
     * construct with obCompareOp / obTableFilter
     */
    public obTableFilterList(operator op, obTableFilter... filters) {
        if (null == op) {
            throw new ObTableException("operator is null");
        }
        this.op = op;
        this.filters = new ArrayList<obTableFilter>();

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
    public void addFilter(obTableFilter... filters) {
        for (int i = 0; i < filters.length; ++i) {
            if (null == filters[i] || this == filters[i]) {
                throw new ObTableException(i + "-th input filter is illegal ");
            }
            this.filters.add(filters[i]);
        }
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
            if (null == filterString || filterString.isEmpty() ) {
                continue;
            } else {
                if (0 != i) {
                    filtersString.append(stringOp);
                }
                if (filters.get(i) instanceof obTableFilterList) {
                    filtersString.append("(");
                    filtersString.append(filterString);
                    filtersString.append(")");
                } else {
                    filtersString.append(filterString);
                }
            }
        }
        // if filtersString is empty then return ""
        return filtersString.toString();
    }
}
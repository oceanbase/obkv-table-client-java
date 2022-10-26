package com.alipay.oceanbase.rpc.filter;

import com.alipay.oceanbase.rpc.exception.ObTableException;

public class obTableValueFilter extends obTableFilter {
    private obCompareOp op;
    private String key;
    private String value;

    /*
     * construct with obCompareOp / String / String
     */
    public obTableValueFilter(obCompareOp op, String key, String value) {
        this.op = op;
        this.key = key;
        this.value = value;
    }

    /*
     * construct with obCompareOp / String / int
     */
    public obTableValueFilter(obCompareOp op, String key, int value) {
        this.op = op;
        this.key = key;
        this.value = String.valueOf(value);
    }

    /*
     * construct with obCompareOp / String / long
     */
    public obTableValueFilter(obCompareOp op, String key, long value) {
        this.op = op;
        this.key = key;
        this.value = String.valueOf(value);
    }

    /*
     * construct with obCompareOp / String / short
     */
    public obTableValueFilter(obCompareOp op, String key, short value) {
        this.op = op;
        this.key = key;
        this.value = String.valueOf(value);
    }

    /*
     * set filter
     */
    public void set(obCompareOp op, String key, String value) {
        this.op = op;
        this.key = key;
        this.value = value;
    }

    /*
     * set filter
     */
    public void set(obCompareOp op, String key, int value) {
        this.op = op;
        this.key = key;
        this.value = String.valueOf(value);
    }

    /*
     * set filter
     */
    public void set(obCompareOp op, String key, long value) {
        this.op = op;
        this.key = key;
        this.value = String.valueOf(value);
    }

    /*
     * set filter
     */
    public void set(obCompareOp op, String key, short value) {
        this.op = op;
        this.key = key;
        this.value = String.valueOf(value);
    }

    /*
     * to string
     */
    public String toString() {
        StringBuilder filterString = new StringBuilder();

        // handle empty op / key / value
        if (null == op || null == key || null == value || key.isEmpty() || value.isEmpty()) {
            return null;
        }

        filterString.append(TABLE_COMPARE_FILTER);
        filterString.append("(");
        filterString.append(op.toString());
        filterString.append(", '");
        filterString.append(key);
        filterString.append(":");
        filterString.append(value);
        filterString.append("')");

        return filterString.toString();
    }
}
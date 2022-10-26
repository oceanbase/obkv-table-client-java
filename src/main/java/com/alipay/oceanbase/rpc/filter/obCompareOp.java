package com.alipay.oceanbase.rpc.filter;

public enum obCompareOp {
    /*
     * LT -> less than
     */
    LT,

    /*
     * GT -> greater than
     */
    GT,

    /*
     * LE -> less or equal than
     */
    LE,

    /*
     * GE -> greater or equal than
     */
    GE,

    /*
     * NE -> not equal
     */
    NE,

    /*
     * EQ -> equal
     */
    EQ;

    /*
     * operator to string
     */
    public String toString() {
        String returnString = null;
        // switch sign
        switch (this) {
            case LT:
                returnString = "<";
                break;
            case GT:
                returnString =  ">";
                break;
            case LE:
                returnString = "<=";
                break;
            case GE:
                returnString = ">=";
                break;
            case NE:
                returnString = "!=";
                break;
            case EQ:
                returnString = "=";
                break;
            default:
                returnString = "";
                break;
        }
        return returnString;
    }
}

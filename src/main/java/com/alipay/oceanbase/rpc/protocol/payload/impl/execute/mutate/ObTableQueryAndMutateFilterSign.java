package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate;

public enum ObTableQueryAndMutateFilterSign {
    /*
     * LT -> less than
     * GT -> greater than
     * LE -> less equal than
     * GE -> greater equal than
     * NE -> not equal
     * EQ -> equal
     */
    LT, GT, LE, GE, NE, EQ;
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
};
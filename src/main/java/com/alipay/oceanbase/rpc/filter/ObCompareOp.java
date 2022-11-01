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

public enum ObCompareOp {
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

/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
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

package com.alipay.oceanbase.rpc.dds.parser;

public enum DistributeRuleSimpleToken {

    COMMA(","), LPAREN("("), RPAREN(")"), SUB("-"), PLUS("+"), LITERAL_HEX, LITERAL_FLOAT, LITERAL_INT, IDENTIFIER, ERROR, EOF;

    public final String name;

    DistributeRuleSimpleToken() {
        this(null);
    }

    /**
     * get DistributeRuleSimpleToken name
     * @param name
     */
    DistributeRuleSimpleToken(String name) {
        this.name = name;
    }
}

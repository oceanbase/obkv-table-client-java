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

package com.alipay.oceanbase.rpc.protocol.payload.impl.parser;

import java.util.HashMap;
import java.util.Map;

public enum ObGeneratedColumnFuncName {
    UNKNOWN("unknown"), SUB_STR("substr");

    public final String name;

    ObGeneratedColumnFuncName(String name) {
        this.name = name;
    }

    private static final Map<String, ObGeneratedColumnFuncName> tokenMap = new HashMap<String, ObGeneratedColumnFuncName>();

    static {
        for (ObGeneratedColumnFuncName token : ObGeneratedColumnFuncName.values()) {
            tokenMap.put(token.name, token);
        }
    }

    /*
     * Func to token.
     */
    public static ObGeneratedColumnFuncName funcToToken(String func) {
        ObGeneratedColumnFuncName token = tokenMap.get(func);
        if (token != null) {
            return token;
        } else {
            return UNKNOWN;
        }
    }
}

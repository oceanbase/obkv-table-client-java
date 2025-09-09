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

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhiqi.zzq
 * @since 2018/8/20 下午11:09
 */
public enum DistributeRuleFuncName {
    UNKNOWN("unknown"),

    SUB_STR("substr"),

    HASH_STR("hash"),

    HASH_DIVISION_STR("hash_division"),

    HASH_SALT("hash_salt"),

    HASH_SALT_DIVISION("hash_salt_division"),

    HASH_SUB_STR("hash_substr"),

    HASH_MULTIPLE_FIELD("hash_multiple_field"),

    SUB_STR_LOAD_TEST("substr_loadtest"),

    SUB_STR_REVERSE("substr_reverse"),

    SUB_STR_REVERSE_LOAD_TEST("substr_reverse_loadtest"),

    TO_INT("toint");

    public final String name;

    DistributeRuleFuncName(String name) {
        this.name = name;
    }

    private static final Map<String, DistributeRuleFuncName> tokenMap = new HashMap<String, DistributeRuleFuncName>();

    static {
        for (DistributeRuleFuncName token : DistributeRuleFuncName.values()) {
            tokenMap.put(token.name, token);
        }
    }

    /**
     * Func to token.
     */
    public static DistributeRuleFuncName funcToToken(String func) {
        DistributeRuleFuncName token = tokenMap.get(func);
        if (token != null) {
            return token;
        } else {
            return UNKNOWN;
        }
    }
}

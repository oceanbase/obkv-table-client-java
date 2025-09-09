/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2023 Ant Financial Services Group
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

/*
 * Ant Group
 * Copyright (c) 2004-2024 All Rights Reserved.
 */
package com.alipay.oceanbase.rpc.dds.parser;

import java.util.List;

/**
 * * hash前先对字段进行取指定的位数。
 * 本表达式可以看成 hash(substr(K,1,2), 10)的嵌套.
 * hash_substr(K, 1, 32, 10) -> 取 K 字段的前 32位，做 hash 取模 10
 *
 * @author stream
 * @version DistributeRuleHashSubStrFunc.java, v 0.1 2024年10月12日 17:31 stream
 */
public class DistributeRuleHashSubStrFunc extends AbstractHashStrFunc implements
                                                                     DistributeRuleSimpleFunc {

    private int                      mod = 1;
    private DistributeRuleSubStrFunc subStrFunc;

    public DistributeRuleHashSubStrFunc() {
        super(DistributeRuleFuncName.HASH_SUB_STR);
        subStrFunc = new DistributeRuleSubStrFunc();
    }

    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        if (parameters.size() < 4) {
            throw new IllegalArgumentException("hashSubStr argument size must be bigger than 4 "
                                               + parameters);
        }

        //设置列名
        setColumnNames(parameters);

        //设置字符串分割表达式
        subStrFunc.setParameters(parameters.subList(0, 3));

        //获取mod值
        this.mod = getPositionNum(parameters.get(3), "fourth");
    }

    @Override
    public int getMinParameters() {
        return 4;
    }

    @Override
    public int getMaxParameters() {
        return 4;
    }

    @Override
    public List<String> getRefColumnNames() {
        return this.refColumnNames;
    }

    @Override
    public Object evalValue(List<Object> refs) throws IllegalArgumentException {
        if (refs.size() != refColumnNames.size()) {
            throw new IllegalArgumentException("DistributeRuleHashSubStrFunc is refer to "
                                               + refColumnNames
                                               + " so that the length of the refs must be equal "
                                               + refColumnNames.size() + ". refs:" + refs);
        }

        //先算 subStr
        Object subValue = subStrFunc.evalValue(refs.subList(0, 1));

        //再算 hash 取模
        return hashValue((String) subValue) % mod;
    }

    @Override
    public String toString() {
        return "DistributeRuleHashSubStrFunc{" + "refColumnNames=" + refColumnNames + ", mod="
               + mod;
    }

}

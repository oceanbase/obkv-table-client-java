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
 * Copyright (c) 2004-2023 All Rights Reserved.
 */
package com.alipay.oceanbase.rpc.dds.parser;

import java.util.List;

/**
 * * hash前拼接salt值，再模指定的数值
 * * hash_salt(c1,mySalt,100) -> hash(c1Value + "mySalt") 再模100
 *
 * @author stream
 * @version DistributeRuleHashSaltFunc.java, v 0.1 2023年07月27日 17:31 stream
 */
public class DistributeRuleHashSaltFunc extends AbstractHashStrFunc implements
                                                                   DistributeRuleSimpleFunc {

    private String saltValue = "";

    private int    mod       = 1;

    public DistributeRuleHashSaltFunc() {
        super(DistributeRuleFuncName.HASH_SALT);
    }

    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        if (parameters.size() < 3) {
            throw new IllegalArgumentException("hashSalt argument size must be bigger than 3 "
                                               + parameters);
        }

        //设置列名
        setColumnNames(parameters);

        //获取salt值
        this.saltValue = getPositionStr(parameters.get(1), "second");

        //获取mod值
        this.mod = getPositionNum(parameters.get(2), "third");
    }

    @Override
    public int getMinParameters() {
        return 3;
    }

    @Override
    public int getMaxParameters() {
        return 3;
    }

    @Override
    public List<String> getRefColumnNames() {
        return this.refColumnNames;
    }

    @Override
    public Object evalValue(List<Object> refs) throws IllegalArgumentException {
        //先拼接salt，再取模
        return evalHashValue(refs, saltValue) % mod;
    }

    @Override
    public String toString() {
        return "DistributeRuleHashSaltFunc{" + "refColumnNames=" + refColumnNames + ", saltValue="
               + saltValue + ", mod=" + mod;
    }
}

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

import java.util.List;

/**
 * @author zhiqi.zzq
 * @since 2018/8/20 下午6:14
 */
public class DistributeRuleHashStrFunc extends AbstractHashStrFunc implements
                                                                  DistributeRuleSimpleFunc {

    private int mod = 1;

    public DistributeRuleHashStrFunc() {
        super(DistributeRuleFuncName.HASH_STR);
    }

    /**
     * Set parameters.
     */
    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        if (parameters.size() < 2) {
            throw new IllegalArgumentException("hashStr argument size must be bigger than 2 "
                                               + parameters);
        }
        //设置列名
        setColumnNames(parameters);

        //获取mod值
        this.mod = getPositionNum(parameters.get(1), "second");
    }

    /**
     * Get min parameters.
     */
    @Override
    public int getMinParameters() {
        return 2;
    }

    /**
     * Get max parameters.
     */
    @Override
    public int getMaxParameters() {
        return 2;
    }

    /**
     * Get ref column names.
     */
    @Override
    public List<String> getRefColumnNames() {
        return refColumnNames;
    }

    /**
     * Eval value.
     */
    @Override
    public Object evalValue(List<Object> refs) throws IllegalArgumentException {
        return evalHashValue(refs) % mod;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return "DistributeRuleSubStrFunc{" + "refColumnNames=" + refColumnNames + ", mod=" + mod;
    }
}

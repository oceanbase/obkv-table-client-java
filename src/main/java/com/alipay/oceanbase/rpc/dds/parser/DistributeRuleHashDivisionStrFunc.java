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
 * hash并执行除法操作
 * hash_division(c1,100,10) -> c1的value执行hashCode后，模100，再除以10
 *
 * @author stream
 * @version DistributeRuleHashDivisionStrFunc.java, v 0.1 2023年07月11日 16:11 stream
 */
public class DistributeRuleHashDivisionStrFunc extends AbstractHashStrFunc implements
                                                                          DistributeRuleSimpleFunc {
    private int mod     = 1;
    private int divisor = 1;

    public DistributeRuleHashDivisionStrFunc() {
        super(DistributeRuleFuncName.HASH_DIVISION_STR);
    }

    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        if (parameters.size() < 3) {
            throw new IllegalArgumentException(
                "hashDivisionStr argument size must be bigger than 3 " + parameters);
        }

        //设置列名
        setColumnNames(parameters);

        //获取mod值
        this.mod = getPositionNum(parameters.get(1), "second");

        //获取除数
        this.divisor = getPositionNum(parameters.get(2), "third");
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
        //先对列的值取模,再除以指定的值
        return evalHashValue(refs) % mod / divisor;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return className + "{" + "refColumnNames=" + refColumnNames + ", mod=" + mod + ", divisor="
               + divisor;
    }
}

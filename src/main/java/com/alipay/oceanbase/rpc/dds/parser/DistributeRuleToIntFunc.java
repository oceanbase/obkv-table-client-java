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

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhiqi.zzq
 * @since 2018/8/20 下午6:14
 */
public class DistributeRuleToIntFunc implements DistributeRuleSimpleFunc {

    private List<String> refColumnNames = new ArrayList<String>();

    private Integer      result;

    /**
     * Set parameters.
     */
    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        Object parameter1 = parameters.get(0);

        if (!(parameter1 instanceof Long)) {
            throw new IllegalArgumentException("toInt first argument must be Long " + parameter1);
        }
        Long ref = (Long) parameters.get(0);
        result = ref.intValue();
    }

    /**
     * Get min parameters.
     */
    @Override
    public int getMinParameters() {
        return 1;
    }

    /**
     * Get max parameters.
     */
    @Override
    public int getMaxParameters() {
        return 1;
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
        return result;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return "DistributeRuleToIntFunc{" + "refColumnNames=" + refColumnNames + ", result="
               + result + '}';
    }
}

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
 * @since 2018/8/20 下午5:13
 */
public class DistributeRuleReferFunc implements DistributeRuleSimpleFunc {
    private final List<String> refColumnNames;

    /**
     * Ob generated column refer func.
     */
    public DistributeRuleReferFunc(String columnName) {
        List<String> refColumnNameList = new ArrayList<String>(1);
        refColumnNameList.add(columnName);
        this.refColumnNames = refColumnNameList;
    }

    /**
     * Set parameters.
     */
    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        //ignore
    }

    /**
     * Get min parameters.
     */
    @Override
    public int getMinParameters() {
        return 0;
    }

    /**
     * Get max parameters.
     */
    @Override
    public int getMaxParameters() {
        return 0;
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
        if (refs.size() == 0 || refs.size() > 1) {
            throw new IllegalArgumentException(
                "DistributeRuleReferFunc is refer to itself so that the length of the refs must be 1. refs:"
                        + refs);
        }

        return refs.get(0);
    }
}

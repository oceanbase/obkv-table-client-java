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
 *
 */
public interface DistributeRuleSimpleFunc {

    /**
     *
     * @param parameters
     * @throws IllegalArgumentException
     */
    void setParameters(List<Object> parameters) throws IllegalArgumentException;

    /**
     *
     * @return
     */
    int getMinParameters();

    /**
     *
     * @return
     */
    int getMaxParameters();

    /**
     *
     * @return
     */
    List<String> getRefColumnNames();

    /**
     *
     * @param refs
     * @return
     * @throws IllegalArgumentException
     */
    Object evalValue(List<Object> refs) throws IllegalArgumentException;

}

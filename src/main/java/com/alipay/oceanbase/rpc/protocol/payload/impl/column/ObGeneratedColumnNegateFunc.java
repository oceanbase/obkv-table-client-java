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

package com.alipay.oceanbase.rpc.protocol.payload.impl.column;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;

import java.util.ArrayList;
import java.util.List;

public class ObGeneratedColumnNegateFunc implements ObGeneratedColumnSimpleFunc {
    private final List<String> refColumnNames;

    /*
     * Ob generated column refer func.
     */
    public ObGeneratedColumnNegateFunc(String refColumn) {
        List<String> refColumnNameList = new ArrayList<String>(1);
        refColumnNameList.add(refColumn);
        this.refColumnNames = refColumnNameList;
    }

    /*
     * Set parameters.
     */
    @Override
    public void setParameters(List<Object> parameters) {
        //ignore
    }

    /*
     * Get min parameters.
     */
    @Override
    public int getMinParameters() {
        return 0;
    }

    /*
     * Get max parameters.
     */
    @Override
    public int getMaxParameters() {
        return 0;
    }

    /*
     * Get ref column names.
     */
    @Override
    public List<String> getRefColumnNames() {
        return refColumnNames;
    }

    /*
     * Eval value.
     */
    @Override
    public Object evalValue(ObCollationType collationType, Object... refs)
                                                                          throws IllegalArgumentException {
        if (refs == null || refs.length == 0) {
            throw new IllegalArgumentException("Input references cannot be null or empty");
        }
        Object ref = refs[0];
        if (ref instanceof Long) {
            return -(Long) ref;
        } else if (ref instanceof Integer) {
            Integer value = (Integer) ref;
            if (value == Integer.MIN_VALUE) {
                throw new IllegalArgumentException(
                    "The currently provided parameter is the "
                            + "minimum value of the Integer type, and its negation will cause an overflow.");
            }
            return -(Integer) value;
        } else {
            throw new IllegalArgumentException("Object [" + ref + "] can not evaluate by"
                                               + " ObGeneratedColumnNegateFunc");
        }
    }
}

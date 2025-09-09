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

import com.alipay.oceanbase.rpc.util.ObVString;
import com.alipay.oceanbase.rpc.util.Serialization;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author stream
 * @version AbstractHashStrFunc.java, v 0.1 2023年07月11日 16:13 stream
 */
abstract class AbstractHashStrFunc {
    private final String   funcName;
    protected final String className;

    protected List<String> refColumnNames = new ArrayList<String>();

    public AbstractHashStrFunc(DistributeRuleFuncName funcName) {
        //运行时获取实现类的类名,用于打印日志
        this.className = this.getClass().getSimpleName();
        this.funcName = funcName.name;
    }

    protected void setColumnNames(List<Object> parameters) throws IllegalArgumentException {
        Object parameter1 = parameters.get(0);

        if (!(parameter1 instanceof String)) {
            throw new IllegalArgumentException(funcName
                                               + " first argument must be column or string "
                                               + parameter1);
        }
        String ref = (String) parameters.get(0);
        refColumnNames.add(ref);
    }

    /**
     * 获取表达式里指定位置的字符
     *
     * @param value
     * @param posStr
     * @return
     */
    protected String getPositionStr(Object value, String posStr) {
        Objects.requireNonNull(value, funcName + " " + posStr + " argument can not be null ");
        return String.valueOf(value);
    }

    /**
     * 获取表达式里指定位置的数值
     *
     * @param num    parameter里的对象
     * @param posStr 用于打印日志
     * @return 正整数
     */
    protected int getPositionNum(Object num, String posStr) {
        if (!((num instanceof Long || (num instanceof Integer)))) {
            throw new IllegalArgumentException(funcName + " " + posStr
                                               + " argument pos must be int " + num);
        }

        Long numLong = ((Number) num).longValue();

        if (numLong == 0 || numLong > Integer.MAX_VALUE || numLong < Integer.MIN_VALUE) {
            throw new IllegalArgumentException(funcName + " " + posStr
                                               + " argument pos must be int exclude zero");
        }

        return numLong.intValue();
    }

    protected int evalHashValue(List<Object> refs) throws IllegalArgumentException {
        String evalStr = getEvalStr(refs);
        return hashValue(evalStr);
    }

    protected int evalHashValue(List<Object> refs, String salt) throws IllegalArgumentException {
        String evalStr = getEvalStr(refs);
        return hashValue(evalStr + salt);
    }

    private String getEvalStr(List<Object> refs) {
        if (refs.size() != refColumnNames.size()) {
            throw new IllegalArgumentException(className + " is refer to " + refColumnNames
                                               + " so that the length of the refs must be equal "
                                               + refColumnNames.size() + ". refs:" + refs);
        }
        Object ref = refs.get(0);

        String evalStr;
        if (ref instanceof String) {
            evalStr = (String) ref;
        } else if (ref instanceof byte[]) {
            evalStr = Serialization.decodeVString((byte[]) ref);
        } else if (ref instanceof ObVString) {
            evalStr = ((ObVString) ref).getStringVal();
        } else {
            throw new IllegalArgumentException("Object [" + ref + "] can not evaluate by "
                                               + className);
        }
        return evalStr;
    }

    protected int hashValue(String evalStr) {
        int hash = evalStr.hashCode();
        //避免hashCode为负数,并考虑最小值的影响
        if (hash == Integer.MIN_VALUE) {
            hash = Integer.MAX_VALUE;
        }
        return Math.abs(hash);
    }

}

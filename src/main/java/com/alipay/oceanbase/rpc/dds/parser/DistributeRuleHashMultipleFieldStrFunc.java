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

/**
 * @author ly429391
 * @since 2024/3/12
 */
public class DistributeRuleHashMultipleFieldStrFunc extends AbstractHashStrFunc implements
                                                                               DistributeRuleSimpleFunc {
    private final String   funcName;

    protected final String className;

    protected List<String> refColumnNames = new ArrayList<String>();

    // total table number
    private int            mod            = 1;

    // table number per database
    private int            divisor        = 1;

    public DistributeRuleHashMultipleFieldStrFunc() {
        super(DistributeRuleFuncName.HASH_MULTIPLE_FIELD);
        //运行时获取实现类的类名,用于打印日志
        this.className = this.getClass().getSimpleName();
        this.funcName = DistributeRuleFuncName.HASH_MULTIPLE_FIELD.name;
    }

    protected void setColumnNames(List<Object> parameters) throws IllegalArgumentException {
        for (int i = 0; i < parameters.size() - 2; i++) {
            Object parameter = parameters.get(i);

            if (!(parameter instanceof String)) {
                throw new IllegalArgumentException(
                    funcName + ": Except for the last parameter, all other parameters "
                            + "must be strings, error parameter:" + parameter);
            }
            String ref = (String) parameters.get(i);
            refColumnNames.add(ref);
        }
    }

    protected int evalHashValue(List<Object> refs) throws IllegalArgumentException {
        String evalStr = getEvalStr(refs);
        return hashValue(evalStr);
    }

    protected int evalHashValue(List<Object> refs, String salt) throws IllegalArgumentException {
        String evalStr = getEvalStr(refs);
        return hashValue(evalStr + salt);
    }

    public String getEvalStr(List<Object> refs) {
        if (refs.size() != refColumnNames.size()) {
            throw new IllegalArgumentException(className + " is refer to " + refColumnNames
                                               + " so that the length of the refs must be equal "
                                               + refColumnNames.size() + ". refs:" + refs);
        }

        StringBuilder evalStr = new StringBuilder();
        for (int i = 0; i < refs.size(); i++) {
            Object ref = refs.get(i);

            if (ref instanceof String) {
                evalStr.append((String) ref);
            } else if (ref instanceof byte[]) {
                evalStr.append(Serialization.decodeVString((byte[]) ref));
            } else if (ref instanceof ObVString) {
                evalStr.append(((ObVString) ref).getStringVal());
            } else {
                throw new IllegalArgumentException("Object [" + ref + "] can not evaluate by "
                                                   + className);
            }
        }
        return evalStr.toString();
    }

    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        if (parameters.size() < 3) {
            throw new IllegalArgumentException("hashStr argument size must be bigger than 3 "
                                               + parameters);
        }
        //设置列名
        setColumnNames(parameters);

        //获取mod值
        this.mod = getPositionNum(parameters.get(parameters.size() - 2), "mod");
        this.divisor = getPositionNum(parameters.get(parameters.size() - 1), "divisor");
    }

    @Override
    public int getMinParameters() {
        return 3;
    }

    @Override
    public int getMaxParameters() {
        return 13;
    }

    @Override
    public List<String> getRefColumnNames() {
        return refColumnNames;
    }

    @Override
    public Object evalValue(List<Object> refs) throws IllegalArgumentException {
        if (refs.size() != refColumnNames.size()) {
            throw new IllegalArgumentException(className + " is refer to " + refColumnNames
                                               + " so that the length of the refs must be equal "
                                               + refColumnNames.size() + ". refs:" + refs);
        }

        StringBuilder evalStr = new StringBuilder();
        for (int i = 0; i < refs.size(); i++) {
            Object ref = refs.get(i);

            if (ref instanceof String) {
                evalStr.append((String) ref);
            } else if (ref instanceof byte[]) {
                evalStr.append(Serialization.decodeVString((byte[]) ref));
            } else if (ref instanceof ObVString) {
                evalStr.append(((ObVString) ref).getStringVal());
            } else {
                throw new IllegalArgumentException("Object [" + ref + "] can not evaluate by "
                                                   + className);
            }
        }

        return hashValue(evalStr.toString()) % mod / divisor;
    }
}

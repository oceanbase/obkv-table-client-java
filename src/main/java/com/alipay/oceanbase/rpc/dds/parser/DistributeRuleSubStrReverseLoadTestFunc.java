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

import com.alipay.common.tracer.util.LoadTestUtil;
import com.alipay.oceanbase.rpc.util.ObVString;
import com.alipay.oceanbase.rpc.util.Serialization;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhiqi.zzq
 * @since 2018/8/20 下午6:14
 */
public class DistributeRuleSubStrReverseLoadTestFunc implements DistributeRuleSimpleFunc {

    private List<String>        refColumnNames = new ArrayList<String>();
    private int                 pos            = 0;
    private int                 len            = Integer.MIN_VALUE;
    /**影子表分库标志位*/
    private static final String SHADOW_FLAG    = "abcdefghij";

    /**
     * Set parameters.
     */
    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        if (parameters.size() < 2) {
            throw new IllegalArgumentException(
                "DistributeRuleSubStrReverseLoad argument size must be bigger than 2 " + parameters);
        }

        Object parameter1 = parameters.get(0);

        if (!(parameter1 instanceof String)) {
            throw new IllegalArgumentException(
                "DistributeRuleSubStrReverseLoad first argument must be column or string "
                        + parameter1);
        }
        String ref = (String) parameters.get(0);
        refColumnNames.add(ref);

        Object parameter2 = parameters.get(1);

        if (!((parameter2 instanceof Long || (parameter2 instanceof Integer)))) {
            throw new IllegalArgumentException(
                "DistributeRuleSubStrReverseLoad second argument pos must be int " + parameter2);
        }

        Long pos = ((Number) parameters.get(1)).longValue();

        if (pos == 0 || pos > Integer.MAX_VALUE || pos < Integer.MIN_VALUE) {
            throw new IllegalArgumentException(
                "DistributeRuleSubStrReverseLoad second argument pos must be int exclude zero");
        }

        this.pos = pos.intValue();

        if (parameters.size() == 3) {

            Object parameter3 = parameters.get(2);

            if (!((parameter3 instanceof Long) || (parameter3 instanceof Integer))) {
                throw new IllegalArgumentException(
                    "DistributeRuleSubStrReverseLoad third argument len must be int " + parameter3);
            }

            Long len = ((Number) parameters.get(2)).longValue();

            if (len <= 0 || len > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                    "DistributeRuleSubStrReverseLoad third argument len must be positive int");
            }
            this.len = len.intValue();
        }
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
        return 3;
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

        if (refs.size() != refColumnNames.size()) {
            throw new IllegalArgumentException("DistributeRuleSubStrReverseLoad is refer to "
                                               + refColumnNames
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
            throw new IllegalArgumentException("Object [" + ref
                                               + "] can not evaluate by DistributeRuleSubStrFunc ");
        }

        String res = "";
        int evalStrLen = evalStr.length();
        if (pos > 0) {
            if (pos > evalStrLen) {
                throw new IllegalArgumentException("the length of param :" + evalStrLen
                                                   + " is less than the pos " + pos);
            } else {
                if (len > 0 && pos - 1 + len <= evalStrLen) {
                    res = evalStr.substring(pos - 1, pos - 1 + len);
                } else {
                    res = evalStr.substring(pos - 1);
                }
            }
        } else { // pos can not be 0
            if (-pos > evalStrLen) {
                throw new IllegalArgumentException("the length of param:" + evalStrLen
                                                   + " is less than the pos " + pos);
            } else {
                int pos = evalStrLen + this.pos;
                if (len > 0 && pos + len <= evalStrLen) {
                    res = evalStr.substring(pos, pos + len);
                } else {
                    res = evalStr.substring(pos);
                }
            }
        }

        res = new StringBuffer(res).reverse().toString();
        //        String loadTestUid;
        if (LoadTestUtil.isLoadTestMode()) {
            // 消费记录压测出现压测标不一致的场景，且未排查出具体哪个应用设置错误，
            // 评估后取消该检查，因为目前只有消费记录使用
            // 注释代码为最初的实现，保留在此
            try {
                res = convertToNumber(res);
            } catch (Exception e) {
                throw new IllegalArgumentException(
                    "DistributeRuleSubStrReverseLoadTestFunc uid is not standard, evalStr: "
                            + evalStr + ", res: " + res);
            }
            //            loadTestUid = LoadTestUtil.getLoadTestUid();
            //            if (res.equalsIgnoreCase(loadTestUid)) {
            //                try {
            //                    res = convertToNumber(loadTestUid);
            //                } catch (Exception e) {
            //                    throw new IllegalArgumentException(
            //                            "DistributeRuleSubStrReverseLoadTestFunc uid is not standard, evalStr: " + evalStr + ", res: " + res);
            //                }
            //            }
        }

        boolean onlyDigit = true;
        for (int i = 0; i < res.length(); i++) {
            if (!Character.isDigit(res.charAt(i))) {
                onlyDigit = false;
                break;
            }
        }

        if (!onlyDigit) {
            throw new IllegalArgumentException(
                "DistributeRuleSubStrReverseLoad uid result has non-digit character, evalStr: "
                        + evalStr + ", res: " + res);
        }

        return res;
    }

    /**
     * 将影子表字符转换成对应数字
     */
    public static String convertToNumber(String flag) throws Exception {
        flag = flag.toLowerCase();
        StringBuffer sb = new StringBuffer();
        char[] chars = flag.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            int index = SHADOW_FLAG.indexOf(chars[i]);
            if (index == -1) {
                sb.append(chars[i]);
            } else {
                if (i != (chars.length - 1)) {
                    throw new Exception("UID is not standard");
                }
                sb.append(index);
            }
        }

        return sb.toString();
    }
}

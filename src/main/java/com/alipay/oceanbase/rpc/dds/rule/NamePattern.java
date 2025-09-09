/*-
* #%L
 * * OceanBase Table Client Framework
 * *
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
 * *
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

package com.alipay.oceanbase.rpc.dds.rule;

import com.alipay.oceanbase.rpc.util.StringUtil;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
* @author zhiqi.zzq
* @since 2021/7/7 下午5:49
*/
public class NamePattern {

    private static final String HOLDER_FIRST        = "#F#";
    private static final String HOLDER_SECOND       = "#S#";

    private String              patternString;
    private String              patternHolder;
    private int                 valueAlignLenFirst  = 0;
    private int                 valueAlignLenSecond = 0;

    private int                 minValue;
    private int                 maxValue;

    private int                 startValueFirst;
    private int                 endValueFirst;
    private int                 startValueSecond;
    private int                 endValueSecond;
    private int                 valueRangeSecond;

    private boolean             hasTwoColumn        = false;

    public NamePattern(String patternString) {
        this.patternString = patternString;
        String regex = "\\{\\d+-\\d+\\}";
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(patternString);
        int i = 0;
        while (matcher.find()) {
            if (i == 0) {
                parseFirst(matcher.group(0));
                patternString = StringUtil.replaceOnce(patternString, matcher.group(0),
                    HOLDER_FIRST);
            } else {
                hasTwoColumn = true;
                parseSecond(matcher.group(0));
                patternString = StringUtil.replaceOnce(patternString, matcher.group(0),
                    HOLDER_SECOND);
                // two column 模式下，{from-to} from 大于 0 的情况下，用户返回的 Int 也不好计算如何落到哪个表上面
                // 同样使用 {from-to} 是为了统一和 single column 一样的用法
                if (this.startValueFirst > 0 || this.startValueSecond > 0) {
                    throw new IllegalArgumentException(
                        "two column NamePattern cannot start from non-zero value: "
                                + this.patternString);
                }
            }
            i++;
        }
        this.patternHolder = patternString;
    }

    private void parseFirst(String pattern) {
        int index0 = 0; // index of '{'
        int index1 = pattern.length() - 1; // index of '}'
        int index2 = pattern.indexOf('-', index0); // index of '-'
        this.valueAlignLenFirst = index2 - index0 - 1; // {00-99} 中数字的位数

        this.startValueFirst = Integer.parseInt(pattern.substring(index0 + 1, index2));
        this.endValueFirst = Integer.parseInt(pattern.substring(index2 + 1, index1));

        this.minValue = startValueFirst;
        this.maxValue = endValueFirst;
    }

    private void parseSecond(String pattern) {
        int index0 = 0; // index of '{'
        int index1 = pattern.length() - 1; // index of '}'
        int index2 = pattern.indexOf('-', index0); // index of '-'
        this.valueAlignLenSecond = index2 - index0 - 1; // {00-99} 中数字的位数

        this.startValueSecond = Integer.parseInt(pattern.substring(index0 + 1, index2));
        this.endValueSecond = Integer.parseInt(pattern.substring(index2 + 1, index1));

        this.valueRangeSecond = endValueSecond - startValueSecond + 1;
        this.minValue = startValueFirst * valueRangeSecond + startValueSecond;
        this.maxValue = endValueFirst * valueRangeSecond + endValueSecond;
    }

    public String wrapValue(String value) {
        int intValue = Integer.parseInt(value);

        int firstValue = intValue;
        int secondValue = -1;
        if (hasTwoColumn) {
            firstValue = intValue / valueRangeSecond;
            secondValue = intValue % valueRangeSecond;
        }

        // 技术上没必要控制得如此严格
        // 但是为了统一 DB 层库表分布的元数据，需要控制一下
        if (intValue < minValue || intValue > maxValue) {
            throw new IllegalArgumentException("NamePattern value out of bound. value: " + value
                                               + ", minValue: " + minValue + ", maxValue: "
                                               + maxValue + ", pattern: " + patternString);
        }

        String firstReplaced = StringUtil.replaceOnce(patternHolder, HOLDER_FIRST,
            StringUtil.alignRight(String.valueOf(firstValue), valueAlignLenFirst, "0"));
        if (!hasTwoColumn) {
            return firstReplaced;
        } else {
            return StringUtil.replaceOnce(firstReplaced, HOLDER_SECOND,
                StringUtil.alignRight(String.valueOf(secondValue), valueAlignLenSecond, "0"));
        }
    }

    public int getMinValue() {
        return minValue;
    }

    public int getMaxValue() {
        return maxValue;
    }

}

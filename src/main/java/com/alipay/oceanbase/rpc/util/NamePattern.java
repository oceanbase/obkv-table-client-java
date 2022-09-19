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

package com.alipay.oceanbase.rpc.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * NamePattern: patternPrefix_{from1-to1}_{from2-to2}_patternSuffix
 *
 *  user_{00-15}_suffix, user_{00-15}_{00-11}_suffix
 *
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
        this.maxValue = endValueFirst * valueRangeSecond + endValueSecond + 1;
    }

    /**
     * Wrap value.
     * @param value value
     * @return replace
     */
    public String wrapValue(int value) {
        int firstValue = value;
        int secondValue = -1;
        if (hasTwoColumn) {
            firstValue = value / valueRangeSecond;
            secondValue = value % valueRangeSecond;
        }

        // 统一 DB 层库表分布的元数据
        if (value < minValue || value > maxValue) {
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

    /**
     * Get min value.
     * @return min value
     */
    public int getMinValue() {
        return minValue;
    }

    /**
     * Get max value.
     * @return max value
     */
    public int getMaxValue() {
        return maxValue;
    }

    /**
     * Get size.
     * @return size
     */
    public int getSize() {
        return maxValue - minValue + 1;
    }

}

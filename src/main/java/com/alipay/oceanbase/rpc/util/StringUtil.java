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

public class StringUtil {

    public static boolean isEmpty(String str) {
        return str == null || str.isEmpty();
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static final boolean isNotBlank(String str) {
        return !isBlank(str);
    }

    public static final boolean isBlank(String str) {
        int strLen = 0;
        if (str == null || (strLen = str.length()) == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if ((Character.isWhitespace(str.charAt(i)) == false))
                return false;
        }
        return true;
    }

    /**
     * 扩展并右对齐字符串，用指定字符填充左边。
     * <pre>
     * StringUtil.alignRight(null, *, *)     = null
     * StringUtil.alignRight("", 3, 'z')     = "zzz"
     * StringUtil.alignRight("bat", 3, 'z')  = "bat"
     * StringUtil.alignRight("bat", 5, 'z')  = "zzbat"
     * StringUtil.alignRight("bat", 1, 'z')  = "bat"
     * StringUtil.alignRight("bat", -1, 'z') = "bat"
     * </pre>
     *
     * @param str 要对齐的字符串
     * @param size 扩展字符串到指定宽度
     * @param padChar 填充字符
     *
     * @return 扩展后的字符串，如果字符串为<code>null</code>，则返回<code>null</code>
     */
    public static String alignRight(String str, int size, char padChar) {
        if (str == null) {
            return null;
        }

        int pads = size - str.length();

        if (pads <= 0) {
            return str;
        }

        return alignRight(str, size, String.valueOf(padChar));
    }

    /**
     * 扩展并右对齐字符串，用指定字符串填充左边。
     * <pre>
     * StringUtil.alignRight(null, *, *)      = null
     * StringUtil.alignRight("", 3, "z")      = "zzz"
     * StringUtil.alignRight("bat", 3, "yz")  = "bat"
     * StringUtil.alignRight("bat", 5, "yz")  = "yzbat"
     * StringUtil.alignRight("bat", 8, "yz")  = "yzyzybat"
     * StringUtil.alignRight("bat", 1, "yz")  = "bat"
     * StringUtil.alignRight("bat", -1, "yz") = "bat"
     * StringUtil.alignRight("bat", 5, null)  = "  bat"
     * StringUtil.alignRight("bat", 5, "")    = "  bat"
     * </pre>
     *
     * @param str 要对齐的字符串
     * @param size 扩展字符串到指定宽度
     * @param padStr 填充字符串
     *
     * @return 扩展后的字符串，如果字符串为<code>null</code>，则返回<code>null</code>
     */
    public static String alignRight(String str, int size, String padStr) {
        if (str == null) {
            return null;
        }

        if ((padStr == null) || (padStr.length() == 0)) {
            padStr = " ";
        }

        int padLen = padStr.length();
        int strLen = str.length();
        int pads = size - strLen;

        if (pads <= 0) {
            return str;
        }

        if (pads == padLen) {
            return padStr.concat(str);
        } else if (pads < padLen) {
            return padStr.substring(0, pads).concat(str);
        } else {
            char[] padding = new char[pads];
            char[] padChars = padStr.toCharArray();

            for (int i = 0; i < pads; i++) {
                padding[i] = padChars[i % padLen];
            }

            return new String(padding).concat(str);
        }
    }

    /**
     * 替换指定的子串，只替换第一个出现的子串。
     *
     * 如果字符串为<code>null</code>则返回<code>null</code>，如果指定子串为<code>null</code>，则返回原字符串。
     * <pre>
     * StringUtil.replaceOnce(null, *, *)        = null
     * StringUtil.replaceOnce("", *, *)          = ""
     * StringUtil.replaceOnce("aba", null, null) = "aba"
     * StringUtil.replaceOnce("aba", null, null) = "aba"
     * StringUtil.replaceOnce("aba", "a", null)  = "aba"
     * StringUtil.replaceOnce("aba", "a", "")    = "ba"
     * StringUtil.replaceOnce("aba", "a", "z")   = "zba"
     * </pre>
     *
     * @param text 要扫描的字符串
     * @param repl 要搜索的子串
     * @param with 替换字符串
     *
     * @return 被替换后的字符串，如果原始字符串为<code>null</code>，则返回<code>null</code>
     */
    public static String replaceOnce(String text, String repl, String with) {
        return replace(text, repl, with, 1);
    }

    /**
     * 替换指定的子串，替换指定的次数。
     *
     * 如果字符串为<code>null</code>则返回<code>null</code>，如果指定子串为<code>null</code>，则返回原字符串。
     * <pre>
     * StringUtil.replace(null, *, *, *)         = null
     * StringUtil.replace("", *, *, *)           = ""
     * StringUtil.replace("abaa", null, null, 1) = "abaa"
     * StringUtil.replace("abaa", null, null, 1) = "abaa"
     * StringUtil.replace("abaa", "a", null, 1)  = "abaa"
     * StringUtil.replace("abaa", "a", "", 1)    = "baa"
     * StringUtil.replace("abaa", "a", "z", 0)   = "abaa"
     * StringUtil.replace("abaa", "a", "z", 1)   = "zbaa"
     * StringUtil.replace("abaa", "a", "z", 2)   = "zbza"
     * StringUtil.replace("abaa", "a", "z", -1)  = "zbzz"
     * </pre>
     *
     * @param text 要扫描的字符串
     * @param repl 要搜索的子串
     * @param with 替换字符串
     * @param max maximum number of values to replace, or <code>-1</code> if no maximum
     *
     * @return 被替换后的字符串，如果原始字符串为<code>null</code>，则返回<code>null</code>
     */
    public static String replace(String text, String repl, String with, int max) {
        if ((text == null) || (repl == null) || (with == null) || (repl.length() == 0)
            || (max == 0)) {
            return text;
        }

        StringBuffer buf = new StringBuffer(text.length());
        int start = 0;
        int end = 0;

        while ((end = text.indexOf(repl, start)) != -1) {
            buf.append(text.substring(start, end)).append(with);
            start = end + repl.length();

            if (--max == 0) {
                break;
            }
        }

        buf.append(text.substring(start));
        return buf.toString();
    }

    /**
     * Parse name pattern.
     * @param pattern pattern
     * @return string array
     */
    public static String[] parseNamePattern(String pattern) {
        String dbIndexes[];
        //去掉*{}
        int headerIdx = pattern.indexOf("{");
        int footerIdx = pattern.indexOf("}");
        String header = pattern.substring(0, headerIdx);
        String footer = pattern.substring(footerIdx + 1);
        pattern = pattern.substring(headerIdx + 1, footerIdx);

        String[] temp = pattern.split("-");
        if (temp.length != 2) {
            throw new IllegalArgumentException();
        }
        temp[0] = temp[0].trim();
        temp[1] = temp[1].trim();
        int firstNumFrom = firstNum(temp[0]);
        int firstNot0From = firstNot0(temp[0], firstNumFrom);
        int firstNumTo = firstNum(temp[1]);
        int firstNot0To = firstNot0(temp[1], firstNumTo);
        if (firstNumFrom == -1 || firstNumTo == -1) {
            throw new IllegalArgumentException();
        }
        if (firstNumFrom != firstNumTo) {
            throw new IllegalArgumentException("padding width different");
        }
        if (temp[0].length() != temp[1].length() && !(
        //_0-_16
            (firstNot0From == -1 && firstNumFrom == temp[0].length() - 1 && firstNot0To == firstNumTo)
            //_1-_16
            || (firstNot0From == firstNumFrom && firstNot0To == firstNumTo))) {
            throw new IllegalArgumentException("dbindex width different");
        }
        if (firstNumFrom != 0) {
            String fromPadding = temp[0].substring(0, firstNumFrom);
            String toPadding = temp[1].substring(0, firstNumTo);
            if (!fromPadding.equals(toPadding)) {
                throw new IllegalArgumentException("padding different");
            }

        }
        int suffixFrom = firstNot0From == -1 ? 0 //_0-_16
            : Integer.parseInt(temp[0].substring(firstNot0From));

        int suffixTo = Integer.parseInt(temp[1].substring(firstNot0To));

        if (suffixTo <= suffixFrom) {
            throw new IllegalArgumentException("length is error");
        }

        int suffixWidth = temp[0].length() != temp[1].length() ? 0 : temp[0].length()
                                                                     - firstNumFrom;

        dbIndexes = new String[suffixTo - suffixFrom + 1];
        for (int i = suffixFrom; i <= suffixTo; i++) {
            String dbIndex;
            dbIndex = header + StringUtil.alignRight(String.valueOf(i), suffixWidth, '0') + footer;
            dbIndexes[i] = dbIndex;
        }

        return dbIndexes;
    }

    private static int firstNum(String str) {
        char c;
        for (int i = 0; i < str.length(); i++) {
            c = str.charAt(i);
            if (c >= '0' && c <= '9') {
                return i;
            }
        }
        return -1;
    }

    private static int firstNot0(String str, int start) {
        char c;
        for (int i = start; i < str.length(); i++) {
            c = str.charAt(i);
            if (c != '0') {
                return i;
            }
        }
        return -1;
    }

    /**
     *
    * @param str
    * @param searchChar
    * @return
    */
    public static int indexOf(String str, char searchChar) {
        return str != null && str.length() != 0 ? str.indexOf(searchChar) : -1;
    }

    /**
     *
    * @param str
    * @param searchChar
    * @param startPos
    * @return
    */
    public static int indexOf(String str, char searchChar, int startPos) {
        return str != null && str.length() != 0 ? str.indexOf(searchChar, startPos) : -1;
    }

    /**
     *
    * @param str
    * @param searchStr
    * @return
    */
    public static int indexOf(String str, String searchStr) {
        return str != null && searchStr != null ? str.indexOf(searchStr) : -1;
    }

    /**
     *
    * @param str
    * @param searchStr
    * @param startPos
    * @return
    */
    public static int indexOf(String str, String searchStr, int startPos) {
        if (str != null && searchStr != null) {
            return searchStr.length() == 0 && startPos >= str.length() ? str.length() : str
                .indexOf(searchStr, startPos);
        } else {
            return -1;
        }
    }

    /**
     *
    * @param str1
    * @param str2
    * @return
    */
    public static boolean equals(String str1, String str2) {
        if (str1 == null) {
            return str2 == null;
        } else {
            return str1.equals(str2);
        }
    }

}

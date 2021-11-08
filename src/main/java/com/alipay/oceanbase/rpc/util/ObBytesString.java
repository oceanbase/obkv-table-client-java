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

/**
 * binary bytes string without charset
 *
 */
public class ObBytesString implements Comparable<ObBytesString> {

    public byte[] bytes;

    /**
     * Ob bytes string.
     */
    public ObBytesString() {
        this.bytes = new byte[0];
    }

    /**
     * Ob bytes string.
     */
    public ObBytesString(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("ObBytesString bytes can not be null ");
        }
        this.bytes = bytes;
    }

    /**
     * Ob bytes string.
     */
    public ObBytesString(String str) {
        if (str == null) {
            throw new IllegalArgumentException("ObBytesString str can not be null ");
        }
        this.bytes = Serialization.strToBytes(str);
    }

    /**
     * Length.
     */
    public int length() {
        return bytes.length;
    }

    /**
     * Equals.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ObBytesString that = (ObBytesString) o;
        return compare(bytes, that.bytes) == 0;
    }

    /**
     * Compare to.
     */
    @Override
    public int compareTo(ObBytesString another) {
        return compare(bytes, another.bytes);
    }

    private int compare(byte[] s, byte[] t) {
        int len1 = s.length;
        int len2 = t.length;
        int lim = Math.min(len1, len2);
        int k = 0;
        while (k < lim) {
            byte c1 = s[k];
            byte c2 = t[k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }
}

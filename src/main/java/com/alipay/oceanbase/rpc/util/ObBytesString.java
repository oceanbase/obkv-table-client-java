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

import java.util.ArrayList;

/**
 * binary bytes string without charset
 */
public class ObBytesString implements Comparable<ObBytesString> {

    public byte[] bytes;

    public ObBytesString() {
        this.bytes = new byte[0];
    }

    public ObBytesString(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("ObBytesString bytes can not be null ");
        }
        this.bytes = bytes;
    }

    public ObBytesString(String str) {
        if (str == null) {
            throw new IllegalArgumentException("ObBytesString str can not be null ");
        }
        this.bytes = Serialization.strToBytes(str);
    }

    /**
     * Get length
     * @return length
     */
    public int length() {
        return bytes.length;
    }

    /**
     * Equals.
     * @param o object
     * @return equal or not
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
     * Compare
     * @param another byte string
     * @return integer greater than, equal to, or less than 0
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

    public ObBytesString[] split(byte delim) {
        ArrayList<ObBytesString> list = new ArrayList<>();
        int start = 0;
        for (int i = 0; i < bytes.length; ++i) {
            if (bytes[i] == delim) {
                byte[] data = new byte[i - start];
                System.arraycopy(bytes, start, data, 0, data.length);
                ObBytesString str = new ObBytesString(data);
                list.add(str);
                start = i + 1;
            }
        }
        if (start < bytes.length) {
            byte[] data = new byte[bytes.length - start];
            System.arraycopy(bytes, start, data, 0, data.length);
            ObBytesString str = new ObBytesString(data);
            list.add(str);
        }
        return list.toArray(new ObBytesString[0]);
    }

    @Override
    public String toString() {
        return "ObBytesString{" +
                "bytes.length=" + (bytes != null ? bytes.length : 0) +
                ", bytes=" + (bytes != null ? java.util.Arrays.toString(bytes) : "null") +
                '}';
    }

}

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
    public int    offset;
    private int   stringLength;

    public ObBytesString() {
        this.bytes = new byte[0];
        this.offset = 0;
        this.stringLength = 0;
    }

    public ObBytesString(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("ObBytesString bytes can not be null ");
        }
        this.bytes = bytes;
        this.offset = 0;
        this.stringLength = bytes.length;
    }

    /**
     * View into {@code bytes[offset, offset+length)}. Encode copies only this region.
     */
    public ObBytesString(byte[] bytes, int offset, int length) {
        if (bytes == null) {
            throw new IllegalArgumentException("ObBytesString bytes can not be null ");
        }
        if (offset < 0 || length < 0 || offset > bytes.length || length > bytes.length - offset) {
            throw new IllegalArgumentException("ObBytesString invalid range offset=" + offset
                                               + " length=" + length + " bytes.length="
                                               + bytes.length);
        }
        this.bytes = bytes;
        this.offset = offset;
        this.stringLength = length;
    }

    public ObBytesString(String str) {
        if (str == null) {
            throw new IllegalArgumentException("ObBytesString str can not be null ");
        }
        this.bytes = Serialization.strToBytes(str);
        this.offset = 0;
        this.stringLength = this.bytes.length;
    }

    /**
     * Get length
     * @return length
     */
    public int length() {
        return stringLength;
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
        return compare(this, that) == 0;
    }

    /**
     * Compare
     * @param another byte string
     * @return integer greater than, equal to, or less than 0
     */
    @Override
    public int compareTo(ObBytesString another) {
        return compare(this, another);
    }

    private static int compare(ObBytesString s, ObBytesString t) {
        int len1 = s.stringLength;
        int len2 = t.stringLength;
        int lim = Math.min(len1, len2);
        int k = 0;
        while (k < lim) {
            byte c1 = s.bytes[s.offset + k];
            byte c2 = t.bytes[t.offset + k];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return len1 - len2;
    }

    public ObBytesString[] split(byte delim) {
        ArrayList<ObBytesString> list = new ArrayList<>();
        int start = offset;
        int end = offset + stringLength;
        for (int i = offset; i < end; ++i) {
            if (bytes[i] == delim) {
                byte[] data = new byte[i - start];
                System.arraycopy(bytes, start, data, 0, data.length);
                list.add(new ObBytesString(data));
                start = i + 1;
            }
        }
        if (start < end) {
            byte[] data = new byte[end - start];
            System.arraycopy(bytes, start, data, 0, data.length);
            list.add(new ObBytesString(data));
        }
        return list.toArray(new ObBytesString[0]);
    }
}

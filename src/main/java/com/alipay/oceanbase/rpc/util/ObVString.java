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

import java.io.UnsupportedEncodingException;

public class ObVString {
    private String stringVal;
    private byte[] bytesVal;
    private byte[] encodeBytes;

    public ObVString(String stringVal) {
        this.stringVal = stringVal;
        if (stringVal == null) {
            bytesVal = new byte[0];
        } else {
            try {
                bytesVal = stringVal.getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new IllegalArgumentException("can not encode str to u", e);
            }
        }
        this.encodeBytes = Serialization.encodeVString(stringVal);
    }

    /**
     * Get encode need bytes.
     * @return return length
     */
    public int getEncodeNeedBytes() {
        return encodeBytes.length;
    }

    public byte[] getBytesVal() {
        return bytesVal;
    }

    public byte[] getEncodeBytes() {
        return encodeBytes;
    }

    public String getStringVal() {
        return stringVal;
    }
}

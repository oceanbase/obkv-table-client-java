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

package com.alipay.oceanbase.rpc.util.hash;

public class ObHashSortBin {
    /**
     * Ob hash sort bin.
     */
    public static long obHashSortBin(byte[] s, int len, long n1, long n2) {
        for (int i = 0; i < len; ++i) {
            n1 ^= (((n1 & 63) + n2) * (toInt(s[i])) + (n1 << 8));
            n2 += 3;
        }
        return n1;
    }

    private static int toInt(byte b) {
        return 0xFF & b;
    }
}

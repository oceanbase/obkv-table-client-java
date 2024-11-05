/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

public enum ObAddrVersion {
    ObAddrIPV4((byte) 4), ObAddrIPV6((byte) 6);

    private final byte value;

    ObAddrVersion(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static ObAddrVersion fromValue(int value) {
        for (ObAddrVersion obAddrVersion : values()) {
            if (obAddrVersion.value == value) {
                return obAddrVersion;
            }
        }
        return ObAddrIPV4; //默认使用IPV4, 或者抛异常。
    }
}
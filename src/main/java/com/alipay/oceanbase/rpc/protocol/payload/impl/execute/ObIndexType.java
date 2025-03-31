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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import java.util.HashMap;
import java.util.Map;

public enum ObIndexType {
    IndexTypeIsNot(0), IndexTypeNormalLocal(1), IndexTypeUniqueLocal(2), IndexTypeNormalGlobal(3), IndexTypeUniqueGlobal(
                                                                                                                         4), IndexTypePrimary(
                                                                                                                                              5), IndexTypeDomainCtxcat(
                                                                                                                                                                        6), IndexTypeNormalGlobalLocalStorage(
                                                                                                                                                                                                              7), IndexTypeUniqueGlobalLocalStorage(
                                                                                                                                                                                                                                                    8), IndexTypeSpatialLocal(
                                                                                                                                                                                                                                                                              10), IndexTypeSpatialGlobal(
                                                                                                                                                                                                                                                                                                          11), IndexTypeSpatialGlobalLocalStorage(
                                                                                                                                                                                                                                                                                                                                                  12), IndexTypeRowkeyDocIdLocal(
                                                                                                                                                                                                                                                                                                                                                                                 13), IndexTypeDocIdRowkeyLocal(
                                                                                                                                                                                                                                                                                                                                                                                                                14), IndexTypeFtsIndexLocal(
                                                                                                                                                                                                                                                                                                                                                                                                                                            15), IndexTypeFtsDocWordLocal(
                                                                                                                                                                                                                                                                                                                                                                                                                                                                          16),
    /*
    IndexTypeDocIdRowkeyGlobal(17),
    IndexTypeFtsIndexGlobal(18),
    IndexTypeFtsDocWordGlobal(19),
    IndexTypeDocIdRowkeyGlobalLocalStorage(20),
    IndexTypeFtsIndexGlobalLocalStorage(21),
    IndexTypeFtsDocWordGlobalLocalStorage(22),
    IndexTypeNormalMultivalueLocal(23),
    IndexTypeUniqueMultivalueLocal(24),
    IndexTypeVecRowkeyVidLocal(25),
    IndexTypeVecVidRowkeyLocal(26),
    IndexTypeVecDeltaBufferLocal(27),
    IndexTypeVecIndexIdLocal(28),
    IndexTypeVecIndexSnapshotDataLocal(29),
    */
    IndexTypeMax(30);

    private int                              value;
    private static Map<Integer, ObIndexType> map = new HashMap<Integer, ObIndexType>();

    ObIndexType(int value) {
        this.value = value;
    }

    static {
        for (ObIndexType type : ObIndexType.values()) {
            map.put(type.value, type);
        }
    }

    /*
     * Value of.
     */
    public static ObIndexType valueOf(int value) {
        return map.get(value);
    }

    /*
     * Get value.
     */
    public int getValue() {
        return value;
    }

    /*
     * Get byte value.
     */
    public byte getByteValue() {
        return (byte) value;
    }

    public boolean isGlobalIndex() {
        return valueOf(value) == ObIndexType.IndexTypeNormalGlobal
               || valueOf(value) == ObIndexType.IndexTypeUniqueGlobal
               || valueOf(value) == ObIndexType.IndexTypeSpatialGlobal;
    }
}

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

package com.alipay.oceanbase.rpc.protocol.payload.impl.column;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;

import java.util.Arrays;
import java.util.Collections;

public class ObSimpleColumn extends ObColumn {

    public static ObColumn DEFAULT_UTF8MB4_GENERAL_CI = new ObSimpleColumn(
                                                          "default",
                                                          -1,
                                                          ObObjType.ObVarcharType,
                                                          ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI);

    public static ObColumn DEFAULT_UTF8MB4            = new ObSimpleColumn("default", -1,
                                                          ObObjType.ObVarcharType,
                                                          ObCollationType.CS_TYPE_UTF8MB4_BIN);

    public static ObColumn DEFAULT_BINARY             = new ObSimpleColumn("default", -1,
                                                          ObObjType.ObVarcharType,
                                                          ObCollationType.CS_TYPE_BINARY);

    /*
     * Ob simple column.
     */
    public ObSimpleColumn(String columnName, int index, ObObjType obObjType,
                          ObCollationType obCollationType) {
        super(columnName, index, obObjType, obCollationType, Collections.singletonList(columnName),
            null);
    }

    /*
     * Eval value.
     */
    @Override
    public Object evalValue(Object... refs) throws IllegalArgumentException {
        if (refs.length == 0 || refs.length > 1) {
            throw new IllegalArgumentException(
                "ObSimpleColumn is refer to itself so that the length of the refs must be 1. refs:"
                        + Arrays.toString(refs));
        }
        return obObjType.parseToComparable(refs[0], obCollationType);
    }
}

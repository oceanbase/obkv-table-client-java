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
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnExpressParser;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ObGeneratedColumn extends ObColumn {

    /*
     * Ob generated column.
     */
    public ObGeneratedColumn(String columnName, int index, ObObjType obObjType,
                             ObCollationType obCollationType,
                             ObGeneratedColumnSimpleFunc columnExpress) {
        super(columnName, index, obObjType, obCollationType, columnExpress.getRefColumnNames(),
            columnExpress);
    }

    /*
     * Eval value.
     */
    public Object evalValue(Object... refs) throws IllegalArgumentException {

        if (refColumnNames.size() != refs.length) {
            throw new IllegalArgumentException("ObGeneratedColumn if refer to " + refColumnNames
                                               + " so that the length of refs must be "
                                               + refColumnNames.size() + ". refs"
                                               + Arrays.toString(refs));
        }

        return obObjType.parseToComparable(columnExpress.evalValue(obCollationType, refs),
            obCollationType);
    }
}

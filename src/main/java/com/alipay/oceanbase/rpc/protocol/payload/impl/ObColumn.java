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

package com.alipay.oceanbase.rpc.protocol.payload.impl;

import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnSimpleFunc;

import java.util.List;

public abstract class ObColumn {

    protected final String                      columnName;
    protected final int                         index;
    protected final ObObjType                   obObjType;
    protected final ObCollationType             obCollationType;
    protected final List<String>                refColumnNames;
    protected final ObGeneratedColumnSimpleFunc columnExpress;

    /**
     * Ob column.
     */
    public ObColumn(String columnName, int index, ObObjType obObjType,
                    ObCollationType obCollationType, List<String> refColumnNames,
                    ObGeneratedColumnSimpleFunc columnExpress) {
        this.columnName = columnName;
        this.index = index;
        this.obObjType = obObjType;
        this.obCollationType = obCollationType;
        this.refColumnNames = refColumnNames;
        this.columnExpress = columnExpress;
    }

    /**
     * Get column name.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Get index.
     */
    public int getIndex() {
        return index;
    }

    /**
     * Get ob obj type.
     */
    public ObObjType getObObjType() {
        return obObjType;
    }

    /**
     * Get ob collation type.
     */
    public ObCollationType getObCollationType() {
        return obCollationType;
    }

    /**
     * Get ref column names.
     */
    public List<String> getRefColumnNames() {
        return refColumnNames;
    }

    /**
     * Get ob generated column simple func.
     */
    public ObGeneratedColumnSimpleFunc getObGeneratedColumnSimpleFunc() {
        return columnExpress;
    }

    public abstract Object evalValue(Object... refs) throws IllegalArgumentException;

    /**
     * To string.
     */
    @Override
    public String toString() {
        return "ObColumn{" + "columnName='" + columnName + '\'' + ", index=" + index
               + ", obObjType=" + obObjType + ", obCollationType=" + obCollationType
               + ", refColumnNames=" + refColumnNames + ", columnExpress=" + columnExpress + '}';
    }
}

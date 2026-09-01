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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * Compact representation of one HBase K/Q/T/V response page. Binary values own their byte arrays
 * and remain valid after the response buffer is released.
 */
public final class ObHBaseCellBatch {

    private static final int HBASE_KQTV_COLUMN_COUNT = 4;

    private final byte[][]   rowKeys;
    private final byte[][]   qualifiers;
    private final long[]     timestamps;
    private final byte[][]   values;
    private final ObObjMeta[] metas = new ObObjMeta[HBASE_KQTV_COLUMN_COUNT];

    public ObHBaseCellBatch(int size) {
        if (size < 0) {
            throw new IllegalArgumentException("negative HBase cell batch size: " + size);
        }
        this.rowKeys = new byte[size][];
        this.qualifiers = new byte[size][];
        this.timestamps = new long[size];
        this.values = new byte[size][];
    }

    public void setMeta(int columnIndex, ObObjMeta meta) {
        metas[columnIndex] = meta;
    }

    ObObjMeta getMeta(int columnIndex) {
        return metas[columnIndex];
    }

    public void setCell(int index, byte[] rowKey, byte[] qualifier, long timestamp, byte[] value) {
        rowKeys[index] = rowKey;
        qualifiers[index] = qualifier;
        timestamps[index] = timestamp;
        values[index] = value;
    }

    public int size() {
        return timestamps.length;
    }

    public byte[] getRowKey(int index) {
        return rowKeys[index];
    }

    public byte[] getQualifier(int index) {
        return qualifiers[index];
    }

    public long getTimestamp(int index) {
        return timestamps[index];
    }

    public byte[] getValue(int index) {
        return values[index];
    }

    List<ObObj> materializeRow(int index) {
        List<ObObj> row = new ArrayList<ObObj>(HBASE_KQTV_COLUMN_COUNT);
        row.add(new ObObj(metas[0], rowKeys[index]));
        row.add(new ObObj(metas[1], qualifiers[index]));
        row.add(new ObObj(metas[2], timestamps[index]));
        row.add(new ObObj(metas[3], values[index]));
        return row;
    }

    List<List<ObObj>> materializeRows(int fromIndex) {
        if (fromIndex < 0 || fromIndex > size()) {
            throw new IndexOutOfBoundsException("invalid materialize start index: " + fromIndex);
        }
        List<List<ObObj>> rows = new ArrayList<List<ObObj>>(size() - fromIndex);
        for (int i = fromIndex; i < size(); i++) {
            rows.add(materializeRow(i));
        }
        return rows;
    }
}

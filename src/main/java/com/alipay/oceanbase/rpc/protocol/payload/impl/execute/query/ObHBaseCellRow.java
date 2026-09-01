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

import java.util.ArrayList;
import java.util.List;

/**
 * Read-only view of the compact cells that belong to one HBase row. A row may span multiple
 * decoded batches, so the view keeps batch ranges instead of copying fields into per-cell DTOs.
 */
public final class ObHBaseCellRow {

    private final byte[]                      rowKey;
    private final List<ObHBaseCellBatchSlice> slices = new ArrayList<ObHBaseCellBatchSlice>(1);
    private int                               cellCount;

    ObHBaseCellRow(byte[] rowKey) {
        if (rowKey == null) {
            throw new NullPointerException("rowKey is null");
        }
        this.rowKey = rowKey;
    }

    void addSlice(ObHBaseCellBatch batch, int fromIndex, int toIndex) {
        if (batch == null) {
            throw new NullPointerException("batch is null");
        }
        if (fromIndex < 0 || toIndex <= fromIndex || toIndex > batch.size()) {
            throw new IndexOutOfBoundsException("invalid HBase cell batch slice [" + fromIndex
                                                + ", " + toIndex + ") for batch size "
                                                + batch.size());
        }
        slices.add(new ObHBaseCellBatchSlice(batch, fromIndex, toIndex));
        cellCount += toIndex - fromIndex;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public int getCellCount() {
        return cellCount;
    }

    public int getSliceCount() {
        return slices.size();
    }

    public ObHBaseCellBatch getBatch(int sliceIndex) {
        return slices.get(sliceIndex).batch;
    }

    public int getFromIndex(int sliceIndex) {
        return slices.get(sliceIndex).fromIndex;
    }

    public int getToIndex(int sliceIndex) {
        return slices.get(sliceIndex).toIndex;
    }

    private static final class ObHBaseCellBatchSlice {
        private final ObHBaseCellBatch batch;
        private final int              fromIndex;
        private final int              toIndex;

        private ObHBaseCellBatchSlice(ObHBaseCellBatch batch, int fromIndex, int toIndex) {
            this.batch = batch;
            this.fromIndex = fromIndex;
            this.toIndex = toIndex;
        }
    }
}

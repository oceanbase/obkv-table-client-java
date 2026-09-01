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

import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationLevel;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CompactHBaseStreamResultTest {

    @Test
    public void testCompactCellsAdvanceWithoutMaterializingRows() throws Exception {
        TestStreamResult streamResult = new TestStreamResult();
        ObHBaseCellBatch batch = batch();
        streamResult.addBatch(batch);

        assertEquals(2, streamResult.getCachedRowCount());
        assertTrue(streamResult.next());
        assertTrue(streamResult.isCurrentHBaseCell());
        assertEquals(0, streamResult.getCurrentHBaseCellIndex());
        assertArrayEquals(bytes("row-1"), streamResult.getCurrentHBaseCellBatch().getRowKey(0));
        assertEquals(1, streamResult.getCachedRowCount());

        assertTrue(streamResult.next());
        assertTrue(streamResult.isCurrentHBaseCell());
        assertEquals(1, streamResult.getCurrentHBaseCellIndex());
        assertEquals(102L, streamResult.getCurrentHBaseCellBatch().getTimestamp(1));
        assertEquals(0, streamResult.getCachedRowCount());
        assertFalse(streamResult.next());
    }

    @Test
    public void testLegacyAccessMaterializesOnlyWhenRequested() throws Exception {
        TestStreamResult streamResult = new TestStreamResult();
        streamResult.addBatch(batch());

        assertTrue(streamResult.next());
        List<ObObj> row = streamResult.getRow();
        assertArrayEquals(bytes("row-1"), (byte[]) row.get(0).getValue());
        assertEquals(101L, row.get(2).getValue());

        assertEquals(1, streamResult.getCacheRows().size());
        assertFalse(streamResult.isCurrentHBaseCell());
        assertTrue(streamResult.next());
        assertArrayEquals(bytes("row-2"), (byte[]) streamResult.getRow().get(0).getValue());
    }

    @Test
    public void testDrainCurrentHBaseRowLeavesNextRowUnread() throws Exception {
        TestStreamResult streamResult = new TestStreamResult();
        ObHBaseCellBatch batch = batch(new String[] { "row-1", "row-1", "row-2" }, new String[] {
                "q-1", "q-2", "q-3" });
        streamResult.addBatch(batch);

        assertTrue(streamResult.next());
        ObHBaseCellRow row = streamResult.drainCurrentHBaseRow();

        assertArrayEquals(bytes("row-1"), row.getRowKey());
        assertEquals(2, row.getCellCount());
        assertEquals(1, row.getSliceCount());
        assertEquals(batch, row.getBatch(0));
        assertEquals(0, row.getFromIndex(0));
        assertEquals(2, row.getToIndex(0));
        assertEquals(1, streamResult.getRowIndex());
        assertEquals(1, streamResult.getCachedRowCount());

        assertTrue(streamResult.next());
        assertEquals(2, streamResult.getCurrentHBaseCellIndex());
        assertArrayEquals(bytes("row-2"), streamResult.getCurrentHBaseCellBatch().getRowKey(2));
    }

    @Test
    public void testDrainCurrentHBaseRowAcrossCachedBatches() throws Exception {
        TestStreamResult streamResult = new TestStreamResult();
        ObHBaseCellBatch first = batch(new String[] { "row-1", "row-1" }, new String[] { "q-1",
                "q-2" });
        ObHBaseCellBatch second = batch(new String[] { "row-1", "row-2" }, new String[] { "q-3",
                "q-4" });
        streamResult.addBatch(first);
        streamResult.addBatch(second);

        assertTrue(streamResult.next());
        ObHBaseCellRow row = streamResult.drainCurrentHBaseRow();

        assertEquals(3, row.getCellCount());
        assertEquals(2, row.getSliceCount());
        assertEquals(first, row.getBatch(0));
        assertEquals(0, row.getFromIndex(0));
        assertEquals(2, row.getToIndex(0));
        assertEquals(second, row.getBatch(1));
        assertEquals(0, row.getFromIndex(1));
        assertEquals(1, row.getToIndex(1));
        assertEquals(2, streamResult.getRowIndex());
        assertEquals(1, streamResult.getCachedRowCount());

        assertTrue(streamResult.next());
        assertEquals(second, streamResult.getCurrentHBaseCellBatch());
        assertEquals(1, streamResult.getCurrentHBaseCellIndex());
        assertArrayEquals(bytes("row-2"), streamResult.getCurrentHBaseCellBatch().getRowKey(1));
    }

    @Test
    public void testDrainCurrentHBaseRowDoesNotConsumeDifferentRowInNextBatch() throws Exception {
        TestStreamResult streamResult = new TestStreamResult();
        ObHBaseCellBatch first = batch(new String[] { "row-1" }, new String[] { "q-1" });
        ObHBaseCellBatch second = batch(new String[] { "row-2" }, new String[] { "q-2" });
        streamResult.addBatch(first);
        streamResult.addBatch(second);

        assertTrue(streamResult.next());
        ObHBaseCellRow row = streamResult.drainCurrentHBaseRow();

        assertEquals(1, row.getCellCount());
        assertEquals(1, row.getSliceCount());
        assertEquals(1, streamResult.getCachedRowCount());
        assertTrue(streamResult.next());
        assertEquals(second, streamResult.getCurrentHBaseCellBatch());
        assertEquals(0, streamResult.getCurrentHBaseCellIndex());
    }

    private static ObHBaseCellBatch batch() {
        return batch(new String[] { "row-1", "row-2" }, new String[] { "q-1", "q-2" });
    }

    private static ObHBaseCellBatch batch(String[] rowKeys, String[] qualifiers) {
        assertEquals(rowKeys.length, qualifiers.length);
        ObHBaseCellBatch batch = new ObHBaseCellBatch(rowKeys.length);
        ObObjMeta binaryMeta = new ObObjMeta(ObObjType.ObVarcharType,
            ObCollationLevel.CS_LEVEL_EXPLICIT, ObCollationType.CS_TYPE_BINARY, (byte) 10);
        ObObjMeta timestampMeta = new ObObjMeta(ObObjType.ObInt64Type,
            ObCollationLevel.CS_LEVEL_NUMERIC, ObCollationType.CS_TYPE_BINARY, (byte) 10);
        batch.setMeta(0, binaryMeta);
        batch.setMeta(1, binaryMeta);
        batch.setMeta(2, timestampMeta);
        batch.setMeta(3, binaryMeta);
        for (int i = 0; i < rowKeys.length; i++) {
            batch
                .setCell(i, bytes(rowKeys[i]), bytes(qualifiers[i]), 101L + i, bytes("value-" + i));
        }
        return batch;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static class TestStreamResult extends AbstractQueryStreamResult {

        TestStreamResult() {
            initialized = true;
            expectant = new LinkedHashMap<Long, ObPair<Long, ObTableParam>>();
        }

        void addBatch(ObHBaseCellBatch batch) {
            cacheHBaseCellBatches.addLast(batch);
        }

        @Override
        protected ObPayload referToNewPartition(ObPair<Long, ObTableParam> partIdWithObTable) {
            return null;
        }

        @Override
        protected ObTableQueryResult execute(ObPair<Long, ObTableParam> partIdWithObTable,
                                             ObPayload streamRequest) {
            return null;
        }

        @Override
        protected ObTableQueryAsyncResult executeAsync(ObPair<Long, ObTableParam> partIdWithObTable,
                                                       ObPayload streamRequest) {
            return null;
        }

        @Override
        protected Map<Long, ObPair<Long, ObTableParam>> refreshPartition(ObTableQuery tableQuery,
                                                                         String tableName) {
            return Collections.emptyMap();
        }
    }
}

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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationLevel;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ObTableQueryResultHBaseDecodeTest {

    @Test
    public void testFastDecodeMatchesGenericDecodeAndCachesMeta() {
        List<List<ObObj>> fastRows = new ArrayList<List<ObObj>>();
        fastRows.add(newHBaseRow(bytes("row-1"), bytes("q-1"), 101L, bytes("value-1"), (byte) 10));
        fastRows.add(newHBaseRow(bytes("row-2"), bytes("q-2"), -102L, bytes("value-2"), (byte) 10));

        ObTableQueryResult fastResult = decode(
            newQueryResult(new String[] { "K", "Q", "T", "V" }, fastRows), true);
        ObTableQueryResult genericResult = decode(
            newQueryResult(new String[] { "K", "Q", "T", "VALUE" }, copyRows(fastRows)), false);

        assertTrue(fastResult.hasHBaseCellBatch());
        ObHBaseCellBatch batch = fastResult.getHBaseCellBatch();
        assertEquals(2, batch.size());
        assertArrayEquals(bytes("row-1"), batch.getRowKey(0));
        assertArrayEquals(bytes("q-2"), batch.getQualifier(1));
        assertEquals(-102L, batch.getTimestamp(1));
        assertArrayEquals(bytes("value-2"), batch.getValue(1));

        assertRowsEqual(genericResult.getPropertiesRows(), fastResult.getPropertiesRows());
        assertFalse(fastResult.hasHBaseCellBatch());
        assertTrue(fastResult.getPropertiesRows() instanceof ArrayList);
        for (int columnIndex = 0; columnIndex < 4; columnIndex++) {
            assertSame(fastResult.getPropertiesRows().get(0).get(columnIndex).getMeta(), fastResult
                .getPropertiesRows().get(1).get(columnIndex).getMeta());
            assertNotSame(genericResult.getPropertiesRows().get(0).get(columnIndex).getMeta(),
                genericResult.getPropertiesRows().get(1).get(columnIndex).getMeta());
        }
    }

    @Test
    public void testUnexpectedFirstRowMetaFallsBackToGenericDecode() {
        List<List<ObObj>> rows = new ArrayList<List<ObObj>>();
        rows.add(newTextKeyRow("row-1", bytes("q-1"), 101L, bytes("value-1")));
        rows.add(newTextKeyRow("row-2", bytes("q-2"), 102L, bytes("value-2")));

        ObTableQueryResult result = decode(
            newQueryResult(new String[] { "K", "Q", "T", "V" }, rows), false);

        assertEquals("row-1", result.getPropertiesRows().get(0).get(0).getValue());
        assertEquals("row-2", result.getPropertiesRows().get(1).get(0).getValue());
        assertNotSame(result.getPropertiesRows().get(0).get(0).getMeta(), result
            .getPropertiesRows().get(1).get(0).getMeta());
    }

    @Test
    public void testLaterRowMetaMismatchFailsClosed() {
        List<List<ObObj>> rows = new ArrayList<List<ObObj>>();
        rows.add(newHBaseRow(bytes("row-1"), bytes("q-1"), 101L, bytes("value-1"), (byte) 10));
        rows.add(newHBaseRow(bytes("row-2"), bytes("q-2"), 102L, bytes("value-2"), (byte) 11));
        ObTableQueryResult encodedResult = newQueryResult(new String[] { "K", "Q", "T", "V" },
            rows);
        ByteBuf buf = Unpooled.wrappedBuffer(encodedResult.encode());
        try {
            IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> new ObTableQueryResult().decode(buf));
            assertTrue(exception.getMessage().contains("meta changed"));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testResponseMetaCacheDoesNotCrossResponses() {
        List<List<ObObj>> firstRows = new ArrayList<List<ObObj>>();
        firstRows.add(newHBaseRow(bytes("row-1"), bytes("q-1"), 101L, bytes("value-1"), (byte) 10));
        List<List<ObObj>> secondRows = new ArrayList<List<ObObj>>();
        secondRows
            .add(newHBaseRow(bytes("row-2"), bytes("q-2"), 102L, bytes("value-2"), (byte) 11));

        ObTableQueryResult first = decode(
            newQueryResult(new String[] { "K", "Q", "T", "V" }, firstRows), false);
        ObTableQueryResult second = decode(
            newQueryResult(new String[] { "K", "Q", "T", "V" }, secondRows), false);

        assertEquals(10, first.getPropertiesRows().get(0).get(0).getMeta().getScale());
        assertEquals(11, second.getPropertiesRows().get(0).get(0).getMeta().getScale());
        assertNotSame(first.getPropertiesRows().get(0).get(0).getMeta(), second.getPropertiesRows()
            .get(0).get(0).getMeta());
    }

    @Test
    public void testCompactBatchCanBeEncodedWithoutMaterializingRows() {
        List<List<ObObj>> rows = new ArrayList<List<ObObj>>();
        rows.add(newHBaseRow(bytes("row-1"), bytes("q-1"), 101L, bytes("value-1"),
            (byte) 10));
        rows.add(newHBaseRow(bytes("row-2"), bytes("q-2"), 102L, bytes("value-2"),
            (byte) 10));

        ObTableQueryResult decoded = decode(
            newQueryResult(new String[] { "K", "Q", "T", "V" }, rows), true);
        assertTrue(decoded.hasHBaseCellBatch());

        ObTableQueryResult roundTripped = decode(decoded, false);
        assertTrue(decoded.hasHBaseCellBatch());
        assertTrue(roundTripped.hasHBaseCellBatch());
        assertRowsEqual(rows, roundTripped.getPropertiesRows());
    }

    @Test
    public void testEmptyKqtvResult() {
        ObTableQueryResult result = decode(
            newQueryResult(new String[] { "K", "Q", "T", "V" }, new ArrayList<List<ObObj>>()),
            false);

        assertEquals(0, result.getRowCount());
        assertTrue(result.getPropertiesRows().isEmpty());
    }

    @Test
    public void testDecodeBinaryColumnFromHeapAndDirectBuffer() {
        byte[] expected = bytes("binary-value");
        byte[] encoded = Serialization.encodeBytesString(new ObBytesString(expected));

        ByteBuf heapBuf = Unpooled.wrappedBuffer(encoded);
        try {
            assertArrayEquals(expected, Serialization.decodeBinaryColumn(heapBuf));
            assertFalse(heapBuf.isReadable());
        } finally {
            heapBuf.release();
        }

        ByteBuf directBuf = PooledByteBufAllocator.DEFAULT.directBuffer(encoded.length);
        try {
            directBuf.writeBytes(encoded);
            assertArrayEquals(expected, Serialization.decodeBinaryColumn(directBuf));
            assertFalse(directBuf.isReadable());
        } finally {
            directBuf.release();
        }
    }

    @Test
    public void testDecodeEmptyBinaryColumn() {
        byte[] encoded = Serialization.encodeBytesString(new ObBytesString(new byte[0]));
        ByteBuf buf = Unpooled.wrappedBuffer(encoded);
        try {
            assertArrayEquals(new byte[0], Serialization.decodeBinaryColumn(buf));
            assertFalse(buf.isReadable());
        } finally {
            buf.release();
        }
    }

    @Test
    public void testDecodeBinaryColumnRejectsTruncatedValue() {
        ByteBuf buf = Unpooled.buffer();
        try {
            buf.writeBytes(Serialization.encodeVi32(2));
            buf.writeByte(1);
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> Serialization.decodeBinaryColumn(buf));
            assertTrue(exception.getMessage().contains("length"));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testDecodeBinaryColumnRejectsNegativeLength() {
        ByteBuf buf = Unpooled.wrappedBuffer(Serialization.encodeVi32(-1));
        try {
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> Serialization.decodeBinaryColumn(buf));
            assertTrue(exception.getMessage().contains("length"));
        } finally {
            buf.release();
        }
    }

    @Test
    public void testDecodeBinaryColumnRejectsInvalidTerminator() {
        ByteBuf buf = Unpooled.buffer();
        try {
            buf.writeBytes(Serialization.encodeVi32(1));
            buf.writeByte(1);
            buf.writeByte(2);
            IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> Serialization.decodeBinaryColumn(buf));
            assertTrue(exception.getMessage().contains("terminator"));
        } finally {
            buf.release();
        }
    }

    private ObTableQueryResult decode(ObTableQueryResult encodedResult, boolean direct) {
        byte[] encoded = encodedResult.encode();
        ByteBuf buf = direct ? PooledByteBufAllocator.DEFAULT.directBuffer(encoded.length)
            : Unpooled.buffer(encoded.length);
        try {
            buf.writeBytes(encoded);
            ObTableQueryResult result = new ObTableQueryResult();
            result.decode(buf);
            assertFalse(buf.isReadable());
            return result;
        } finally {
            buf.release();
        }
    }

    private ObTableQueryResult newQueryResult(String[] propertyNames, List<List<ObObj>> rows) {
        ObTableQueryResult result = new ObTableQueryResult();
        for (String propertyName : propertyNames) {
            result.addPropertiesName(propertyName);
        }
        result.addAllPropertiesRows(rows);
        result.setRowCount(rows.size());
        return result;
    }

    private List<List<ObObj>> copyRows(List<List<ObObj>> rows) {
        List<List<ObObj>> copiedRows = new ArrayList<List<ObObj>>(rows.size());
        for (List<ObObj> row : rows) {
            copiedRows.add(newHBaseRow((byte[]) row.get(0).getValue(), (byte[]) row.get(1)
                .getValue(), (Long) row.get(2).getValue(), (byte[]) row.get(3).getValue(),
                row.get(0).getMeta().getScale()));
        }
        return copiedRows;
    }

    private List<ObObj> newHBaseRow(byte[] key, byte[] qualifier, long timestamp, byte[] value,
                                    byte scale) {
        List<ObObj> row = new ArrayList<ObObj>(4);
        row.add(newBinaryObj(key, scale));
        row.add(newBinaryObj(qualifier, scale));
        row.add(new ObObj(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_NUMERIC,
            ObCollationType.CS_TYPE_BINARY, scale), timestamp));
        row.add(newBinaryObj(value, scale));
        return row;
    }

    private List<ObObj> newTextKeyRow(String key, byte[] qualifier, long timestamp, byte[] value) {
        List<ObObj> row = new ArrayList<ObObj>(4);
        row.add(new ObObj(new ObObjMeta(ObObjType.ObVarcharType,
            ObCollationLevel.CS_LEVEL_EXPLICIT, ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI,
            (byte) 10), key));
        row.add(newBinaryObj(qualifier, (byte) 10));
        row.add(new ObObj(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_NUMERIC,
            ObCollationType.CS_TYPE_BINARY, (byte) 10), timestamp));
        row.add(newBinaryObj(value, (byte) 10));
        return row;
    }

    private ObObj newBinaryObj(byte[] value, byte scale) {
        return new ObObj(new ObObjMeta(ObObjType.ObVarcharType, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, scale), value);
    }

    private void assertRowsEqual(List<List<ObObj>> expectedRows, List<List<ObObj>> actualRows) {
        assertEquals(expectedRows.size(), actualRows.size());
        for (int rowIndex = 0; rowIndex < expectedRows.size(); rowIndex++) {
            List<ObObj> expectedRow = expectedRows.get(rowIndex);
            List<ObObj> actualRow = actualRows.get(rowIndex);
            assertEquals(expectedRow.size(), actualRow.size());
            for (int columnIndex = 0; columnIndex < expectedRow.size(); columnIndex++) {
                ObObj expected = expectedRow.get(columnIndex);
                ObObj actual = actualRow.get(columnIndex);
                if (columnIndex == 2) {
                    assertEquals(expected.getValue(), actual.getValue());
                } else {
                    assertArrayEquals((byte[]) expected.getValue(), (byte[]) actual.getValue());
                }
                assertEquals(expected.getMeta().getType(), actual.getMeta().getType());
                assertEquals(expected.getMeta().getCsLevel(), actual.getMeta().getCsLevel());
                assertEquals(expected.getMeta().getCsType(), actual.getMeta().getCsType());
                assertEquals(expected.getMeta().getScale(), actual.getMeta().getScale());
            }
        }
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

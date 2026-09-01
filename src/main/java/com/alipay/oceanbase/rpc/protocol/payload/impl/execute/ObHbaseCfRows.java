/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ObHbaseCfRows extends AbstractPayload {
    private String realTableName; // column family
    private List<Integer> keyIndex = new ArrayList<>(); // original keys index
    private List<Integer> cellNumArray = new ArrayList<>(); // the number of original cells to each key
    private List<ObHbaseCell> cells = new ArrayList<>(); // original cells

    private byte[][] compactQualifierArrays;
    private int[]    compactQualifierOffsets;
    private int[]    compactQualifierLengths;
    private long[]   compactTimestamps;
    private byte[][] compactValueArrays;
    private int[]    compactValueOffsets;
    private int[]    compactValueLengths;
    private int[]    compactCellContentSizes;
    private long[]   compactKeyRunTtls;
    private boolean  compactMode;
    private int      compactCellCount;
    private int      compactKeyRunCount;
    private int      compactRunRemainingCells;
    private long     compactRunTtl = Long.MAX_VALUE;
    private long     compactCellsPayloadSize;

    public ObHbaseCfRows() {}

    public ObHbaseCfRows(List<Integer> keyIndex, List<Integer> cellNumArray, List<ObHbaseCell> cells) {
        this.keyIndex = keyIndex;
        this.cellNumArray = cellNumArray;
        this.cells = cells;
    }

    public String getRealTableName() {
        return realTableName;
    }

    public void setRealTableName(String realTableName) {
        this.realTableName = realTableName;
    }

    public void add(Integer index, Integer cellNum, List<ObHbaseCell> cells) {
        ensureLegacyMode();
        this.keyIndex.add(index);
        this.cellNumArray.add(cellNum);
        this.cells.addAll(cells);
    }

    /**
     * Begin a key's cell run without a temporary {@code List<ObHbaseCell>}.
     * Call {@link #appendCell(ObHbaseCell)} {@code cellCount} times next.
     */
    public void beginKeyCells(int index, int cellCount) {
        ensureLegacyMode();
        this.keyIndex.add(index);
        this.cellNumArray.add(cellCount);
    }

    /** Append one cell after {@link #beginKeyCells(int, int)}. */
    public void appendCell(ObHbaseCell cell) {
        ensureLegacyMode();
        this.cells.add(cell);
    }

    /** Begin a key's compact Put cell run. TTL is shared by all cells in this run. */
    public void beginCompactKeyCells(int index, int cellCount, long ttl) {
        ensureCompactMode();
        compactMode = true;
        if (cellCount < 0) {
            throw new IllegalArgumentException("cell count is negative: " + cellCount);
        }
        if (compactRunRemainingCells != 0) {
            throw new IllegalStateException("previous compact key run is incomplete: "
                                            + compactRunRemainingCells);
        }
        ensureCompactKeyRunCapacity(compactKeyRunCount + 1);
        keyIndex.add(index);
        cellNumArray.add(cellCount);
        compactKeyRunTtls[compactKeyRunCount++] = ttl;
        compactRunRemainingCells = cellCount;
        compactRunTtl = ttl;
        resetPayloadContentSize();
    }

    /** Append one compact Put cell after {@link #beginCompactKeyCells(int, int, long)}. */
    public void appendCompactCell(byte[] qualifier, int qualifierOffset, int qualifierLength,
                                  long timestamp, byte[] value, int valueOffset, int valueLength) {
        ensureCompactMode();
        if (compactRunRemainingCells <= 0) {
            throw new IllegalStateException("no compact key run is awaiting cells");
        }
        checkSlice(qualifier, qualifierOffset, qualifierLength, "qualifier");
        checkSlice(value, valueOffset, valueLength, "value");
        ensureCompactCellCapacity(compactCellCount + 1);

        compactQualifierArrays[compactCellCount] = qualifier;
        compactQualifierOffsets[compactCellCount] = qualifierOffset;
        compactQualifierLengths[compactCellCount] = qualifierLength;
        compactTimestamps[compactCellCount] = timestamp;
        compactValueArrays[compactCellCount] = value;
        compactValueOffsets[compactCellCount] = valueOffset;
        compactValueLengths[compactCellCount] = valueLength;
        boolean hasTtl = compactRunTtl != Long.MAX_VALUE;
        long cellContentSize = ObHbasePutCellCodec.getCellContentSize(qualifierLength, timestamp,
            valueLength, hasTtl, compactRunTtl);
        if (cellContentSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("compact cell content is too large: "
                                               + cellContentSize);
        }
        compactCellContentSizes[compactCellCount] = (int) cellContentSize;
        long cellPayloadSize = ObHbasePutCellCodec.getCellPayloadSize(cellContentSize);
        if (compactCellsPayloadSize > Long.MAX_VALUE - cellPayloadSize) {
            throw new IllegalArgumentException("compact cells payload size overflow");
        }
        compactCellsPayloadSize += cellPayloadSize;
        compactCellCount++;
        compactRunRemainingCells--;
        payLoadContentSize = INVALID_PAYLOAD_CONTENT_SIZE;
    }

    /** Reserve compact cell arrays without creating per-cell objects. */
    public void reserveAdditionalCompactCells(int additionalCapacity) {
        ensureCompactMode();
        compactMode = true;
        if (additionalCapacity < 0 || additionalCapacity > Integer.MAX_VALUE - compactCellCount) {
            throw new IllegalArgumentException("invalid additional compact cell capacity: "
                                               + additionalCapacity);
        }
        ensureCompactCellCapacity(compactCellCount + additionalCapacity);
    }

    public boolean hasCompactCells() {
        return compactMode;
    }

    /** Return the first cell qualifier in the representation used by table routing. */
    public Object getFirstCellQualifierValue() {
        if (!hasCompactCells()) {
            return getFirstLegacyCell().getQ().getValue();
        }
        validateCompactFirstCell();
        byte[] qualifier = compactQualifierArrays[0];
        int offset = compactQualifierOffsets[0];
        int length = compactQualifierLengths[0];
        return offset == 0 && length == qualifier.length ? qualifier : new ObBytesString(qualifier,
            offset, length);
    }

    /** Return the first cell timestamp in the representation used by table routing. */
    public Object getFirstCellTimestampValue() {
        if (!hasCompactCells()) {
            return getFirstLegacyCell().getT().getValue();
        }
        validateCompactFirstCell();
        return compactTimestamps[0];
    }

    /** Hint the number of key runs that will be appended. */
    public void reserveKeyRuns(int minCapacity) {
        if (keyIndex instanceof ArrayList) {
            ((ArrayList<Integer>) keyIndex).ensureCapacity(minCapacity);
        }
        if (cellNumArray instanceof ArrayList) {
            ((ArrayList<Integer>) cellNumArray).ensureCapacity(minCapacity);
        }
    }

    /** Reserve room for another cell run without over-allocating other families. */
    public void reserveAdditionalCells(int additionalCapacity) {
        ensureLegacyMode();
        if (additionalCapacity < 0 || additionalCapacity > Integer.MAX_VALUE - cells.size()) {
            throw new IllegalArgumentException("invalid additional cell capacity: "
                                               + additionalCapacity);
        }
        if (cells instanceof ArrayList) {
            ((ArrayList<ObHbaseCell>) cells).ensureCapacity(cells.size() + additionalCapacity);
        }
    }

    public List<Integer> getKeyIndex() {
        return keyIndex;
    }

    public int getKeyIndex(int idx) {
        return keyIndex.get(idx);
    }

    public List<Integer> getCellNumArray() {
        return cellNumArray;
    }

    public int getCellNum(int idx) {
        return cellNumArray.get(idx);
    }

    public List<ObHbaseCell> getCells() {
        return cells;
    }

    public void encode(ObByteBuf buf) {
        // 0. encode header
        encodeHeader(buf);

        // 1. encode family
        Serialization.encodeVString(buf, realTableName);

        // 2. encode keyIndex
        Serialization.encodeVi64(buf, keyIndex.size());
        for (long idx : keyIndex) {
            Serialization.encodeVi64(buf, idx);
        }

        // 3. encode cellNumArray
        Serialization.encodeVi64(buf, cellNumArray.size());
        for (long cellNum : cellNumArray) {
            Serialization.encodeVi64(buf, cellNum);
        }

        // 4. encode cells without length
        if (hasCompactCells()) {
            encodeCompactCells(buf);
        } else {
            for (ObHbaseCell cell : cells) {
                cell.encode(buf);
            }
        }
    }

    @Override
    public byte[] encode() {
        long payloadSize = getPayloadSize();
        if (payloadSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("ObHbaseCfRows payload is too large: "
                                               + payloadSize);
        }
        ObByteBuf buf = new ObByteBuf((int) payloadSize);
        encode(buf);
        if (buf.pos != buf.bytes.length) {
            throw new IllegalArgumentException("error in encode ObHbaseCfRows (pos:" + buf.pos
                                               + ", capacity:" + buf.bytes.length + ")");
        }
        return buf.bytes;
    }

    @Override
    public long getPayloadContentSize() {
        if (this.payLoadContentSize == INVALID_PAYLOAD_CONTENT_SIZE) {
            long payloadContentSize = 0;
            // add family len
            payloadContentSize += Serialization.getNeedBytes(realTableName);

            // add key index array size and index
            payloadContentSize += Serialization.getNeedBytes(keyIndex.size());
            for (Integer index : keyIndex) {
                payloadContentSize += Serialization.getNeedBytes(index);
            }

            // add cell num array size and cell num
            payloadContentSize += Serialization.getNeedBytes(cellNumArray.size());
            for (Integer cellNum : cellNumArray) {
                payloadContentSize += Serialization.getNeedBytes(cellNum);
            }

            // only add cells size
            if (hasCompactCells()) {
                validateCompactState();
                payloadContentSize += compactCellsPayloadSize;
            } else {
                for (ObHbaseCell cell : cells) {
                    payloadContentSize += cell.getPayloadSize();
                }
            }
            this.payLoadContentSize = payloadContentSize;
        }
        return this.payLoadContentSize;
    }

    @Override
    public void resetPayloadContentSize() {
        super.resetPayloadContentSize();
        if (!hasCompactCells()) {
            for (ObHbaseCell cell : cells) {
                if (cell != null) {
                    cell.resetPayloadContentSize();
                }
            }
        }
    }

    private void encodeCompactCells(ObByteBuf buf) {
        validateCompactState();
        int cellIndex = 0;
        for (int runIndex = 0; runIndex < compactKeyRunCount; runIndex++) {
            long ttl = compactKeyRunTtls[runIndex];
            int cellCount = cellNumArray.get(runIndex);
            for (int i = 0; i < cellCount; i++, cellIndex++) {
                ObHbasePutCellCodec.encodeCell(buf, compactQualifierArrays[cellIndex],
                    compactQualifierOffsets[cellIndex], compactQualifierLengths[cellIndex],
                    compactTimestamps[cellIndex], compactValueArrays[cellIndex],
                    compactValueOffsets[cellIndex], compactValueLengths[cellIndex],
                    ttl != Long.MAX_VALUE, ttl, compactCellContentSizes[cellIndex]);
            }
        }
    }

    private void validateCompactState() {
        if (compactRunRemainingCells != 0) {
            throw new IllegalStateException("compact key run is incomplete: "
                                            + compactRunRemainingCells);
        }
        if (compactKeyRunCount != cellNumArray.size()) {
            throw new IllegalStateException("compact key run count mismatch: runs="
                                            + compactKeyRunCount + ", cellNumArray="
                                            + cellNumArray.size());
        }
        long expectedCellCount = 0;
        for (Integer cellCount : cellNumArray) {
            expectedCellCount += cellCount;
        }
        if (expectedCellCount != compactCellCount) {
            throw new IllegalStateException("compact cell count mismatch: expected="
                                            + expectedCellCount + ", actual=" + compactCellCount);
        }
    }

    private void validateCompactFirstCell() {
        if (compactCellCount == 0) {
            throw new IllegalStateException("compact cell list is empty");
        }
    }

    private ObHbaseCell getFirstLegacyCell() {
        if (cells.isEmpty()) {
            throw new IllegalStateException("cell list is empty");
        }
        return cells.get(0);
    }

    private void ensureLegacyMode() {
        if (hasCompactCells()) {
            throw new IllegalStateException("legacy and compact cells cannot be mixed");
        }
    }

    private void ensureCompactMode() {
        if (!cells.isEmpty()) {
            throw new IllegalStateException("legacy and compact cells cannot be mixed");
        }
    }

    private void ensureCompactCellCapacity(int requiredCapacity) {
        int oldCapacity = compactQualifierArrays == null ? 0 : compactQualifierArrays.length;
        if (requiredCapacity <= oldCapacity) {
            return;
        }
        int newCapacity = expandedCapacity(oldCapacity, requiredCapacity);
        compactQualifierArrays = copyOf(compactQualifierArrays, newCapacity);
        compactQualifierOffsets = copyOf(compactQualifierOffsets, newCapacity);
        compactQualifierLengths = copyOf(compactQualifierLengths, newCapacity);
        compactTimestamps = copyOf(compactTimestamps, newCapacity);
        compactValueArrays = copyOf(compactValueArrays, newCapacity);
        compactValueOffsets = copyOf(compactValueOffsets, newCapacity);
        compactValueLengths = copyOf(compactValueLengths, newCapacity);
        compactCellContentSizes = copyOf(compactCellContentSizes, newCapacity);
    }

    private void ensureCompactKeyRunCapacity(int requiredCapacity) {
        int oldCapacity = compactKeyRunTtls == null ? 0 : compactKeyRunTtls.length;
        if (requiredCapacity > oldCapacity) {
            compactKeyRunTtls = copyOf(compactKeyRunTtls,
                expandedCapacity(oldCapacity, requiredCapacity));
        }
    }

    private static int expandedCapacity(int oldCapacity, int requiredCapacity) {
        int candidate = oldCapacity == 0 ? 8 : oldCapacity + (oldCapacity >> 1);
        if (candidate < requiredCapacity) {
            candidate = requiredCapacity;
        }
        if (candidate < 0) {
            throw new IllegalArgumentException("compact cell capacity overflow: "
                                               + requiredCapacity);
        }
        return candidate;
    }

    private static byte[][] copyOf(byte[][] source, int length) {
        return source == null ? new byte[length][] : Arrays.copyOf(source, length);
    }

    private static int[] copyOf(int[] source, int length) {
        return source == null ? new int[length] : Arrays.copyOf(source, length);
    }

    private static long[] copyOf(long[] source, int length) {
        return source == null ? new long[length] : Arrays.copyOf(source, length);
    }

    private static void checkSlice(byte[] bytes, int offset, int length, String name) {
        if (bytes == null || offset < 0 || length < 0 || offset > bytes.length - length) {
            throw new IllegalArgumentException("invalid " + name + " slice: offset=" + offset
                                               + ", length=" + length + ", capacity="
                                               + (bytes == null ? -1 : bytes.length));
        }
    }

}

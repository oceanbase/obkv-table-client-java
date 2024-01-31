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

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableSerialUtil;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.*;

public class ObTableSingleOpQuery extends AbstractPayload {
    private String indexName;
    private List<String> scanRangeColumns = new ArrayList<>();
    private byte[] scanRangeBitMap = null;
    private long scanRangeBitLen = 0;
    private List<String> aggColumnNames = new ArrayList<>();

    private List<ObNewRange> scanRanges = new ArrayList<>();

    private String filterString;

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode index name
        int len =  Serialization.getNeedBytes(indexName);
        System.arraycopy(Serialization.encodeVString(indexName), 0, bytes, idx, len);
        idx += len;

        // 2. encode scan ranges columns
        len = Serialization.getNeedBytes(scanRangeBitLen);
        System.arraycopy(Serialization.encodeVi64(scanRangeBitLen), 0, bytes, idx, len);
        idx += len;
        for (byte b : scanRangeBitMap) {
            System.arraycopy(Serialization.encodeI8(b), 0, bytes, idx, 1);
            idx += 1;
        }

        // 3. encode scan ranges
        len = Serialization.getNeedBytes(scanRanges.size());
        System.arraycopy(Serialization.encodeVi64(scanRanges.size()), 0, bytes, idx, len);
        idx += len;
        for (ObNewRange range : scanRanges) {
            len =  ObTableSerialUtil.getEncodedSize(range);
            System.arraycopy(ObTableSerialUtil.encode(range), 0, bytes, idx, len);
            idx += len;
        }

        // 4. encode filter string
        len =  Serialization.getNeedBytes(filterString);
        System.arraycopy(Serialization.encodeVString(filterString), 0, bytes, idx, len);
        idx += len;

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode header
        super.decode(buf);

        // 1. decode tablet id
        this.indexName = Serialization.decodeVString(buf);

        // 2. decode scan ranges columns
        scanRangeBitLen = Serialization.decodeVi64(buf);
        scanRangeBitMap = new byte[(int) Math.ceil(scanRangeBitLen / 8.0)];
        for (int i = 0; i < scanRangeBitMap.length; i++) {
            scanRangeBitMap[i] = Serialization.decodeI8(buf);
            for (int j = 0; j < 8; i++) {
                if ((scanRangeBitMap[i] & (1 << j)) != 0) {
                    if (i * 8 + j < aggColumnNames.size()) {
                        scanRangeColumns.add(aggColumnNames.get(i * 8 + j));
                    }
                }
            }
        }

        // 3. decode scan ranges
        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            ObNewRange range = new ObNewRange();
            ObTableSerialUtil.decode(buf, range);
            scanRanges.add(range);
        }

        // 4. decode filter string
        this.filterString = Serialization.decodeVString(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long payloadContentSize = 0;

        payloadContentSize += Serialization.getNeedBytes(scanRangeBitLen);
        payloadContentSize += scanRangeBitMap.length;

        payloadContentSize += Serialization.getNeedBytes(scanRanges.size());
        for (ObNewRange range : scanRanges) {
            payloadContentSize += ObTableSerialUtil.getEncodedSize(range);
        }

        return payloadContentSize + Serialization.getNeedBytes(indexName)
                + Serialization.getNeedBytes(filterString);
    }

    // Support class, which is used for column name sorted
    private static class ColumnNamePair implements Comparable<ColumnNamePair> {
        long number;
        long origin_idx;

        ColumnNamePair(long number, long obj) {
            this.number = number;
            this.origin_idx = obj;
        }

        @Override
        public int compareTo(ColumnNamePair other) {
            return Long.compare(this.number, other.number);
        }
    }

    /*
     * adjustRowkeyColumnName should be execute in the last
     */
    public void adjustScanRangeColumns(Map<String, Long> columnNameIdxMap) {
        this.scanRangeBitLen = columnNameIdxMap.size();
        int size = (int) Math.ceil(columnNameIdxMap.size() / 8.0);
        byte[] byteArray = new byte[size];
        List<Long> columnNameIdx = new LinkedList<>();


        for (String name : scanRangeColumns) {
            Long index = columnNameIdxMap.get(name);
            columnNameIdx.add(index);
            if (index != null) {
                int byteIndex = index.intValue() / 8;
                int bitIndex = index.intValue() % 8;
                byteArray[byteIndex] |= (byte) (1 << bitIndex);
            }
        }

        List<ColumnNamePair> pairs = new ArrayList<>();
        for (int i = 0; i < columnNameIdx.size(); i++) {
            pairs.add(new ColumnNamePair(columnNameIdx.get(i), i));
        }

        Collections.sort(pairs);

        for (ObNewRange range : scanRanges) {
            List<ObObj> startKey= range.getStartKey().getObjs();
            List<ObObj> endKey= range.getStartKey().getObjs();
            List<ObObj> adjustStartKey = new ArrayList<>(startKey.size());
            List<ObObj> adjustEndtKey = new ArrayList<>(endKey.size());

            for (ColumnNamePair pair : pairs) {
                adjustStartKey.add(startKey.get((int) pair.origin_idx));
                adjustEndtKey.add(endKey.get((int) pair.origin_idx));
            }
            range.getStartKey().setObjs(adjustStartKey);
            range.getEndKey().setObjs(adjustEndtKey);
        }

        this.scanRangeBitMap = byteArray;
    }

    public List<ObNewRange> getScanRanges() {
        return scanRanges;
    }

    public void setScanRanges(List<ObNewRange> scanRanges) {
        this.scanRanges = scanRanges;
    }

    public void addScanRange(ObNewRange scanRange) {
        this.scanRanges.add(scanRange);
    }

    public void addScanRangeColumns(List<String> scanRangeColumns) {
        this.scanRangeColumns = scanRangeColumns;
    }

    public String getFilterString() {
        return filterString;
    }

    public void setFilterString(String filterString) {
        this.filterString = filterString;
    }

    public List<String> getScanRangeColumns() {
        return scanRangeColumns;
    }

    public void setAggColumnNames(List<String> columnNames) {
        this.aggColumnNames = columnNames;
    }
}

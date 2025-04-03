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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableSerialUtil;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.table.ObHBaseParams;
import com.alipay.oceanbase.rpc.table.ObKVParams;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.*;

class StaticObHTableFilter {
    public static ObHTableFilter hFilter = new ObHTableFilter();
    public static byte[] HFilterBytes = null;
    static {
        hFilter.setMaxVersions(1);
        HFilterBytes = hFilter.encode();
    }

    public static byte[] getHfilterBytes() {
        if (HFilterBytes == null) {
            hFilter.setMaxVersions(1);
            HFilterBytes = hFilter.encode();
        }
        return HFilterBytes;
    }
}

class StaticObKvParams {
    public static ObKVParams kvParams = new ObKVParams();
    public static ObHBaseParams hBaseParams = new ObHBaseParams();
    public static byte[] kvParamBytes = null;
    static {
        kvParams.setObParamsBase(hBaseParams);
        kvParamBytes = kvParams.encode();
    }
    public static byte[] getkvParamBytes() {
        if (kvParamBytes == null) {
            kvParams.setObParamsBase(hBaseParams);
            kvParamBytes = kvParams.encode();
        }
        return kvParamBytes;
    }

}

class StaticHbaseColumns {
    public static byte[] selectColumnSize = Serialization.encodeVi64(4L);
    public static byte[] RowkeyColumSize = Serialization.encodeVi64(3L);
    public static byte[] VColumSize = Serialization.encodeVi64(1L);
    public static byte[] KQTVBytes = null;
    public static byte[] KQTBytes = null;
    public static byte[] VBytes = null;
    public static byte[] getKQTVBytes() {
        if (KQTVBytes == null) {
            byte[] K = Serialization.encodeVString("K");
            byte[] Q = Serialization.encodeVString("Q");
            byte[] T = Serialization.encodeVString("T");
            byte[] V = Serialization.encodeVString("V");
            KQTVBytes = new byte[K.length + Q.length + T.length + V.length];
            ObByteBuf buf = new ObByteBuf(KQTVBytes);
            buf.writeBytes(K);
            buf.writeBytes(Q);
            buf.writeBytes(T);
            buf.writeBytes(V);
        }
        return KQTVBytes;
    }
    public static byte[] getKQTBytes() {
        if (KQTBytes == null) {
            byte[] K = Serialization.encodeVString("K");
            byte[] Q = Serialization.encodeVString("Q");
            byte[] T = Serialization.encodeVString("T");
            KQTBytes = new byte[K.length + Q.length + T.length];
            ObByteBuf buf = new ObByteBuf(KQTBytes);
            buf.writeBytes(K);
            buf.writeBytes(Q);
            buf.writeBytes(T);
        }
        return KQTBytes;
    }
    public static byte[] getVBytes() {
        if (VBytes == null) {
            byte[] V = Serialization.encodeVString("V");
            VBytes = new byte[V.length];
            ObByteBuf buf = new ObByteBuf(VBytes);
            buf.writeBytes(V);
        }
        return VBytes;
    }
}

class StaticIndexName {
    public static byte[] indexName = Serialization.encodeVString("PRIMARY");
}

class StaticEmptyStr {
    public static byte[] EmptyStr = Serialization.encodeVString("");
}

public class ObTableSingleOpQuery extends ObTableQuery {
    private List<String> scanRangeColumns = new ArrayList<>();
    private byte[] scanRangeBitMap = null;
    private long scanRangeBitLen = 0;
    private List<String> aggColumnNames = new ArrayList<>();

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
        byte[] indexNameBytes =  Serialization.encodeVString(indexName);
        System.arraycopy(indexNameBytes, 0, bytes, idx, indexNameBytes.length);
        idx += indexNameBytes.length;

        // 2. encode scan ranges columns
        byte[] scanRangeBitLenBytes = Serialization.encodeVi64(scanRangeBitLen);
        System.arraycopy(scanRangeBitLenBytes, 0, bytes, idx, scanRangeBitLenBytes.length);
        idx += scanRangeBitLenBytes.length;
        for (byte b : scanRangeBitMap) {
            System.arraycopy(Serialization.encodeI8(b), 0, bytes, idx, 1);
            idx += 1;
        }

        // 3. encode scan ranges
        byte[] keyRangesBytes = Serialization.encodeVi64(keyRanges.size());
        System.arraycopy(keyRangesBytes, 0, bytes, idx, keyRangesBytes.length);
        idx += keyRangesBytes.length;
        for (ObNewRange range : keyRanges) {
            byte[] rangeBytes =  ObTableSerialUtil.encode(range);
            System.arraycopy(rangeBytes, 0, bytes, idx, rangeBytes.length);
            idx += rangeBytes.length;
        }

        // 4. encode filter string
        byte[] filterStringBytes =  Serialization.encodeVString(filterString);
        System.arraycopy(filterStringBytes, 0, bytes, idx, filterStringBytes.length);
        idx += filterStringBytes.length;

        // encode HBase Batch Get required
        if (isHbaseQuery && ObGlobal.isHBaseBatchGetSupport()) {
            byte[] selectColumnsLenByets = Serialization.encodeVi64(selectColumns.size());
            System.arraycopy(selectColumnsLenByets, 0, bytes, idx, selectColumnsLenByets.length);
            idx += selectColumnsLenByets.length;
            for (String selectColumn : selectColumns) {
                byte[] selectColumnLenBytes = Serialization.encodeVString(selectColumn);
                System.arraycopy(selectColumnLenBytes, 0, bytes, idx, selectColumnLenBytes.length);
                idx += selectColumnLenBytes.length;
            }

            System.arraycopy(Serialization.encodeI8(scanOrder.getByteValue()), 0, bytes, idx, 1);
            idx += 1;

            byte[] hTableFilterLenBytes = hTableFilter.encode();
            System.arraycopy(hTableFilterLenBytes, 0, bytes, idx, hTableFilterLenBytes.length);
            idx += hTableFilterLenBytes.length;

            if (obKVParams != null) {
                byte[] obKVParamsBytes = obKVParams.encode();
                System.arraycopy(obKVParamsBytes, 0, bytes, idx, obKVParamsBytes.length);
            } else {
                System.arraycopy(HTABLE_DUMMY_BYTES, 0, bytes, idx, HTABLE_DUMMY_BYTES.length);
            }
        }
        return bytes;
    }

    public void encode(ObByteBuf buf) {
        // 0. encode header
        encodeHeader(buf);

        // 1. encode index name
//        Serialization.encodeVString(buf, indexName);
        buf.writeBytes(StaticIndexName.indexName);

        // 2. encode scan ranges columns
        if (isHbaseQuery && ObGlobal.isHBaseBatchGetSupport()) {
            Serialization.encodeVi64(buf, 3L);
            Serialization.encodeI8(buf, (byte) 0);
        } else {
            Serialization.encodeVi64(buf, scanRangeBitLen);
            for (byte b : scanRangeBitMap) {
                Serialization.encodeI8(buf, b);
            }
        }


        // 3. encode scan ranges
        Serialization.encodeVi64(buf, keyRanges.size());
        for (ObNewRange range : keyRanges) {
            ObTableSerialUtil.encode(buf, range);
        }

        // 4. encode filter string
//        Serialization.encodeVString(buf, filterString);
        buf.writeBytes(StaticEmptyStr.EmptyStr);

        // encode HBase Batch Get required
        if (isHbaseQuery && ObGlobal.isHBaseBatchGetSupport()) {
            buf.writeBytes(StaticHbaseColumns.selectColumnSize);
            buf.writeBytes(StaticHbaseColumns.getKQTVBytes());
            Serialization.encodeI8(buf, scanOrder.getByteValue());

            buf.writeBytes(StaticObHTableFilter.getHfilterBytes());

            if (obKVParams != null) {
//                obKVParams.encode(buf);
                buf.writeBytes(StaticObKvParams.getkvParamBytes());
            } else {
                buf.writeBytes(HTABLE_DUMMY_BYTES);
            }
        }
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode header
        Serialization.decodeVi64(buf);
        Serialization.decodeVi64(buf);
        // 1. decode tablet id
        this.indexName = Serialization.decodeVString(buf);

        // 2. decode scan ranges columns
        scanRangeBitLen = Serialization.decodeVi64(buf);
        scanRangeBitMap = new byte[(int)(scanRangeBitLen / 8.0) + 1];
        for (int i = 0; i < scanRangeBitMap.length; i++) {
            scanRangeBitMap[i] = Serialization.decodeI8(buf);
            for (int j = 0; j < 8; j++) {
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
            keyRanges.add(range);
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
        if (this.payLoadContentSize == -1) {
            long payloadContentSize = 0;
            if (isHbaseQuery && ObGlobal.isHBaseBatchGetSupport()) {
                payloadContentSize += Serialization.getNeedBytes(3L);
                payloadContentSize += 1;
            } else {
                payloadContentSize += Serialization.getNeedBytes(scanRangeBitLen);
                payloadContentSize += scanRangeBitMap.length;
            }


            payloadContentSize += Serialization.getNeedBytes(keyRanges.size());
            for (ObNewRange range : keyRanges) {
                payloadContentSize += ObTableSerialUtil.getEncodedSize(range);
            }

            payloadContentSize += StaticIndexName.indexName.length;
            payloadContentSize += StaticEmptyStr.EmptyStr.length;

            // calculate part required by HBase Batch Get
            if (isHbaseQuery && ObGlobal.isHBaseBatchGetSupport()) {
                payloadContentSize += StaticHbaseColumns.selectColumnSize.length;
                payloadContentSize += StaticHbaseColumns.getKQTVBytes().length;
                payloadContentSize += 1; // scanOrder

                if (isHbaseQuery) {
                    payloadContentSize += StaticObHTableFilter.getHfilterBytes().length;
                } else {
                    payloadContentSize += HTABLE_DUMMY_BYTES.length;
                }
                if (isHbaseQuery && obKVParams != null) {
//                    payloadContentSize += obKVParams.getPayloadSize();
                    payloadContentSize += StaticObKvParams.getkvParamBytes().length;
                } else {
                    payloadContentSize += HTABLE_DUMMY_BYTES.length;
                }
            }
            this.payLoadContentSize = payloadContentSize;
        }

        return this.payLoadContentSize;
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

        for (ObNewRange range : keyRanges) {
            List<ObObj> startKey= range.getStartKey().getObjs();
            List<ObObj> endKey= range.getStartKey().getObjs();
            List<ObObj> adjustStartKey = new ArrayList<>(startKey.size());
            List<ObObj> adjustEndtKey = new ArrayList<>(endKey.size());

            for (ColumnNamePair pair : pairs) {
                adjustStartKey.add(startKey.get((int) pair.origin_idx));
                adjustEndtKey.add(endKey.get((int) pair.origin_idx));
            }
            if (!adjustStartKey.isEmpty() && !adjustEndtKey.isEmpty()) {
                range.getStartKey().setObjs(adjustStartKey);
                range.getEndKey().setObjs(adjustEndtKey);
            }
        }

        this.scanRangeBitMap = byteArray;
    }

    public List<ObNewRange> getScanRanges() {
        return keyRanges;
    }

    public void setScanRanges(List<ObNewRange> scanRanges) {
        this.keyRanges = scanRanges;
    }

    public void addScanRange(ObNewRange scanRange) {
        this.keyRanges.add(scanRange);
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

    public static ObTableSingleOpQuery getInstance(String indexName,
                                                   List<ObNewRange> keyRanges,
                                                   List<String> selectColumns,
                                                   ObScanOrder scanOrder,
                                                   boolean isHbaseQuery,
                                                   ObHTableFilter obHTableFilter,
                                                   ObKVParams obKVParams,
                                                   String filterString) {
        ObTableSingleOpQuery query = new ObTableSingleOpQuery();
        query.setIndexName(indexName);
        query.setScanRanges(keyRanges);
        query.setSelectColumns(selectColumns);
        query.setScanOrder(scanOrder);
        if (isHbaseQuery) {
            query.sethTableFilter(obHTableFilter);
            query.setObKVParams(obKVParams);
        }
        query.setFilterString(filterString);
        return query;
    }
}

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

import com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class ObTableQueryResult extends AbstractPayload {

    private static final String HBASE_COL_K          = "K";
    private static final String HBASE_COL_Q          = "Q";
    private static final String HBASE_COL_T          = "T";
    private static final String HBASE_COL_V          = "V";
    private static final int    HBASE_KQTV_COL_COUNT = 4;
    private static final int    HBASE_TS_COL_INDEX   = 2;
    private static final int    OB_OBJ_META_SIZE     = 4;

    private ObRpcPacketHeader header;
    private List<String>      propertiesNames = new ArrayList<String>();
    private long              rowCount        = 0;
    // decode to propertiesRows from dataBuffer directly
    // byte[]       dataBuffer;

    // TODO 需要做成流式的，目前 OB 还不支持流式协议，单个 packet 大小过大会失败
    private List<List<ObObj>> propertiesRows  = new ArrayList<List<ObObj>>();
    private ObHBaseCellBatch hbaseCellBatch;

    /*
     * Ob table query result.
     */
    public ObTableQueryResult() {
        this.header = new ObRpcPacketHeader();
    }

    /*
     * Ob table query result.
     */
    public ObTableQueryResult(ObRpcPacketHeader header) {
        this.header = header;
    }

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE_QUERY;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        int len = (int) Serialization.getObUniVersionHeaderLength(getVersion(), getPayloadSize());
        byte[] header = Serialization.encodeObUniVersionHeader(getVersion(), getPayloadSize());
        System.arraycopy(header, 0, bytes, idx, len);
        idx += len;

        // 2. encode it
        len = Serialization.getNeedBytes(propertiesNames.size());
        System.arraycopy(Serialization.encodeVi64(propertiesNames.size()), 0, bytes, idx, len);
        idx += len;
        for (String propertiesName : propertiesNames) {
            len = Serialization.getNeedBytes(propertiesName);
            System.arraycopy(Serialization.encodeVString(propertiesName), 0, bytes, idx, len);
            idx += len;
        }

        len = Serialization.getNeedBytes(rowCount);
        System.arraycopy(Serialization.encodeVi64(rowCount), 0, bytes, idx, len);
        idx += len;

        int resultRowCount = getResultRowCount();
        len = Serialization.getNeedBytes(resultRowCount);
        System.arraycopy(Serialization.encodeVi64(resultRowCount), 0, bytes, idx, len);
        idx += len;
        // ObDataBuffer
        if (hbaseCellBatch != null) {
            for (int rowIndex = 0; rowIndex < hbaseCellBatch.size(); rowIndex++) {
                for (int columnIndex = 0; columnIndex < HBASE_KQTV_COL_COUNT; columnIndex++) {
                    ObObjMeta meta = hbaseCellBatch.getMeta(columnIndex);
                    byte[] metaBytes = meta.encode();
                    System.arraycopy(metaBytes, 0, bytes, idx, metaBytes.length);
                    idx += metaBytes.length;
                    Object value = getHBaseCellValue(hbaseCellBatch, rowIndex, columnIndex);
                    byte[] valueBytes = meta.getType().encode(value);
                    System.arraycopy(valueBytes, 0, bytes, idx, valueBytes.length);
                    idx += valueBytes.length;
                }
            }
        } else {
            for (List<ObObj> row : propertiesRows) {
            for (ObObj obObj : row) {
                len = obObj.getEncodedSize();
                System.arraycopy(obObj.encode(), 0, bytes, idx, len);
                idx += len;
            }
        }
        }

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode version
        super.decode(buf);

        // 1. decode ObTableResult
        // this.header.decode(buf);

        // 2. decode itself
        int propertyCount = checkedCount(Serialization.decodeVi64(buf), "property count");
        List<String> decodedPropertiesNames = new ArrayList<String>(propertyCount);
        for (int i = 0; i < propertyCount; i++) {
            decodedPropertiesNames.add(Serialization.decodeVString(buf));
        }
        long decodedRowCount = Serialization.decodeVi64(buf);
        int resultRowCount = checkedCount(decodedRowCount, "row count");

        // ObDataBuffer
        Serialization.decodeVi64(buf); // dataBuffer length
        List<List<ObObj>> decodedPropertiesRows = new ArrayList<List<ObObj>>(0);
        ObHBaseCellBatch decodedHBaseCellBatch = null;
        if (resultRowCount > 0 && isHBaseKqtvSchema(decodedPropertiesNames)) {
            int rowsStartIndex = buf.readerIndex();
            decodedHBaseCellBatch = tryDecodeHBaseKqtvBatch(buf, resultRowCount);
            if (decodedHBaseCellBatch == null) {
                buf.readerIndex(rowsStartIndex);
                decodedPropertiesRows = decodeGenericRows(buf, resultRowCount, propertyCount);
            }
        } else {
            decodedPropertiesRows = decodeGenericRows(buf, resultRowCount, propertyCount);
        }

        this.propertiesNames = decodedPropertiesNames;
        this.rowCount = decodedRowCount;
        this.propertiesRows = decodedPropertiesRows;
        this.hbaseCellBatch = decodedHBaseCellBatch;

        return this;
    }

    private static List<List<ObObj>> decodeGenericRows(ByteBuf buf, int rowCount, int columnCount) {
        List<List<ObObj>> rows = new ArrayList<List<ObObj>>(rowCount);
        for (int r = 0; r < rowCount; r++) {
            List<ObObj> row = new ArrayList<ObObj>(columnCount);
            for (int i = 0; i < columnCount; i++) {
                ObObj obObj = new ObObj();
                obObj.decode(buf);
                row.add(obObj);
            }
            rows.add(row);
        }

        return rows;
    }

    private static boolean isHBaseKqtvSchema(List<String> propertiesNames) {
        return propertiesNames.size() == HBASE_KQTV_COL_COUNT
               && HBASE_COL_K.equals(propertiesNames.get(0))
               && HBASE_COL_Q.equals(propertiesNames.get(1))
               && HBASE_COL_T.equals(propertiesNames.get(2))
               && HBASE_COL_V.equals(propertiesNames.get(3));
    }

    private static ObHBaseCellBatch tryDecodeHBaseKqtvBatch(ByteBuf buf, int rowCount) {
        HBaseKqtvMetaCache metaCache = new HBaseKqtvMetaCache();
        ObHBaseCellBatch batch = new ObHBaseCellBatch(rowCount);
        for (int r = 0; r < rowCount; r++) {
            byte[] rowKey = null;
            byte[] qualifier = null;
            long timestamp = 0;
            byte[] value = null;
            for (int c = 0; c < HBASE_KQTV_COL_COUNT; c++) {
                ObObjMeta meta;
                if (r == 0) {
                    int metaBits = readMetaBits(buf);
                    meta = new ObObjMeta();
                    meta.decode(buf);
                    if (!isExpectedHBaseMeta(c, meta)) {
                        return null;
                    }
                    metaCache.metaBits[c] = metaBits;
                    metaCache.metas[c] = meta;
                    batch.setMeta(c, meta);
                } else {
                    int actualMetaBits = readMetaBits(buf);
                    if (actualMetaBits != metaCache.metaBits[c]) {
                        throw new IllegalStateException("HBase KQTV meta changed at row " + r
                                                        + ", column " + c);
                    }
                    buf.skipBytes(OB_OBJ_META_SIZE);
                    meta = metaCache.metas[c];
                }

                if (c == 0) {
                    rowKey = Serialization.decodeBinaryColumn(buf);
                } else if (c == 1) {
                    qualifier = Serialization.decodeBinaryColumn(buf);
                } else if (c == HBASE_TS_COL_INDEX) {
                    timestamp = Serialization.decodeVi64(buf);
                } else {
                    value = Serialization.decodeBinaryColumn(buf);
                }
            }
            batch.setCell(r, rowKey, qualifier, timestamp, value);
        }
        return batch;
    }

    private static int readMetaBits(ByteBuf buf) {
        if (buf.readableBytes() < OB_OBJ_META_SIZE) {
            throw new IllegalArgumentException("not enough bytes to decode ObObjMeta");
        }
        return buf.getInt(buf.readerIndex());
    }

    private static boolean isExpectedHBaseMeta(int columnIndex, ObObjMeta meta) {
        if (columnIndex == HBASE_TS_COL_INDEX) {
            return meta.getType() == ObObjType.ObInt64Type;
        }
        return meta.getType() == ObObjType.ObVarcharType
               && meta.getCsType() == ObCollationType.CS_TYPE_BINARY;
    }

    private static final class HBaseKqtvMetaCache {
        private final int[]       metaBits = new int[HBASE_KQTV_COL_COUNT];
        private final ObObjMeta[] metas    = new ObObjMeta[HBASE_KQTV_COL_COUNT];
    }

    private static int checkedCount(long count, String fieldName) {
        if (count < 0 || count > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("invalid " + fieldName + ": " + count);
        }
        return (int) count;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long size = 0;
        // size += header.getPayloadSize();
        size += Serialization.getNeedBytes(propertiesNames.size());
        for (String propertiesName : propertiesNames) {
            size += Serialization.getNeedBytes(propertiesName);
        }
        size += Serialization.getNeedBytes(rowCount);

        size += Serialization.getNeedBytes(getResultRowCount());
        if (hbaseCellBatch != null) {
            for (int rowIndex = 0; rowIndex < hbaseCellBatch.size(); rowIndex++) {
                for (int columnIndex = 0; columnIndex < HBASE_KQTV_COL_COUNT; columnIndex++) {
                    ObObjMeta meta = hbaseCellBatch.getMeta(columnIndex);
                    Object value = getHBaseCellValue(hbaseCellBatch, rowIndex, columnIndex);
                    size += meta.getEncodedSize() + meta.getType().getEncodedSize(value);
                }
            }
        } else {
            for (List<ObObj> row : propertiesRows) {
            for (ObObj obObj : row) {
                size += obObj.getEncodedSize();
                }
            }
        }

        return size;
    }

    /*
     * Get properties names.
     */
    public List<String> getPropertiesNames() {
        return propertiesNames;
    }

    /*
     * Set properties names.
     */
    public void setPropertiesNames(List<String> propertiesNames) {
        this.propertiesNames = propertiesNames;
    }

    /*
     * Add properties name.
     */
    public void addPropertiesName(String propertiesName) {
        this.propertiesNames.add(propertiesName);
    }

    /*
     * Get row count.
     */
    public long getRowCount() {
        return rowCount;
    }

    /*
     * Set row count.
     */
    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    /*
     * Get properties rows.
     */
    public List<List<ObObj>> getPropertiesRows() {
        if (hbaseCellBatch != null) {
            propertiesRows = hbaseCellBatch.materializeRows(0);
            hbaseCellBatch = null;
        }
        return propertiesRows;
    }

    public ObHBaseCellBatch getHBaseCellBatch() {
        return hbaseCellBatch;
    }

    public boolean hasHBaseCellBatch() {
        return hbaseCellBatch != null;
    }

    /*
     * Set properties rows.
     */
    public void setPropertiesRows(List<List<ObObj>> propertiesRows) {
        this.propertiesRows = propertiesRows;
        this.hbaseCellBatch = null;
    }

    /*
     * Add properties row.
     */
    public void addPropertiesRow(List<ObObj> propertiesRow) {
        getPropertiesRows().add(propertiesRow);
    }

    /*
     * Add all properties rows.
     */
    public void addAllPropertiesRows(List<List<ObObj>> propertiesRows) {
        getPropertiesRows().addAll(propertiesRows);
    }

    private int getResultRowCount() {
        return hbaseCellBatch == null ? propertiesRows.size() : hbaseCellBatch.size();
    }

    private static Object getHBaseCellValue(ObHBaseCellBatch batch, int rowIndex,
                                            int columnIndex) {
        switch (columnIndex) {
            case 0:
                return batch.getRowKey(rowIndex);
            case 1:
                return batch.getQualifier(rowIndex);
            case HBASE_TS_COL_INDEX:
                return batch.getTimestamp(rowIndex);
            case 3:
                return batch.getValue(rowIndex);
            default:
                throw new IllegalArgumentException("invalid HBase KQTV column: " + columnIndex);
        }
    }

    /*
     * Is stream.
     */
    public boolean isStream() {
        return header.isStream();
    }

    /*
     * Is stream last.
     */
    public boolean isStreamLast() {
        return header.isStreamLast();
    }

    /*
     * Is stream next.
     */
    public boolean isStreamNext() {
        return header.isStreamNext();
    }

    /*
     * Get session id.
     */
    public long getSessionId() {
        return header.getSessionId();
    }
}

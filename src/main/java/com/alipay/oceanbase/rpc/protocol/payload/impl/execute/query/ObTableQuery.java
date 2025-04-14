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

import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.table.ObFTSParams;
import com.alipay.oceanbase.rpc.table.ObHBaseParams;
import com.alipay.oceanbase.rpc.table.ObKVParams;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregationSingle;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregationType;
import com.alipay.oceanbase.rpc.table.ObKVParamsBase;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.ByteUtil.*;
import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/**
 *
OB_UNIS_DEF_SERIALIZE(ObTableQuery,
     key_ranges_,
     select_columns_,
     filter_string_,
     limit_,
     offset_,
     scan_order_,
     index_name_,
     batch_size_,
     max_result_size_,
     htable_filter_,
     key_range_columns));
 *
 */
public class ObTableQuery extends AbstractPayload {

    protected List<ObNewRange>    keyRanges                 = new LinkedList<ObNewRange>();
    protected List<String>        selectColumns             = new LinkedList<String>();
    protected String              filterString;
    protected int                 limit                     = -1;
    protected int                 offset                    = 0;
    protected ObScanOrder         scanOrder                 = ObScanOrder.Forward;
    protected String              indexName;
    protected int                 batchSize                 = -1;
    protected long                maxResultSize             = -1;
    protected ObHTableFilter      hTableFilter;

    protected static final byte[] HTABLE_DUMMY_BYTES = new byte[] { 0x01, 0x00 };
    protected boolean             isHbaseQuery              = false;
    protected boolean             isFTSQuery                = false;
    protected List<String>        scanRangeColumns          = new LinkedList<String>();

    protected List<ObTableAggregationSingle>    aggregations       = new LinkedList<>();

    private Long partId = null;

    protected ObKVParams obKVParams = null;

    public void adjustStartKey(List<ObObj> key) throws IllegalArgumentException {
        List<ObNewRange> keyRanges = getKeyRanges();
        for (ObNewRange range : keyRanges) {
            if (key != null && isKeyInRange(range, key)) {
                ObRowKey newStartKey;
                if (getScanOrder() == ObScanOrder.Forward) {
                    // get the real rowkey
                    newStartKey = ObRowKey.getInstance(new Object[]{key.get(0).getValue(), ObObj.getMax(), ObObj.getMax()});
                } else {
                    newStartKey = ObRowKey.getInstance(new Object[]{key.get(0).getValue(), ObObj.getMax(), ObObj.getMax()});
                }
                range.setStartKey(newStartKey);
                return;
            }
        }
        /* keyRanges not changed */
    }

    private byte[] parseStartKeyToBytes(List<ObObj> key) {
        if (key != null) {
            ObObj obObjKey = key.get(0);
            return obObjKey.encode();
        }
        return new byte[0];
    }

    private boolean isKeyInRange(ObNewRange range, List<ObObj> key) {
        byte[] startKeyBytes = parseStartKeyToBytes(range.getStartKey().getObjs());
        byte[] endKeyBytes = parseStartKeyToBytes(range.getEndKey().getObjs());
        byte[] keyBytes = parseStartKeyToBytes(key);

        int startComparison = compareByteArrays(startKeyBytes, keyBytes);
        int endComparison = compareByteArrays(endKeyBytes, keyBytes);

        boolean withinStart = startComparison <= 0;
        boolean withinEnd = endComparison > 0;

        return withinStart && withinEnd;
    }


    /*
     * Check filter.
     */
    public boolean isFilterNull() {
         return filterString != null;
    }

    /*
     * Check aggregation.
     */
    public boolean isAggregation() {
        if (aggregations.isEmpty()) {
            return false;
        }
        return true;
    }

    /*
     * Add aggregation.
     */
    public void addAggregation(ObTableAggregationType aggType, String aggColumn) {
        this.aggregations.add(new ObTableAggregationSingle(aggType, aggColumn));
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        byte[] headerBytes = encodeObUniVersionHeader(getVersion(), getPayloadContentSize());
        System.arraycopy(headerBytes, 0, bytes,
            idx, headerBytes.length);
        idx += headerBytes.length;

        // 1. encode
        byte[] keyRangesByte = Serialization.encodeVi64(keyRanges.size());
        System.arraycopy(keyRangesByte, 0, bytes, idx, keyRangesByte.length);
        idx += keyRangesByte.length;
        for (ObNewRange keyRange : keyRanges) {
            byte[] keyRangeBytes = keyRange.encode();
            System.arraycopy(keyRangeBytes, 0, bytes, idx, keyRangeBytes.length);
            idx += keyRangeBytes.length;
        }

        byte[] selectColumnsBytes = Serialization.encodeVi64(selectColumns.size());
        System.arraycopy(selectColumnsBytes, 0, bytes, idx, selectColumnsBytes.length);
        idx += selectColumnsBytes.length;
        for (String selectColumn : selectColumns) {
            byte[] selectColumnBytes = Serialization.encodeVString(selectColumn);
            System.arraycopy(selectColumnBytes, 0, bytes, idx, selectColumnBytes.length);
            idx += selectColumnBytes.length;
        }

        byte[] filterStringBytes = Serialization.encodeVString(filterString);
        System.arraycopy(filterStringBytes, 0, bytes, idx, filterStringBytes.length);
        idx += filterStringBytes.length;

        byte[] limitBytes = Serialization.encodeVi32(limit);
        System.arraycopy(limitBytes, 0, bytes, idx, limitBytes.length);
        idx += limitBytes.length;
        byte[] offsetBytes = Serialization.encodeVi32(offset);
        System.arraycopy(offsetBytes, 0, bytes, idx, offsetBytes.length);
        idx += offsetBytes.length;
        System.arraycopy(Serialization.encodeI8(scanOrder.getByteValue()), 0, bytes, idx, 1);
        idx += 1;

        byte[] indexNameBytes = Serialization.encodeVString(indexName);
        System.arraycopy(indexNameBytes, 0, bytes, idx, indexNameBytes.length);
        idx += indexNameBytes.length;

        byte[] batchSizeBytes = Serialization.encodeVi32(batchSize);
        System.arraycopy(batchSizeBytes, 0, bytes, idx, batchSizeBytes.length);
        idx += batchSizeBytes.length;
        byte[] maxResultSizeBytes = Serialization.encodeVi64(maxResultSize);
        System.arraycopy(maxResultSizeBytes, 0, bytes, idx, maxResultSizeBytes.length);
        idx += maxResultSizeBytes.length;

        if (isHbaseQuery) {
            byte[] hTableFilterBytes = hTableFilter.encode();
            System.arraycopy(hTableFilterBytes, 0, bytes, idx, hTableFilterBytes.length);
            idx += hTableFilterBytes.length;
        } else {
            System.arraycopy(HTABLE_DUMMY_BYTES, 0, bytes, idx, HTABLE_DUMMY_BYTES.length);
            idx += HTABLE_DUMMY_BYTES.length;
        }

        byte[] scanRangeColumnsBytes = Serialization.encodeVi64(scanRangeColumns.size());
        System.arraycopy(scanRangeColumnsBytes, 0, bytes, idx, scanRangeColumnsBytes.length);
        idx += scanRangeColumnsBytes.length;
        for (String keyRangeColumn : scanRangeColumns) {
            byte[] keyRangeColumnBytes = Serialization.encodeVString(keyRangeColumn);
            System.arraycopy(keyRangeColumnBytes, 0, bytes, idx, keyRangeColumnBytes.length);
            idx += keyRangeColumnBytes.length;
        }

        //Aggregation
        byte[] aggregationsSizeBytes = Serialization.encodeVi64(aggregations.size());
        System.arraycopy(aggregationsSizeBytes, 0, bytes, idx, aggregationsSizeBytes.length);
        idx += aggregationsSizeBytes.length;
        for (ObTableAggregationSingle obTableAggregationSingle : aggregations) {
            byte[] obTableAggregationSingleBytes = obTableAggregationSingle.encode();
            System.arraycopy(obTableAggregationSingleBytes, 0, bytes, idx, obTableAggregationSingleBytes.length);
            idx += obTableAggregationSingleBytes.length;
        }

        if (obKVParams != null) { // hbaseQuery or FTSQuery will use obKVParams
            byte[] obKVParamsBytes = obKVParams.encode();
            System.arraycopy(obKVParamsBytes, 0, bytes, idx, obKVParamsBytes.length);
            idx += obKVParamsBytes.length;
        } else {
            System.arraycopy(HTABLE_DUMMY_BYTES, 0, bytes, idx, HTABLE_DUMMY_BYTES.length);
            idx += HTABLE_DUMMY_BYTES.length;
        }

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode header
        super.decode(buf);

        // 1. decode
        long size = Serialization.decodeVi64(buf);
        for (int i = 0; i < size; i++) {
            ObNewRange obNewRange = new ObNewRange();
            obNewRange.decode(buf);
            this.keyRanges.add(obNewRange);
        }
        size = Serialization.decodeVi64(buf);
        for (int i = 0; i < size; i++) {
            this.selectColumns.add(Serialization.decodeVString(buf));
        }

        this.filterString = Serialization.decodeVString(buf);
        this.limit = Serialization.decodeVi32(buf);
        this.offset = Serialization.decodeVi32(buf);
        this.scanOrder = ObScanOrder.valueOf(Serialization.decodeI8(buf));
        this.indexName = Serialization.decodeVString(buf);

        this.batchSize = Serialization.decodeVi32(buf);
        this.maxResultSize = Serialization.decodeVi64(buf);

        buf.markReaderIndex();
        buf.readByte();
        if (Serialization.decodeVi64(buf) > 0) {
            buf.resetReaderIndex();

            this.isHbaseQuery = true;
            this.hTableFilter = new ObHTableFilter();
            this.hTableFilter.decode(buf);
        } else {
            buf.resetReaderIndex();

            buf.readByte();
            buf.readByte();
        }
        size = Serialization.decodeVi64(buf);
        for (int i = 0; i < size; i++) {
            this.scanRangeColumns.add(Serialization.decodeVString(buf));
        }

        size = Serialization.decodeVi64(buf);
        for (int i = 0; i < size; i++) {
            byte agg_type = Serialization.decodeI8(buf);
            String agg_column = Serialization.decodeVString(buf);
            this.aggregations.add(new ObTableAggregationSingle(ObTableAggregationType.fromByte(agg_type), agg_column));
        }

        buf.markReaderIndex();
        if (buf.readByte() > 0) {
            // read pType if is exists
            buf.resetReaderIndex();
            obKVParams = new ObKVParams();
            this.obKVParams.decode(buf);
        }
        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        if (this.payLoadContentSize == -1) {
            long contentSize = 0;
            contentSize += Serialization.getNeedBytes(keyRanges.size());
            for (ObNewRange obNewRange : keyRanges) {
                contentSize += obNewRange.getEncodedSize();
            }
            contentSize += Serialization.getNeedBytes(selectColumns.size());
            for (String selectColumn : selectColumns) {
                contentSize += Serialization.getNeedBytes(selectColumn);
            }
            contentSize += Serialization.getNeedBytes(filterString);
            contentSize += Serialization.getNeedBytes(limit);
            contentSize += Serialization.getNeedBytes(offset);
            contentSize += 1; // scanOrder
            contentSize += Serialization.getNeedBytes(indexName);

            contentSize += Serialization.getNeedBytes(batchSize);
            contentSize += Serialization.getNeedBytes(maxResultSize);

            if (isHbaseQuery) {
                contentSize += hTableFilter.getPayloadSize();
            } else {
                contentSize += HTABLE_DUMMY_BYTES.length;
            }
            if (obKVParams != null) {
                contentSize += obKVParams.getPayloadSize();
            } else {
                contentSize += HTABLE_DUMMY_BYTES.length;
            }
            contentSize += Serialization.getNeedBytes(scanRangeColumns.size());
            for (String scanRangeColumn : scanRangeColumns) {
                contentSize += Serialization.getNeedBytes(scanRangeColumn);
            }

            contentSize += Serialization.getNeedBytes(aggregations.size());
            for (ObTableAggregationSingle obTableAggregationSingle : aggregations) {
                contentSize += obTableAggregationSingle.getPayloadSize();
            }
            this.payLoadContentSize = contentSize;
        }
        return this.payLoadContentSize;
    }

    /*
     * Get key ranges.
     */
    public List<ObNewRange> getKeyRanges() {
        return keyRanges;
    }

    /*
     * Set key ranges.
     */
    public void setKeyRanges(List<ObNewRange> keyRanges) {
        this.keyRanges = keyRanges;
    }

    /*
     * Add key range.
     */
    public void addKeyRange(ObNewRange keyRange) {
        this.keyRanges.add(keyRange);
    }

    /*
     * Get select columns.
     */
    public List<String> getSelectColumns() {
        return selectColumns;
    }

    /*
     * Set select columns.
     */
    public void setSelectColumns(List<String> selectColumns) {
        this.selectColumns = selectColumns;
    }

    /*
     * Add select column.
     */
    public void addSelectColumn(String selectColumn) {
        this.selectColumns.add(selectColumn);
    }

    /*
     * Get filter string.
     */
    public String getFilterString() {
        return filterString;
    }

    /*
     * Set filter string.
     */
    public void setFilterString(String filterString) {
        this.filterString = filterString;
    }

    /*
     * Get limit.
     */
    public int getLimit() {
        return limit;
    }

    /*
     * Set limit.
     */
    public void setLimit(int limit) {
        this.limit = limit;
    }

    /*
     * Get offset.
     */
    public int getOffset() {
        return offset;
    }

    /*
     * Set offset.
     */
    public void setOffset(int offset) {
        this.offset = offset;
    }

    /*
     * Get scan order.
     */
    public ObScanOrder getScanOrder() {
        return scanOrder;
    }

    /*
     * Set scan order.
     */
    public void setScanOrder(ObScanOrder scanOrder) {
        this.scanOrder = scanOrder;
    }

    /*
     * Get index name.
     */
    public String getIndexName() {
        return indexName;
    }

    /*
     * Set index name.
     */
    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    /*
     * Get batch size.
     */
    public int getBatchSize() {
        return batchSize;
    }

    /*
     * Set batch size.
     */
    public void setBatchSize(int batchSize) {
        if (batchSize > 0) {
            this.batchSize = batchSize;
        }
    }

    /*
     * Get max result size.
     */
    public long getMaxResultSize() {
        return maxResultSize;
    }

    /*
     * Set max result size.
     */
    public void setMaxResultSize(long maxResultSize) {
        if (maxResultSize > 0) {
            this.maxResultSize = maxResultSize;
        }
    }

    /*
     * Geth table filter.
     */
    public ObHTableFilter gethTableFilter() {
        return hTableFilter;
    }

    /*
     * Seth table filter.
     */
    public void sethTableFilter(ObHTableFilter hTableFilter) {
        this.isHbaseQuery = true;
        this.hTableFilter = hTableFilter;
    }

    /*
     * Is hbase query.
     */
    public boolean isHbaseQuery() {
        return isHbaseQuery;
    }

    /*
     * Set hbase query.
     */
    public void setHbaseQuery(boolean hbaseQuery) {
        isHbaseQuery = hbaseQuery;
    }

    /*
     * Get select columns.
     */
    public List<String> getScanRangeColumns() {
        return scanRangeColumns;
    }

    /*
     * Set select columns.
     */
    public void setScanRangeColumns(String... scanRangeColumns) {
        this.scanRangeColumns.clear();
        for (String scanRangeCol : scanRangeColumns) {
            this.scanRangeColumns.add(scanRangeCol);
        }
    }

    public void setScanRangeColumns(List<String> scanRangeColumns) {
        this.scanRangeColumns = scanRangeColumns;
    }

    public void setPartId(Long partId) {
        this.partId = partId;
    }

    public Long getPartId() { return this.partId; }

    // This interface is just for OBKV-Hbase
    public void setObKVParams(ObKVParams obKVParams) {
        if (!(obKVParams.getObParamsBase() instanceof ObHBaseParams)) {
            throw new FeatureNotSupportedException("only ObHBaseParams support currently");
        }
        this.isHbaseQuery = true;
        this.obKVParams = obKVParams;
    }

    public void setSearchText(String searchText) {
        if (this.isHbaseQuery) {
            throw new FeatureNotSupportedException("Hbase query not support full text search currently");
        }
        if (this.obKVParams == null) {
            obKVParams = new ObKVParams();
        }
        ObFTSParams ftsParams = (ObFTSParams)obKVParams.getObParams(ObKVParamsBase.paramType.FTS);
        ftsParams.setSearchText(searchText);
        this.obKVParams.setObParamsBase(ftsParams);
        this.isFTSQuery = true;
    }

    public ObKVParams getObKVParams() {
        return obKVParams;
    }

    public boolean isFTSQuery() { return isFTSQuery; }
}

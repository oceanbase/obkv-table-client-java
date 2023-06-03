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

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregationSingle;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregationType;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

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

    private List<ObNewRange>    keyRanges                 = new LinkedList<ObNewRange>();
    private List<String>        selectColumns             = new LinkedList<String>();
    private String              filterString;
    private int                 limit                     = -1;
    private int                 offset                    = 0;
    private ObScanOrder         scanOrder                 = ObScanOrder.Forward;
    private String              indexName;
    private int                 batchSize                 = -1;
    private long                maxResultSize             = -1;
    private ObHTableFilter      hTableFilter;

    private static final byte[] HTABLE_FILTER_DUMMY_BYTES = new byte[] { 0x01, 0x00 };
    private boolean             isHbaseQuery              = false;
    private List<String>        scanRangeColumns          = new LinkedList<String>();
    
    private List<ObTableAggregationSingle>    aggregations       = new LinkedList<>();

    /*
     * Check aggregation
     *
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
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        // 1. encode
        int len = Serialization.getNeedBytes(keyRanges.size());
        System.arraycopy(Serialization.encodeVi64(keyRanges.size()), 0, bytes, idx, len);
        idx += len;
        for (ObNewRange keyRange : keyRanges) {
            len = keyRange.getEncodedSize();
            System.arraycopy(keyRange.encode(), 0, bytes, idx, len);
            idx += len;
        }

        len = Serialization.getNeedBytes(selectColumns.size());
        System.arraycopy(Serialization.encodeVi64(selectColumns.size()), 0, bytes, idx, len);
        idx += len;
        for (String selectColumn : selectColumns) {
            len = Serialization.getNeedBytes(selectColumn);
            System.arraycopy(Serialization.encodeVString(selectColumn), 0, bytes, idx, len);
            idx += len;
        }

        len = Serialization.getNeedBytes(filterString);
        System.arraycopy(Serialization.encodeVString(filterString), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(limit);
        System.arraycopy(Serialization.encodeVi32(limit), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(offset);
        System.arraycopy(Serialization.encodeVi32(offset), 0, bytes, idx, len);
        idx += len;
        System.arraycopy(Serialization.encodeI8(scanOrder.getByteValue()), 0, bytes, idx, 1);
        idx += 1;

        len = Serialization.getNeedBytes(indexName);
        System.arraycopy(Serialization.encodeVString(indexName), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(batchSize);
        System.arraycopy(Serialization.encodeVi32(batchSize), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(maxResultSize);
        System.arraycopy(Serialization.encodeVi64(maxResultSize), 0, bytes, idx, len);
        idx += len;

        if (isHbaseQuery) {
            len = (int) hTableFilter.getPayloadSize();
            System.arraycopy(hTableFilter.encode(), 0, bytes, idx, len);
        } else {
            len = HTABLE_FILTER_DUMMY_BYTES.length;
            System.arraycopy(HTABLE_FILTER_DUMMY_BYTES, 0, bytes, idx, len);
        }
        idx += len;

        len = Serialization.getNeedBytes(scanRangeColumns.size());
        System.arraycopy(Serialization.encodeVi64(scanRangeColumns.size()), 0, bytes, idx, len);
        idx += len;
        for (String keyRangeColumn : scanRangeColumns) {
            len = Serialization.getNeedBytes(keyRangeColumn);
            System.arraycopy(Serialization.encodeVString(keyRangeColumn), 0, bytes, idx, len);
            idx += len;
        }

        //Aggregation
        len = Serialization.getNeedBytes(aggregations.size());
        System.arraycopy(Serialization.encodeVi64(aggregations.size()), 0, bytes, idx, len);
        idx += len;
        for (ObTableAggregationSingle obTableAggregationSingle : aggregations) {
            len = (int) obTableAggregationSingle.getPayloadSize();
            System.arraycopy(obTableAggregationSingle.encode(), 0, bytes, idx, len);
            idx += len;
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

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
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
            contentSize += HTABLE_FILTER_DUMMY_BYTES.length;
        }
        contentSize += Serialization.getNeedBytes(scanRangeColumns.size());
        for (String scanRangeColumn : scanRangeColumns) {
            contentSize += Serialization.getNeedBytes(scanRangeColumn);
        }

        contentSize += Serialization.getNeedBytes(aggregations.size());
        for (ObTableAggregationSingle obTableAggregationSingle : aggregations) {
            contentSize += obTableAggregationSingle.getPayloadSize();
        }
        return contentSize;
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
}

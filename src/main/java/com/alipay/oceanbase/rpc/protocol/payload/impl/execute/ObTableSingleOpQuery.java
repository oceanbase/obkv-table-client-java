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
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class ObTableSingleOpQuery extends AbstractPayload {
    private String indexName;
    private List<String> scanRangeColumns = new ArrayList<>();

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
        len = Serialization.getNeedBytes(scanRangeColumns.size());
        System.arraycopy(Serialization.encodeVi64(scanRangeColumns.size()), 0, bytes, idx, len);
        idx += len;
        for (String column : scanRangeColumns) {
            len =  Serialization.getNeedBytes(column);
            System.arraycopy(column, 0, bytes, idx, len);
            idx += len;
        }

        // 3. encode scan ranges
        len = Serialization.getNeedBytes(scanRanges.size());
        System.arraycopy(Serialization.encodeVi64(scanRanges.size()), 0, bytes, idx, len);
        idx += len;
        for (ObNewRange range : scanRanges) {
            len =  range.getEncodedSize();
            System.arraycopy(range.encode(), 0, bytes, idx, len);
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
        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            String column = Serialization.decodeVString(buf);
            scanRangeColumns.add(column);
        }

        // 3. decode scan ranges
        len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            ObNewRange range = new ObNewRange();
            range.decode(buf);
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
        payloadContentSize += Serialization.getNeedBytes(scanRangeColumns.size());
        for (String column : scanRangeColumns) {
            payloadContentSize += Serialization.getNeedBytes(column);
        }

        payloadContentSize += Serialization.getNeedBytes(scanRanges.size());
        for (ObNewRange range : scanRanges) {
            payloadContentSize += range.getEncodedSize();
        }

        return payloadContentSize + Serialization.getNeedBytes(indexName)
                + Serialization.getNeedBytes(filterString);
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

    public String getFilterString() {
        return filterString;
    }

    public void setFilterString(String filterString) {
        this.filterString = filterString;
    }

}

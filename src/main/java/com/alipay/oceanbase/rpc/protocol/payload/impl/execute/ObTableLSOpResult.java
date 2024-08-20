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
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableLSOpResult extends AbstractPayload {

    private List<ObTableTabletOpResult> results = new ArrayList<ObTableTabletOpResult>();
    private List<String> propertiesColumnNames = new ArrayList<>();

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_LS_EXECUTE;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode columnNames
        int len = Serialization.getNeedBytes(this.propertiesColumnNames.size());
        System.arraycopy(Serialization.encodeVi64(this.propertiesColumnNames.size()), 0, bytes, idx, len);
        for (String columnName : propertiesColumnNames) {
            len =  Serialization.getNeedBytes(columnName);
            System.arraycopy(Serialization.encodeVString(columnName), 0, bytes, idx, len);
            idx += len;
        }

        // 2. encode results
        len = Serialization.getNeedBytes(this.results.size());
        System.arraycopy(Serialization.encodeVi64(this.results.size()), 0, bytes, idx, len);
        idx += len;

        for (ObTableTabletOpResult result : results) {
            len = (int) result.getPayloadSize();
            System.arraycopy(result.encode(), 0, bytes, idx, len);
            idx += len;
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

        // 1. decode column names
        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            String column = Serialization.decodeVString(buf);
            propertiesColumnNames.add(column);
        }

        // 2. decode results
        len = (int) Serialization.decodeVi64(buf);
        results = new ArrayList<ObTableTabletOpResult>(len);
        for (int i = 0; i < len; i++) {
            ObTableTabletOpResult tabletOpResult = new ObTableTabletOpResult();
            tabletOpResult.setPropertiesColumnNames(this.propertiesColumnNames);
            tabletOpResult.decode(buf);
            results.add(tabletOpResult);
        }

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long payloadContentSize = 0;
        payloadContentSize += Serialization.getNeedBytes(results.size());
        for (ObTableTabletOpResult result : results) {
            payloadContentSize += result.getPayloadSize();
        }

        return payloadContentSize;
    }

    /*
     * Get results.
     */
    public List<ObTableTabletOpResult> getResults() {
        return results;
    }

    /*
     * Set results.
     */
    public void setResults(List<ObTableTabletOpResult> results) {
        this.results = results;
    }

    /*
     * Add result.
     */
    public void addResult(ObTableTabletOpResult result) {
        this.results.add(result);
    }

    /*
     * Add all results.
     */
    public void addAllResults(List<ObTableTabletOpResult> results) {
        this.results.addAll(results);
    }

}

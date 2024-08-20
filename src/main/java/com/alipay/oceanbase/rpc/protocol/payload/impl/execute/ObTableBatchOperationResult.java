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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableBatchOperationResult extends AbstractPayload {

    private List<ObTableOperationResult> results = new ArrayList<ObTableOperationResult>();

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_BATCH_EXECUTE;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        // ver + plen + payload
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        // 1. encode results
        int len = Serialization.getNeedBytes(this.results.size());
        System.arraycopy(Serialization.encodeVi64(this.results.size()), 0, bytes, idx, len);
        idx += len;

        for (ObTableOperationResult result : results) {
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

        // 1. decode results
        int len = (int) Serialization.decodeVi64(buf);
        results = new ArrayList<ObTableOperationResult>(len);
        for (int i = 0; i < len; i++) {
            ObTableOperationResult obTableOperationResult = new ObTableOperationResult();
            obTableOperationResult.decode(buf);
            results.add(obTableOperationResult);
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
        for (ObTableOperationResult result : results) {
            payloadContentSize += result.getPayloadSize();
        }

        return payloadContentSize;
    }

    /*
     * Get results.
     */
    public List<ObTableOperationResult> getResults() {
        return results;
    }

    /*
     * Set results.
     */
    public void setResults(List<ObTableOperationResult> results) {
        this.results = results;
    }

    /*
     * Add result.
     */
    public void addResult(ObTableOperationResult result) {
        this.results.add(result);
    }

    /*
     * Add all results.
     */
    public void addAllResults(List<ObTableOperationResult> results) {
        this.results.addAll(results);
    }
}

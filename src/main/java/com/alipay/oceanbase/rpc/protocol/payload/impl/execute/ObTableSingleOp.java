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
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutate;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableSingleOp extends AbstractPayload {
    private ObTableQueryAndMutate queryAndMutate = new ObTableQueryAndMutate();
    private ObTableSingleOpType   singleOpType;

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode op type
        byte opTypeVal = singleOpType.getValue();
        System.arraycopy(Serialization.encodeI8(opTypeVal), 0, bytes, idx, 1);
        idx += 1;

        // 1. encode query and mutate/op
        int len = (int) queryAndMutate.getPayloadSize();
        System.arraycopy(queryAndMutate.encode(), 0, bytes, idx, len);
        idx += len;

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        this.queryAndMutate = new ObTableQueryAndMutate();
        this.queryAndMutate.decode(buf);

        byte opTypeVal = Serialization.decodeI8(buf);

        this.singleOpType.setValue(opTypeVal);
        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {

        long opTypeLen = Serialization.getNeedBytes(singleOpType.getValue());
        return queryAndMutate.getPayloadSize() + opTypeLen;

    }

    /*
     * Get table query.
     */
    public ObTableQuery getTableQuery() {
        return queryAndMutate.getTableQuery();
    }

    /*
     * Set table query.
     */
    public void setTableQuery(ObTableQuery tableQuery) {
        this.queryAndMutate.setTableQuery(tableQuery);
    }

    /*
     * Get mutations.
     */
    public ObTableBatchOperation getMutations() {
        return queryAndMutate.getMutations();
    }

    /*
     * Set mutations.
     */
    public void setMutations(ObTableBatchOperation mutations) {
        this.queryAndMutate.setMutations(mutations);
    }

    public void setIsCheckAndExecute(boolean isCheckAndExecute) {
        queryAndMutate.setIsCheckAndExecute(isCheckAndExecute);
    }

    public void setIsCheckNoExists(boolean isCheckNoExists) {
        queryAndMutate.setIsCheckNoExists(isCheckNoExists);
    }

    public ObTableSingleOpType getSingleOpType() {
        return singleOpType;
    }

    public void setSingleOpType(ObTableSingleOpType singleOpType) {
        this.singleOpType = singleOpType;
    }

}

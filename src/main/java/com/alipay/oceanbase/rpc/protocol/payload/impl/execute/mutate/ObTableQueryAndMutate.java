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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/**
 *
 OB_SERIALIZE_MEMBER(ObTableQueryAndMutate,
     query_,
     mutations_);
 *
 */
public class ObTableQueryAndMutate extends AbstractPayload {

    private ObTableQuery          tableQuery;
    private ObTableBatchOperation mutations;
    private boolean               returnAffectedEntity = true;

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
        // ver + plen + payload
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        // 1. encode payload
        int len = (int) tableQuery.getPayloadSize();
        System.arraycopy(tableQuery.encode(), 0, bytes, idx, len);
        idx += len;

        len = (int) mutations.getPayloadSize();
        System.arraycopy(mutations.encode(), 0, bytes, idx, len);
        idx += len;
        System.arraycopy(Serialization.encodeI8(returnAffectedEntity ? (byte) 1 : (byte) 0), 0,
            bytes, idx, 1);

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        this.tableQuery = new ObTableQuery();
        this.tableQuery.decode(buf);

        this.mutations = new ObTableBatchOperation();
        this.mutations.decode(buf);

        this.returnAffectedEntity = Serialization.decodeI8(buf) == 1;
        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return tableQuery.getPayloadSize() //
               + mutations.getPayloadSize() //
               + 1;// returnAffectedEntity
    }

    /*
     * Get table query.
     */
    public ObTableQuery getTableQuery() {
        return tableQuery;
    }

    /*
     * Set table query.
     */
    public void setTableQuery(ObTableQuery tableQuery) {
        this.tableQuery = tableQuery;
    }

    /*
     * Get mutations.
     */
    public ObTableBatchOperation getMutations() {
        return mutations;
    }

    /*
     * Set mutations.
     */
    public void setMutations(ObTableBatchOperation mutations) {
        this.mutations = mutations;
    }
}

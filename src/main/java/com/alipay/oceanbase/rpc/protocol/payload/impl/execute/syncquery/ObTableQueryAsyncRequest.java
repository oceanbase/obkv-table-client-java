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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery;

import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableAbstractOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/**
 OB_SERIALIZE_MEMBER((ObTableQueryAsyncRequest,
     credential_,
     table_name_,
     table_id_,
     partition_id_,
     entity_type_,
     consistency_level_,
     query_,
     query_session_id_,
     query_type_);
 */
public class ObTableQueryAsyncRequest extends ObTableAbstractOperationRequest {
    private ObTableQueryRequest  obTableQueryRequest;
    private long                 querySessionId;
    private ObQueryOperationType queryType           = ObQueryOperationType.QUERY_START;

    private boolean              allowDistributeScan = true;

    /**
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE_QUERY_SYNC;
    }

    @Override
    public byte[] encode() {
        obTableQueryRequest.setCredential(credential);

        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;
        idx = encodeHeader(bytes, idx);

        int len = (int) obTableQueryRequest.getPayloadSize();
        System.arraycopy(obTableQueryRequest.encode(), 0, bytes, idx, len);
        idx += len;

        idx = encodeQuerySessionId(bytes, idx);

        encodeQueryType(bytes, idx);
        return bytes;
    }

    protected int encodeQuerySessionId(byte[] bytes, int idx) {
        int len = Serialization.getNeedBytes(querySessionId);
        System.arraycopy(Serialization.encodeVi64(querySessionId), 0, bytes, idx, len);
        idx += len;
        return idx;
    }

    protected int encodeQueryType(byte[] bytes, int idx) {
        System.arraycopy(Serialization.encodeI8(queryType.getByteValue()), 0, bytes, idx, 1);
        idx += 1;
        return idx;
    }

    public void setQuerySessionId(long querySessionId) {
        this.querySessionId = querySessionId;
    }

    public ObQueryOperationType getQueryType() {
        return queryType;
    }

    public void setQueryType(ObQueryOperationType queryType) {
        this.queryType = queryType;
    }

    /**
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        this.obTableQueryRequest = new ObTableQueryRequest();
        this.obTableQueryRequest.decode(buf);

        this.querySessionId = Serialization.decodeVi64(buf);
        this.queryType = ObQueryOperationType.valueOf(buf.readByte());

        return this;
    }

    /**
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(querySessionId) + 1
               + obTableQueryRequest.getPayloadSize();
    }

    public ObTableQueryRequest getObTableQueryRequest() {
        return obTableQueryRequest;
    }

    public void setObTableQueryRequest(ObTableQueryRequest obTableQueryRequest) {
        this.obTableQueryRequest = obTableQueryRequest;
    }

    public void setAllowDistributeScan(boolean allowDistributeScan) {
        this.allowDistributeScan = allowDistributeScan;
    }

    public boolean isAllowDistributeScan() {
        return allowDistributeScan;
    }

}

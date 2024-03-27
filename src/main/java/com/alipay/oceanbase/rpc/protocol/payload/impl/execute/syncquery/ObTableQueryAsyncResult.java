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

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/**
 OB_SERIALIZE_MEMBER((ObTableQueryAsyncResult, ObTableQueryResult),
     is_end_,
     query_session_id_);
 */
public class ObTableQueryAsyncResult extends AbstractPayload {
    private long               sessionId;
    private boolean            isEnd;
    private ObTableQueryResult affectedEntity = new ObTableQueryResult();

    /**
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE_QUERY_SYNC;
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        int len = (int) Serialization.getObUniVersionHeaderLength(getVersion(), getPayloadSize());
        byte[] header = Serialization.encodeObUniVersionHeader(getVersion(), getPayloadSize());
        System.arraycopy(header, 0, bytes, idx, len);
        idx += len;

        // 1. encode affectedEntity
        len = (int) affectedEntity.getPayloadSize();
        System.arraycopy(affectedEntity.encode(), 0, bytes, idx, len);

        // 2. encode isEnd
        System.arraycopy(Serialization.encodeI8(isEnd ? (byte) 1 : (byte) 0), 0, bytes, idx, 1);
        idx++;

        // 3. encode sessionId
        len = Serialization.getNeedBytes(sessionId);
        System.arraycopy(Serialization.encodeVi64(sessionId), 0, bytes, idx, len);
        idx += len;

        return bytes;
    }

    public Object decode(ByteBuf buf) {
        super.decode(buf);
        this.affectedEntity.decode(buf);
        this.isEnd = Serialization.decodeI8(buf) != 0;
        this.sessionId = Serialization.decodeVi64(buf);
        return this;
    }

    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(sessionId) + 3 + affectedEntity.getPayloadContentSize();
    }

    public long getSessionId() {
        return sessionId;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

    public ObTableQueryResult getAffectedEntity() {
        return affectedEntity;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public void setAffectedEntity(ObTableQueryResult affectedEntity) {
        this.affectedEntity = affectedEntity;
    }

}

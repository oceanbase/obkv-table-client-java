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
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

public class ObTableQueryAndMutateResult extends AbstractPayload {

    private long               affectedRows   = 0;
    private ObTableQueryResult affectedEntity = new ObTableQueryResult();

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_QUERY_AND_MUTATE;
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

        // 1. encode affectedRows
        len = Serialization.getNeedBytes(affectedRows);
        System.arraycopy(Serialization.encodeVi64(affectedRows), 0, bytes, idx, len);
        idx += len;
        // 2. encode affectedEntity
        len = (int) affectedEntity.getPayloadSize();
        System.arraycopy(affectedEntity.encode(), 0, bytes, idx, len);

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode version
        super.decode(buf);

        // 1. decode affected rows
        this.affectedRows = Serialization.decodeVi64(buf);
        affectedEntity.decode(buf);
        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(affectedRows) + affectedEntity.getPayloadSize();
    }

    /*
     * Get affected rows.
     */
    public long getAffectedRows() {
        return affectedRows;
    }

    /*
     * Set affected rows.
     */
    public void setAffectedRows(long affectedRows) {
        this.affectedRows = affectedRows;
    }

    /*
     * Get affected entity.
     */
    public ObTableQueryResult getAffectedEntity() {
        return affectedEntity;
    }

    /*
     * Set affected entity.
     */
    public void setAffectedEntity(ObTableQueryResult affectedEntity) {
        this.affectedEntity = affectedEntity;
    }
}

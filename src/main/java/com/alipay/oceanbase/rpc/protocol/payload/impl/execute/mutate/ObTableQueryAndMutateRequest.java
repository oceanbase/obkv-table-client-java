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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableAbstractOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/*
 *
 OB_SERIALIZE_MEMBER(ObTableQueryAndMutateRequest,
     credential_,
     table_name_,
     table_id_,
     partition_id_,
     entity_type_,
     query_and_mutate_);
 *
 */
public class ObTableQueryAndMutateRequest extends ObTableAbstractOperationRequest {

    private ObTableQueryAndMutate tableQueryAndMutate;

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

        // 0. encode ObTableQueryAndMutateRequest header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        // 1. encode ObTableQueryAndMutateRequest payload
        idx = encodeCredential(bytes, idx);
        idx = encodeTableMetaWithPartitionId(bytes, idx);

        int len = (int) tableQueryAndMutate.getPayloadSize();
        System.arraycopy(tableQueryAndMutate.encode(), 0, bytes, idx, len);

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        this.credential = Serialization.decodeBytesString(buf);
        this.tableName = Serialization.decodeVString(buf);
        this.tableId = Serialization.decodeVi64(buf);
        if (ObGlobal.obVsnMajor() >= 4)
            this.partitionId = Serialization.decodeI64(buf);
        else
            this.partitionId = Serialization.decodeVi64(buf);
        this.entityType = ObTableEntityType.valueOf(buf.readByte());

        this.tableQueryAndMutate = new ObTableQueryAndMutate();
        this.tableQueryAndMutate.decode(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        if (ObGlobal.obVsnMajor() >= 4)
            return Serialization.getNeedBytes(credential) + Serialization.getNeedBytes(tableName)
                   + Serialization.getNeedBytes(tableId) + 8 + 1
                   + tableQueryAndMutate.getPayloadSize();
        else
            return Serialization.getNeedBytes(credential) + Serialization.getNeedBytes(tableName)
                   + Serialization.getNeedBytes(tableId) + Serialization.getNeedBytes(partitionId)
                   + 1 + tableQueryAndMutate.getPayloadSize();

    }

    /*
     * Get table query and mutate.
     */
    public ObTableQueryAndMutate getTableQueryAndMutate() {
        return tableQueryAndMutate;
    }

    /*
     * Set table query and mutate.
     */
    public void setTableQueryAndMutate(ObTableQueryAndMutate tableQueryAndMutate) {
        this.tableQueryAndMutate = tableQueryAndMutate;
    }

    /*
     * Is returning affected entity.
     */
    @Override
    public boolean isReturningAffectedEntity() {
        return tableQueryAndMutate.isReturnAffectedEntity();
    }

    /*
     * Set returning affected entity.
     */
    @Override
    public void setReturningAffectedEntity(boolean returningAffectedEntity) {
        tableQueryAndMutate.setReturnAffectedEntity(returningAffectedEntity);
    }
}

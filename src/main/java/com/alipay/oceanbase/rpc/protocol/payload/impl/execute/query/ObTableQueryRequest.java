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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableAbstractOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableConsistencyLevel;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/**
 *
 OB_SERIALIZE_MEMBER(ObTableQueryRequest,
     credential_,
     table_name_,
     table_id_,
     partition_id_,
     entity_type_,
     consistency_level_,
     query_
     );
 *
 */
public class ObTableQueryRequest extends ObTableAbstractOperationRequest {

    private ObTableQuery tableQuery;

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

        // 0. encode ObTableQueryRequest header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        // 1. encode ObTableQueryRequest payload
        idx = encodeCredential(bytes, idx);

        // 2. encode ObTableQueryRequest meta
        idx = encodeTableMetaWithPartitionId(bytes, idx);

        idx = encodeConsistencyLevel(bytes, idx);

        int len = (int) tableQuery.getPayloadSize();
        System.arraycopy(tableQuery.encode(), 0, bytes, idx, len);

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
        if (ObGlobal.OB_VERSION >= 4)
            this.partitionId = Serialization.decodeI64(buf);
        else
            this.partitionId = Serialization.decodeVi64(buf);
        this.entityType = ObTableEntityType.valueOf(buf.readByte());
        this.consistencyLevel = ObTableConsistencyLevel.valueOf(buf.readByte());

        this.tableQuery = new ObTableQuery();
        this.tableQuery.decode(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        if (ObGlobal.OB_VERSION >= 4)
            return Serialization.getNeedBytes(credential) + Serialization.getNeedBytes(tableName)
                    + Serialization.getNeedBytes(tableId) + 8 + 2
                    + tableQuery.getPayloadSize();
        else
            return Serialization.getNeedBytes(credential) + Serialization.getNeedBytes(tableName)
                   + Serialization.getNeedBytes(tableId) + Serialization.getNeedBytes(partitionId) + 2
                   + tableQuery.getPayloadSize();
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

}

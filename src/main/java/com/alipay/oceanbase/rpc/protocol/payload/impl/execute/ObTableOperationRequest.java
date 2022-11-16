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

import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/**
 *
OB_SERIALIZE_MEMBER(ObTableOperationRequest,
        credential_,
        table_name_,
        table_id_,
        partition_id_,
        entity_type_,
        table_operation_,
        consistency_level_,
        returning_rowkey_,
        returning_affected_entity_,
        returning_affected_rows_
        );
 *
 */
public class ObTableOperationRequest extends ObTableAbstractOperationRequest {

    private ObTableOperation tableOperation;

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode ObTableOperationRequest header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        // 1. encode ObTableOperationRequest payload
        idx = encodeCredential(bytes, idx);
        idx = encodeTableMetaWithPartitionId(bytes, idx);

        int len = (int) tableOperation.getPayloadSize();
        System.arraycopy(tableOperation.encode(), 0, bytes, idx, len);
        idx += len;
        System.arraycopy(Serialization.encodeI8(consistencyLevel.getByteValue()), 0, bytes, idx, 1);
        idx++;
        System.arraycopy(Serialization.encodeI8(returningRowKey ? (byte) 1 : (byte) 0), 0, bytes,
            idx, 1);
        idx++;
        System.arraycopy(Serialization.encodeI8(returningAffectedEntity ? (byte) 1 : (byte) 0), 0,
            bytes, idx, 1);
        idx++;
        System.arraycopy(Serialization.encodeI8(returningAffectedRows ? (byte) 1 : (byte) 0), 0,
            bytes, idx, 1);

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
        this.partitionId = Serialization.decodeVi64(buf);
        this.entityType = ObTableEntityType.valueOf(buf.readByte());
        this.tableOperation = new ObTableOperation();
        if (ObTableEntityType.DYNAMIC.equals(this.entityType)) {
            this.tableOperation.setEntity(new ObTableEntity());
        } else {
            throw new IllegalArgumentException(); // TODO
        }
        this.tableOperation.decode(buf);

        this.consistencyLevel = ObTableConsistencyLevel.valueOf(buf.readByte());
        this.returningRowKey = Serialization.decodeI8(buf) != 0;
        this.returningAffectedEntity = Serialization.decodeI8(buf) != 0;
        this.returningAffectedRows = Serialization.decodeI8(buf) != 0;

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return super.getPayloadContentSize() + tableOperation.getPayloadSize();
    }

    /*
     * Get table operation.
     */
    public ObTableOperation getTableOperation() {
        return tableOperation;
    }

    /*
     * Set table operation.
     */
    public void setTableOperation(ObTableOperation tableOperation) {
        this.tableOperation = tableOperation;
    }

    /*
     * Get instance.
     */
    public static ObTableOperationRequest getInstance(String tableName, //
                                                      ObTableOperationType type,//
                                                      Object[] rowKeys, //
                                                      String[] columns, //
                                                      Object[] properties,//
                                                      long timeout) {

        ObTableOperationRequest request = new ObTableOperationRequest();
        request.setTableName(tableName);
        request.setTimeout(timeout);
        request.setReturningAffectedRows(true); // TODO 可以设置参数，如果不用的话提高性能
        ObTableOperation obTableOperation = ObTableOperation.getInstance(type, rowKeys, columns,
            properties);
        request.setTableOperation(obTableOperation);

        return request;
    }

}

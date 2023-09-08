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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/*
 *
OB_SERIALIZE_MEMBER(ObTableBatchOperationRequest,
        credential_,
        table_name_,
        table_id_,
        entity_type_,
        batch_operation_,
        consistency_level_,
        returning_rowkey_,
        returning_affected_entity_,
        returning_affected_rows_,
        partition_id_,
        batch_operation_as_atomic_
        );
 *
 */
public class ObTableBatchOperationRequest extends ObTableAbstractOperationRequest {

    private ObTableBatchOperation batchOperation;
    private boolean               batchOperationAsAtomic = false;

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

        // 0. encode  ObTableBatchOperationRequest header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        // 1. encode ObTableBatchOperationRequest payload
        idx = encodeCredential(bytes, idx);

        // 2. encode ObTableBatchOperationRequest meta
        idx = encodeTableMetaWithoutPartitionId(bytes, idx);

        // 3. encode batchOperation
        int len = (int) batchOperation.getPayloadSize();
        System.arraycopy(batchOperation.encode(), 0, bytes, idx, len);
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
        idx++;
        if (ObGlobal.OB_VERSION.majorVersion >= 4) {
            System.arraycopy(Serialization.encodeI64(partitionId), 0, bytes, idx, 8);
            idx += 8;
        } else {
            len = Serialization.getNeedBytes(partitionId);
            System.arraycopy(Serialization.encodeVi64(partitionId), 0, bytes, idx, len);
            idx += len;
        }

        System.arraycopy(Serialization.encodeI8(batchOperationAsAtomic ? (byte) 1 : (byte) 0), 0,
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
        this.entityType = ObTableEntityType.valueOf(buf.readByte());
        this.batchOperation = new ObTableBatchOperation();
        this.batchOperation.decode(buf);

        this.consistencyLevel = ObTableConsistencyLevel.valueOf(buf.readByte());
        this.returningRowKey = Serialization.decodeI8(buf) != 0;
        this.returningAffectedEntity = Serialization.decodeI8(buf) != 0;
        this.returningAffectedRows = Serialization.decodeI8(buf) != 0;
        if (ObGlobal.OB_VERSION.majorVersion >= 4)
            this.partitionId = Serialization.decodeI64(buf);
        else
            this.partitionId = Serialization.decodeVi64(buf);
        this.batchOperationAsAtomic = Serialization.decodeI8(buf) != 0;

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return super.getPayloadContentSize() + batchOperation.getPayloadSize() + 1;
    }

    /*
     * Get batch operation.
     */
    public ObTableBatchOperation getBatchOperation() {
        return batchOperation;
    }

    /*
     * Set batch operation.
     */
    public void setBatchOperation(ObTableBatchOperation batchOperation) {
        this.batchOperation = batchOperation;
    }

    /*
     * Whether BatchOperation As Atomic.
     */
    public boolean isBatchOperationAsAtomic() {
        return batchOperationAsAtomic;
    }

    /*
     * Set BatchOperation As Atomic.
     */
    public void setBatchOperationAsAtomic(boolean batchOperationAsAtomic) {
        this.batchOperationAsAtomic = batchOperationAsAtomic;
    }
}

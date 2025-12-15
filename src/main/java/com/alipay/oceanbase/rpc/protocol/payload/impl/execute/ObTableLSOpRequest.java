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
import com.alipay.oceanbase.rpc.protocol.payload.Credentialable;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/*
OB_SERIALIZE_MEMBER(ObTableLSOpRequest,
                    credential_,
                    entity_type_,
                    consistency_level_,
                    ls_op_,
                    hbase_op_type_);
 */
public class ObTableLSOpRequest extends AbstractPayload implements Credentialable {
    protected ObBytesString           credential;
    protected ObTableEntityType       entityType       = ObTableEntityType.KV;
    protected ObReadConsistency       consistencyLevel = ObReadConsistency.STRONG;
    private ObTableLSOperation        lsOperation      = null;
    protected OHOperationType         hbaseOpType      = OHOperationType.INVALID;

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_LS_EXECUTE;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        ObByteBuf buf = new ObByteBuf((int) getPayloadSize());

        // 0. encode ObTableLSOpRequest header
        encodeHeader(buf);

        // 1. encode credential
        Serialization.encodeBytesString(buf, credential);

        // 2. encode entity_type
        Serialization.encodeI8(buf, entityType.getByteValue());

        // 3. encode consistencyLevel
        Serialization.encodeI8(buf, consistencyLevel.getByteValue());

        // 4. encode lsOperation
        lsOperation.encode(buf);

        // 5. encode hbase op type, for table operations, this will be INVALID(0)
        Serialization.encodeI8(buf, hbaseOpType.getByteValue());
        if (buf.pos != buf.bytes.length) {
            throw new IllegalArgumentException("error in encode lsOperationRequest (" +
                    "pos:" + buf.pos + ", buf.capacity:" + buf.bytes.length + ")");
        }
        return buf.bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);
        this.credential = Serialization.decodeBytesString(buf);
        this.entityType = ObTableEntityType.valueOf(buf.readByte());
        this.consistencyLevel = ObReadConsistency.valueOf(buf.readByte());
        this.lsOperation = new ObTableLSOperation();
        this.lsOperation.decode(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        if (payLoadContentSize == INVALID_PAYLOAD_CONTENT_SIZE) {
            payLoadContentSize = lsOperation.getPayloadSize() + Serialization.getNeedBytes(credential) + 1 // entityType
                    + 1 /* consistencyLevel */ + 1 /* hbaseOpType */;
        }
        return payLoadContentSize;
    }

    /*
     * Get batch operation.
     */
    public ObTableLSOperation getLSOperation() {
        return lsOperation;
    }

    /*
     * Set batch operation.
     */
    public void addTabletOperation(ObTableTabletOp tabletOp) {
        lsOperation.addTabletOperation(tabletOp);
    }

    public void setLsOperation(ObTableLSOperation lsOperation) {
        this.lsOperation = lsOperation;
    }

    /*
     * Get entity type.
     */
    public ObTableEntityType getEntityType() {
        return entityType;
    }

    /*
     * Set entity type.
     */
    public void setEntityType(ObTableEntityType entityType) {
        this.entityType = entityType;
    }

    /*
     * Set timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /*
     * Set consistency level.
     */
    public void setConsistencyLevel(ObReadConsistency consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    /*
     * Set credential.
     */
    public void setCredential(ObBytesString credential) {
        this.credential = credential;
    }

    public void setTableId(long tableId) {
        this.lsOperation.setTableId(tableId);
    }

    public void setHbaseOpType(OHOperationType hbaseOpType) {
        this.hbaseOpType = hbaseOpType;
    }

    /**
     * Reset the cached payload content size and propagate to child objects
     */
    @Override
    public void resetPayloadContentSize() {
        super.resetPayloadContentSize();
        if (lsOperation != null) {
            lsOperation.resetPayloadContentSize();
        }
    }
}

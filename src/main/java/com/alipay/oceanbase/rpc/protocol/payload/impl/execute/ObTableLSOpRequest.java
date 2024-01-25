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
                    ls_op_);
 */
public class ObTableLSOpRequest extends AbstractPayload implements Credentialable {
    protected ObBytesString           credential;
    protected ObTableEntityType       entityType       = ObTableEntityType.DYNAMIC;
    protected ObTableConsistencyLevel consistencyLevel = ObTableConsistencyLevel.STRONG;
    private ObTableLSOperation        lsOperation      = new ObTableLSOperation();

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
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode  ObTableLSOpRequest header
        idx = encodeHeader(bytes, idx);

        // 1. encode credential
        byte[] strbytes = Serialization.encodeBytesString(credential);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        // 2. encode entity_type
        System.arraycopy(Serialization.encodeI8(entityType.getByteValue()), 0, bytes, idx, 1);
        idx++;

        // 3. encode consistencyLevel level
        System.arraycopy(Serialization.encodeI8(consistencyLevel.getByteValue()), 0, bytes, idx, 1);
        idx++;

        // 4. encode lsOperation
        int len = (int) lsOperation.getPayloadSize();
        System.arraycopy(lsOperation.encode(), 0, bytes, idx, len);
        idx += len;

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);
        this.credential = Serialization.decodeBytesString(buf);
        this.entityType = ObTableEntityType.valueOf(buf.readByte());
        this.consistencyLevel = ObTableConsistencyLevel.valueOf(buf.readByte());
        this.lsOperation = new ObTableLSOperation();
        this.lsOperation.decode(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return lsOperation.getPayloadSize() + Serialization.getNeedBytes(credential) + 1 + 1 + 1;
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
    public void setConsistencyLevel(ObTableConsistencyLevel consistencyLevel) {
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
}


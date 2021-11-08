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

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/**
 *
 OB_SERIALIZE_MEMBER(ObTableOperation, operation_type_, const_cast<ObITableEntity&>(*entity_));
 *
 */
public class ObTableBatchOperation extends AbstractPayload {

    private List<ObTableOperation> tableOperations = new ArrayList<ObTableOperation>();
    private boolean                isReadOnly      = true;
    private boolean                isSameType;
    private boolean                isSamePropertiesNames;

    /**
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        // 1. encode Operation
        int len = Serialization.getNeedBytes(tableOperations.size());
        System.arraycopy(Serialization.encodeVi64(tableOperations.size()), 0, bytes, idx, len);
        idx += len;
        for (ObTableOperation tableOperation : tableOperations) {
            len = (int) tableOperation.getPayloadSize();
            System.arraycopy(tableOperation.encode(), 0, bytes, idx, len);
            idx += len;
        }

        // 2. encode others
        System
            .arraycopy(Serialization.encodeI8(isReadOnly ? (byte) 1 : (byte) 0), 0, bytes, idx, 1);
        idx++;
        System
            .arraycopy(Serialization.encodeI8(isSameType ? (byte) 1 : (byte) 0), 0, bytes, idx, 1);
        idx++;
        System.arraycopy(Serialization.encodeI8(isSamePropertiesNames ? (byte) 1 : (byte) 0), 0,
            bytes, idx, 1);

        return bytes;
    }

    /**
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode header
        super.decode(buf);

        // 1. decode Operation
        int len = (int) Serialization.decodeVi64(buf);
        tableOperations = new ArrayList<ObTableOperation>(len);
        for (int i = 0; i < len; i++) {
            ObTableOperation obTableOperation = new ObTableOperation();
            obTableOperation.decode(buf);
            tableOperations.add(obTableOperation);
        }

        // 2. decode others
        this.isReadOnly = Serialization.decodeI8(buf) == 1;
        this.isSameType = Serialization.decodeI8(buf) == 1;
        this.isSamePropertiesNames = Serialization.decodeI8(buf) == 1;

        return this;
    }

    /**
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long payloadContentSize = 0;
        payloadContentSize += Serialization.getNeedBytes(tableOperations.size());
        for (ObTableOperation operation : tableOperations) {
            payloadContentSize += operation.getPayloadSize();
        }

        return payloadContentSize + 3;
    }

    /**
     * Get table operations.
     */
    public List<ObTableOperation> getTableOperations() {
        return tableOperations;
    }

    /**
     * Add table operation.
     */
    public void addTableOperation(ObTableOperation tableOperation) {
        this.tableOperations.add(tableOperation);
        if (isReadOnly && !tableOperation.isReadonly()) {
            isReadOnly = false;
        }
    }

    /**
     * Set table operations.
     */
    public void setTableOperations(List<ObTableOperation> tableOperations) {
        this.tableOperations = tableOperations;
        this.isReadOnly = true;
        for (ObTableOperation o : tableOperations) {
            if (!o.isReadonly()) {
                this.isReadOnly = false;
                return;
            }
        }
    }

    /**
     * Is read only.
     */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
     * Set read only.
     */
    public void setReadOnly(boolean readOnly) {
        isReadOnly = readOnly;
    }

    /**
     * Is same type.
     */
    public boolean isSameType() {
        return isSameType;
    }

    /**
     * Set same type.
     */
    public void setSameType(boolean sameType) {
        isSameType = sameType;
    }

    /**
     * Is same properties names.
     */
    public boolean isSamePropertiesNames() {
        return isSamePropertiesNames;
    }

    /**
     * Set same properties names.
     */
    public void setSamePropertiesNames(boolean samePropertiesNames) {
        isSamePropertiesNames = samePropertiesNames;
    }
}

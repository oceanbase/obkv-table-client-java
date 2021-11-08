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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/**
 *
 OB_SERIALIZE_MEMBER(ObTableOperation, operation_type_, const_cast<ObITableEntity&>(*entity_));
 *
 */
public class ObTableOperation extends AbstractPayload {

    private ObTableOperationType operationType;
    private ObITableEntity       entity;       // TODO 我是如何知道类型的？

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
        System.arraycopy(Serialization.encodeI8(operationType.getByteValue()), 0, bytes, idx, 1);
        idx += 1;

        // 2. encode entity
        long len = entity.getPayloadSize();
        System.arraycopy(entity.encode(), 0, bytes, idx, (int) len);

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
        operationType = ObTableOperationType.valueOf(Serialization.decodeI8(buf.readByte()));

        // 2. decode Entity
        this.entity = new ObTableEntity();
        this.entity.decode(buf);

        return this;
    }

    /**
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return 1 + entity.getPayloadSize();
    }

    /**
     * Get operation type.
     */
    public ObTableOperationType getOperationType() {
        return operationType;
    }

    /**
     * Set operation type.
     */
    public void setOperationType(ObTableOperationType operationType) {
        this.operationType = operationType;
    }

    /**
     * Get entity.
     */
    public ObITableEntity getEntity() {
        return entity;
    }

    /**
     * Set entity.
     */
    public void setEntity(ObITableEntity entity) {
        this.entity = entity;
    }

    /**
     * Get instance.
     */
    public static ObTableOperation getInstance(ObTableOperationType type, Object[] rowKeys,
                                               String[] columns, Object[] properties) {

        ObTableOperation obTableOperation = new ObTableOperation();

        obTableOperation.setOperationType(type);
        ObITableEntity entity = new ObTableEntity();
        obTableOperation.setEntity(entity);
        for (int i = 0; i < rowKeys.length; i++) {
            Object rowkey = rowKeys[i];
            ObObjMeta rowkeyMeta = ObObjType.defaultObjMeta(rowkey);

            ObObj obj = new ObObj();
            obj.setMeta(rowkeyMeta);
            obj.setValue(rowkey);
            entity.addRowKeyValue(obj);
        }

        if (columns != null) {
            for (int i = 0; i < columns.length; i++) {
                String name = columns[i];
                Object value = null;
                if (properties != null) {
                    value = properties[i];
                }
                ObObjMeta meta = ObObjType.defaultObjMeta(value);

                ObObj c = new ObObj();
                c.setMeta(meta);
                c.setValue(value);
                entity.setProperty(name, c);
            }
        }

        return obTableOperation;
    }

    /**
     * Is readonly.
     */
    public boolean isReadonly() {
        return this.operationType.isReadonly();
    }
}

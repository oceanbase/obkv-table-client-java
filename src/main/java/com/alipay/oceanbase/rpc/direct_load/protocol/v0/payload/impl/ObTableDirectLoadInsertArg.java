/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

package com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload.impl;

import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;

// OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadInsertArg,
//                            table_id_,
//                            task_id_,
//                            payload_);

public class ObTableDirectLoadInsertArg implements ObSimplePayload {

    private final static ObBytesString emptyPayload       = new ObBytesString();
    private final static ObByteBuf     emptyPayloadBuffer = new ObByteBuf(0);

    private long                       tableId            = 0;
    private long                       taskId             = 0;
    private ObBytesString              payload            = emptyPayload;
    private ObByteBuf                  payloadBuffer      = emptyPayloadBuffer;

    public ObTableDirectLoadInsertArg() {
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public ObBytesString getPayload() {
        return payload;
    }

    public void setPayload(ObBytesString payload) {
        if (payload == null) {
            throw new NullPointerException();
        }
        this.payload = payload;
    }

    public String toString() {
        return String.format("{tableId:%d, taskId:%d, payload:%d, payloadBuffer:%d}", tableId,
            taskId, payload.length(), payloadBuffer.readableBytes());
    }

    public ObByteBuf getPayloadBuffer() {
        return payloadBuffer;
    }

    public void setPayloadBuffer(ObByteBuf payloadBuffer) {
        this.payloadBuffer = payloadBuffer;
    }

    /**
     * Encode.
     */
    @Override
    public byte[] encode() {
        int needBytes = (int) getEncodedSize();
        ObByteBuf buf = new ObByteBuf(needBytes);
        encode(buf);
        return buf.bytes;
    }

    /**
     * Encode.
     */
    @Override
    public void encode(ObByteBuf buf) {
        Serialization.encodeVi64(buf, tableId);
        Serialization.encodeVi64(buf, taskId);
        if (payloadBuffer != emptyPayloadBuffer) {
            buf.writeBytes(payloadBuffer.bytes, 0, payloadBuffer.readableBytes());
        } else {
            Serialization.encodeBytesString(buf, payload);
        }
    }

    /**
     * Decode.
     */
    @Override
    public ObTableDirectLoadInsertArg decode(ByteBuf buf) {
        tableId = Serialization.decodeVi64(buf);
        taskId = Serialization.decodeVi64(buf);
        payload = Serialization.decodeBytesString(buf);
        return this;
    }

    /**
     * Get encoded size.
     */
    @Override
    public int getEncodedSize() {
        return Serialization.getNeedBytes(tableId)
               + Serialization.getNeedBytes(taskId)
               + (payloadBuffer != emptyPayloadBuffer ? payloadBuffer.readableBytes()
                   : Serialization.getNeedBytes(payload));
    }

}

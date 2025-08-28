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

package com.alipay.oceanbase.rpc.table;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObKVParams extends AbstractPayload {

    public ObKVParamsBase obKVParamsBase;

    public ObKVParamsBase getObParams(ObKVParamsBase.paramType pType) {
        switch (pType) {
            case HBase:
                return new ObHBaseParams();
            case Redis:
                throw new RuntimeException("Currently does not support redis type");
            case FTS:
                return new ObFTSParams();
            default:
                throw new RuntimeException("Currently does not support other types except HBase");
        }
    }

    public void setObParamsBase(ObKVParamsBase obKVParamsBase) {
        this.obKVParamsBase = obKVParamsBase;
    }

    public ObKVParamsBase getObParamsBase() {
        return obKVParamsBase;
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        int len = (int) obKVParamsBase.getPayloadContentSize();
        System.arraycopy(obKVParamsBase.encode(), 0, bytes, idx, len);

        return bytes;
    }

    public void encode(ObByteBuf buf) {
        // 0. encode header
        encodeObUniVersionHeader(buf, getVersion(), getPayloadContentSize());
        int posStart = buf.pos;

        obKVParamsBase.encode(buf);
        int writeBufferLength = buf.pos - posStart;
        if (writeBufferLength != this.payLoadContentSize) {
            throw new IllegalArgumentException("error in encode ObKvParams (" +
                    "writeBufferLength:" + writeBufferLength + ", payLoadContentSize:" + this.payLoadContentSize + ")");
        }
    }

    public Object decode(ByteBuf buf) {
        super.decode(buf);
        byte b = buf.readByte();
        ObKVParamsBase.paramType pType = ObKVParamsBase.paramType.values()[b];
        obKVParamsBase = getObParams(pType);
        obKVParamsBase.decode(buf);
        return this;
    }

    @Override
    public long getPayloadContentSize() {
        if (this.payLoadContentSize == INVALID_PAYLOAD_CONTENT_SIZE) {
            this.payLoadContentSize = obKVParamsBase.getPayloadContentSize();
        }
        return this.payLoadContentSize;
    }

    @Override
    public String toString() {
        return "ObKVParams{" +
                "obKVParamsBase=" + obKVParamsBase +
                '}';
    }

}

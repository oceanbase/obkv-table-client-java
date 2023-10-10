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

package com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Credentialable;
import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObAddr;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;

// OB_SERIALIZE_MEMBER(ObTableDirectLoadRequest,
//                     header_,
//                     credential_,
//                     arg_content_);

public class ObTableDirectLoadRequest extends AbstractPayload implements Credentialable {

    private Header        header     = new Header();
    private ObBytesString credential;
    private ObBytesString argContent = new ObBytesString();

    public ObTableDirectLoadRequest() {
        setVersion(2);
    }

    public Header getHeader() {
        return header;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public ObBytesString getCredential() {
        return credential;
    }

    @Override
    public void setCredential(ObBytesString credential) {
        this.credential = credential;
    }

    public ObBytesString getArgContent() {
        return argContent;
    }

    public void setArgContent(ObBytesString argContent) {
        if (argContent == null) {
            throw new NullPointerException();
        }
        this.argContent = argContent;
    }

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_DIRECT_LOAD;
    }

    /**
     * Encode.
     */
    @Override
    public byte[] encode() {
        long payloadContentSize = getPayloadContentSize();
        int needBytes = (int) (Serialization.getObUniVersionHeaderLength(getVersion(),
            payloadContentSize) + payloadContentSize);
        ObByteBuf buf = new ObByteBuf(needBytes);
        Serialization.encodeObUniVersionHeader(buf, getVersion(), payloadContentSize);
        header.encode(buf);
        Serialization.encodeBytesString(buf, credential);
        Serialization.encodeBytesString(buf, argContent);
        return buf.bytes;
    }

    /**
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        header.decode(buf);
        credential = Serialization.decodeBytesString(buf);
        argContent = Serialization.decodeBytesString(buf);
        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return header.getEncodedSize() + Serialization.getNeedBytes(credential)
               + Serialization.getNeedBytes(argContent);
    }

    public static class Header implements ObSimplePayload {
        private ObAddr                         addr          = new ObAddr();
        private ObTableDirectLoadOperationType operationType = ObTableDirectLoadOperationType.MAX_TYPE;

        public Header() {
        }

        public ObAddr getAddr() {
            return addr;
        }

        public void setAddr(ObAddr addr) {
            this.addr = addr;
        }

        public ObTableDirectLoadOperationType getOperationType() {
            return operationType;
        }

        public void setOperationType(ObTableDirectLoadOperationType operationType) {
            this.operationType = operationType;
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
            addr.encode(buf);
            Serialization.encodeI8(buf, operationType.getByteValue());
        }

        /**
         * Decode.
         */
        @Override
        public Header decode(ByteBuf buf) {
            addr.decode(buf);
            operationType = ObTableDirectLoadOperationType.valueOf(Serialization.decodeI8(buf));
            return this;
        }

        /**
         * Get encoded size.
         */
        @Override
        public int getEncodedSize() {
            return addr.getEncodedSize() + 1;
        }

    }

}

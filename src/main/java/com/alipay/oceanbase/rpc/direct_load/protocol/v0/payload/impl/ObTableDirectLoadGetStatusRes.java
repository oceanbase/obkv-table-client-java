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

import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;

// OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadGetStatusRes,
//                            status_,
//                            error_code_);

public class ObTableDirectLoadGetStatusRes implements ObSimplePayload {

    private ObTableLoadClientStatus status    = ObTableLoadClientStatus.MAX_STATUS;
    private int                     errorCode = ResultCodes.OB_SUCCESS.errorCode;

    public ObTableDirectLoadGetStatusRes() {
    }

    public ObTableLoadClientStatus getStatus() {
        return status;
    }

    public void setStatus(ObTableLoadClientStatus status) {
        this.status = status;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(int errorCode) {
        this.errorCode = errorCode;
    }

    public String toString() {
        return String.format("{status:%s, errorCode:%d}", status, errorCode);
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
        Serialization.encodeI8(buf, status.getByteValue());
        Serialization.encodeVi32(buf, errorCode);
    }

    /**
     * Decode.
     */
    @Override
    public ObTableDirectLoadGetStatusRes decode(ByteBuf buf) {
        status = ObTableLoadClientStatus.valueOf(Serialization.decodeI8(buf));
        errorCode = Serialization.decodeVi32(buf);
        return this;
    }

    /**
     * Get encoded size.
     */
    @Override
    public int getEncodedSize() {
        return 1 /*status*/+ Serialization.getNeedBytes(errorCode);
    }

}

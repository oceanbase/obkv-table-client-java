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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableSerialUtil;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

public class ObHbaseCell extends AbstractPayload {
    // if there are more elements in the future
    // add new constructor to create new array
    // cannot List because its size cannot be initialized and is dynamic
    private ObObj[] propertiesValue = null; // Q T V (TTL)

    public ObHbaseCell(boolean isCellTTL) {
        propertiesValue = isCellTTL ? new ObObj[4] : new ObObj[3];
    }

    public void encode(ObByteBuf buf) {
        // 0. encode header
        encodeHeader(buf);

        // 1. encode properties value
        Serialization.encodeVi64(buf, propertiesValue.length);
        for (ObObj obj : propertiesValue) {
            ObTableSerialUtil.encode(buf, obj);
        }
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode properties value
        int len = Serialization.getNeedBytes(propertiesValue.length);
        System.arraycopy(Serialization.encodeVi64(propertiesValue.length), 0, bytes, idx, len);
        idx += len;
        for (ObObj obj : propertiesValue) {
            len = ObTableSerialUtil.getEncodedSize(obj);
            System.arraycopy(ObTableSerialUtil.encode(obj), 0, bytes, idx, len);
            idx += len;
        }
        return bytes;
    }

    @Override
    public long getPayloadContentSize() {
        if (this.payLoadContentSize == INVALID_PAYLOAD_CONTENT_SIZE) {
            long payloadContentSize = 0;
            // add properties value array size and value
            payloadContentSize += Serialization.getNeedBytes(propertiesValue.length);
            for (ObObj obj : propertiesValue) {
                payloadContentSize += ObTableSerialUtil.getEncodedSize(obj);
            }
            this.payLoadContentSize = payloadContentSize;
        }
        return this.payLoadContentSize;
    }

    public ObObj getQ() {
        return propertiesValue[0];
    }

    public ObObj getT() {
        return propertiesValue[1];
    }

    public ObObj getV() {
        return propertiesValue[2];
    }

    public ObObj getTTL() {
        if (propertiesValue.length == 3) {
            throw new IllegalArgumentException("table schema has no cell TTL");
        }
        return propertiesValue[3];
    }

    public void setQ(ObObj Q) {
        propertiesValue[0] = Q;
    }

    public void setT(ObObj T) {
        propertiesValue[1] = T;
    }

    public void setV(ObObj V) {
        propertiesValue[2] = V;
    }

    public void setTTL(ObObj TTL) {
        if (propertiesValue.length == 3) {
            throw new IllegalArgumentException("table schema has no cell TTL");
        }
        propertiesValue[3] = TTL;
    }
}

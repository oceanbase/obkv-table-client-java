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

package com.alipay.oceanbase.rpc.location.model.partition;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

public class OdpSinglePart extends AbstractPayload {
    private long   partId       = Constants.OB_INVALID_ID;
    private long   tabletId     = Constants.OB_INVALID_ID;
    private long   lsId         = Constants.OB_INVALID_ID;
    private long   subPartNum   = Constants.OB_INVALID_ID;
    private String highBoundVal = "";

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        int len = Serialization.getNeedBytes(partId);
        System.arraycopy(Serialization.encodeVi64(partId), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(tabletId);
        System.arraycopy(Serialization.encodeVi64(tabletId), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(lsId);
        System.arraycopy(Serialization.encodeVi64(lsId), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(subPartNum);
        System.arraycopy(Serialization.encodeVi64(subPartNum), 0, bytes, idx, len);
        idx += len;


        byte[] strbytes = Serialization.encodeVString(highBoundVal);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);

        return bytes;
    }

    @Override
    public OdpSinglePart decode(ByteBuf buf) {
        super.decode(buf);

        partId = Serialization.decodeVi64(buf);
        tabletId = Serialization.decodeVi64(buf);
        lsId = Serialization.decodeVi64(buf);
        subPartNum = Serialization.decodeVi64(buf);
        highBoundVal = Serialization.decodeVString(buf);

        return this;
    }


    public long getPartId() {
        return this.partId;
    }

    public long getTabletId() {
        return this.tabletId;
    }

    public long getLsId() {
        return this.lsId;
    }

    public long getSubPartNum() {
        return this.subPartNum;
    }

    public String getHighBoundVal() {
        return this.highBoundVal;
    }

    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(partId)
                + Serialization.getNeedBytes(tabletId)
                + Serialization.getNeedBytes(lsId)
                + Serialization.getNeedBytes(subPartNum)
                + Serialization.getNeedBytes(highBoundVal);
    }

}

/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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

package com.alipay.oceanbase.rpc.meta;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Credentialable;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableMetaRequest extends AbstractPayload implements Credentialable {
    private ObBytesString      credential;
    private ObTableRpcMetaType metaType;
    private String             data;

    @Override
    public void setCredential(ObBytesString credential) {
        this.credential = credential;
    }

    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_META_INFO_EXECUTE;
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
        int len = Serialization.getNeedBytes(credential);
        System.arraycopy(Serialization.encodeBytesString(credential), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(metaType.getType());
        System.arraycopy(Serialization.encodeI8((short)metaType.getType()), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(data);
        System.arraycopy(Serialization.encodeVString(data), 0, bytes, idx, len);
        return bytes;
    }

    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(credential)
               + Serialization.getNeedBytes(metaType.getType()) + Serialization.getNeedBytes(data);
    }

    public void setMetaType(ObTableRpcMetaType metaType) {
        this.metaType = metaType;
    }
    
    public ObTableRpcMetaType getMetaType() {
        return metaType;
    }

    public void setData(String data) {
        this.data = data;
    }
    
    public String getData() {
        return data;
    }
}

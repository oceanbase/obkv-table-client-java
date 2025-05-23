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

import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

public class ObFTSParams extends ObKVParamsBase {
    String searchText = null;

    public ObFTSParams() {
        pType = paramType.FTS;
    }

    public paramType getType() {
        return pType;
    }

    public void setSearchText(String searchText) {
        this.searchText = searchText;
    }

    public String getSearchText() {
        return this.searchText;
    }

    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadContentSize()];
        int idx = 0;
        byte[] b = new byte[] { (byte) pType.ordinal() };
        System.arraycopy(b, 0, bytes, idx, 1);
        idx += 1;
        int len = Serialization.getNeedBytes(searchText);
        System.arraycopy(Serialization.encodeVString(searchText), 0, bytes, idx, len);
        return bytes;
    }

    public void encode(ObByteBuf buf) {
        buf.writeByte((byte)pType.ordinal());
        Serialization.encodeVString(buf, searchText);
    }

    public Object decode(ByteBuf buf) {
        // pType is read by ObKVParams
        this.searchText = Serialization.decodeVString(buf);
        return this;
    }

    public long getPayloadContentSize() {
        return 1 /* pType*/+ Serialization.getNeedBytes(searchText);
    }

    public String toString() {
        return "ObFtsParams: {\n pType = " + pType + ", \n searchText = " + searchText + "\n}\n";
    }
}

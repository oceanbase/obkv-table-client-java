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
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableResult;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableMetaResponse extends AbstractPayload {
    private ObTableRpcMetaType  metaType;                    // 元信息类型
    private final ObTableResult header = new ObTableResult();
    private String              data;                        // 服务端拿到的分片的元数据, json字符串   

    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_META_INFO_EXECUTE;
    }
    
    @Override
    public byte[] encode() {
        return null;
    }

    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);
        // 1. decode ObTableResult
        header.decode(buf);
        // 2. decode itself
        data = Serialization.decodeVString(buf);

        return this;
    }

    @Override
    public long getPayloadContentSize() {
        return 0;
    }
    
    public String getData() {
        return data;
    }
}

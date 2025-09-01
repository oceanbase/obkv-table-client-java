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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableSerialUtil;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class ObHbaseCellResult extends AbstractPayload {
    private long keyIndex; // mapping to the right HBase operation
    private List<ObObj> propertiesValues = new ArrayList<>(); // HBase Get result

    public ObHbaseCellResult() {
        keyIndex = -1;
    }

    public ObHbaseCellResult(long keyIndex) {
        this.keyIndex = keyIndex;
    }

    public long getKeyIndex() {
        return keyIndex;
    }

    public List<ObObj> getPropertiesValues() {
        return propertiesValues;
    }

    @Override
    public byte[] encode() {
        return new byte[0];
    }

    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode version
        super.decode(buf);

        // 1. decode key index
        keyIndex = Serialization.decodeVi64(buf);

        // 2. decode properties values
        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; ++i) {
            ObObj obj = new ObObj();
            ObTableSerialUtil.decode(buf, obj);
            this.propertiesValues.add(obj);
        }
        return this;
    }

    @Override
    public long getPayloadContentSize() {
        return 0;
    }
}

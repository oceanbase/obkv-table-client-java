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

import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

public abstract class ObKVParamsBase {
    public enum paramType {
        HBase((byte) 0), Redis((byte) 1), FTS((byte) 2);
        private final byte value;
        private static final Map<Integer, paramType> map = new HashMap<Integer, paramType>();

        static {
            for (paramType type : paramType.values()) {
                map.put(type.ordinal(), type);
            }
        }

        public static paramType valueOf(int value) { return map.get(value); }

        paramType(byte value) {
            this.value = value;
        }

        public byte getValue() { return value; }
    }

    public int       byteSize;
    public paramType pType;

    public paramType getType() {
        return pType;
    }

    public byte[] encode() {
        return null;
    }

    public Object decode(ByteBuf buf) {
        return null;
    }

    public long getPayloadContentSize() {
        return 0;
    }
}
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

package com.alipay.oceanbase.rpc.protocol.payload.impl;

import java.util.HashMap;
import java.util.Map;

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;

public class ObAddr /*extends ObUnisVersion*/implements ObSimplePayload {

    private static final long UNIS_VERSION = 1;

    private VER               version      = VER.IPV4;
    private int[]             ip           = new int[4];
    private int               port         = 0;

    public ObAddr() {
    }

    public VER getVersion() {
        return version;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public byte[] encode() {
        int needBytes = (int) getEncodedSize();
        ObByteBuf buf = new ObByteBuf(needBytes);
        encode(buf);
        return buf.bytes;
    }

    @Override
    public void encode(ObByteBuf buf) {
        long payloadContentSize = getPayloadContentSize();
        Serialization.encodeObUniVersionHeader(buf, UNIS_VERSION, payloadContentSize);
        Serialization.encodeI8(buf, version.getByteValue());
        for (int i = 0; i < ip.length; ++i) {
            Serialization.encodeVi32(buf, ip[i]);
        }
        Serialization.encodeVi32(buf, port);
    }

    @Override
    public Object decode(ByteBuf buf) {
        long unis_version = Serialization.decodeVi64(buf);
        if (unis_version != UNIS_VERSION) {
            throw new ObTableException("object version mismatch, version:" + unis_version);
        }
        Serialization.decodeVi64(buf); // get payload length, useless now
        version = VER.valueOf(Serialization.decodeI8(buf));
        for (int i = 0; i < ip.length; ++i) {
            ip[i] = Serialization.decodeVi32(buf);
        }
        port = Serialization.decodeVi32(buf);
        return this;
    }

    public long getPayloadContentSize() {
        long payloadContentSize = 0;
        payloadContentSize += 1;
        for (int i = 0; i < ip.length; ++i) {
            payloadContentSize += Serialization.getNeedBytes(ip[i]);
        }
        payloadContentSize += Serialization.getNeedBytes(port);
        return payloadContentSize;
    }

    @Override
    public int getEncodedSize() {
        long payloadContentSize = getPayloadContentSize();
        return (int) (Serialization.getObUniVersionHeaderLength(UNIS_VERSION, payloadContentSize) + payloadContentSize);
    }

    public enum VER {

        IPV4(4), IPV6(6), UNIX(1);

        private final int                      value;
        private static final Map<Integer, VER> map = new HashMap<Integer, VER>();

        static {
            for (VER v : VER.values()) {
                map.put(v.value, v);
            }
        }

        public static VER valueOf(int value) {
            return map.get(value);
        }

        VER(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }

        public byte getByteValue() {
            return (byte) value;
        }
    }

}

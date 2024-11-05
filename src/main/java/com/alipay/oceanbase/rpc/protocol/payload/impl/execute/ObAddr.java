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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

public class ObAddr extends AbstractPayload {

    private ObAddrVersion obAddrVersion;

    private byte[]        ip;
    private int           port;

    public ObAddr() {
        this.obAddrVersion = ObAddrVersion.ObAddrIPV4;
        this.ip = new byte[16];
        this.port = 0;
    }

    public String ipToString() {
        if (isIPv4()) {
            return getIPv4().getHostAddress();
        }
        return getIPv6().getHostAddress();
    }

    public InetAddress getIPv4() {
        if (isIPv6()) {
            return null;
        }
        try {
            return InetAddress.getByAddress(Arrays.copyOf(ip, 4));
        } catch (UnknownHostException e) {
            // 需要查看对应的错误吗进行处理
        }
        return null;
    }

    public InetAddress getIPv6() {
        if (isIPv6()) {
            return null;
        }
        try {
            return InetAddress.getByAddress(ip);
        } catch (UnknownHostException e) {
            // 需要查看对应的错误吗进行处理
        }
        return null;
    }

    public boolean isIPv4() {
        return obAddrVersion == ObAddrVersion.ObAddrIPV4;
    }

    public boolean isIPv6() {
        return obAddrVersion == ObAddrVersion.ObAddrIPV6;
    }

    public int getPort() {
        return port;
    }

    @Override
    public long getPayloadContentSize() {
        return 0;
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);
        System.arraycopy(Serialization.encodeI8(obAddrVersion.getValue()), 0, bytes, idx,
            Serialization.getNeedBytes(obAddrVersion.getValue()));

        ByteBuffer buffer = ByteBuffer.wrap(ip).order(ByteOrder.BIG_ENDIAN);
        int ip1 = buffer.getInt(0);
        int ip2 = buffer.getInt(4);
        int ip3 = buffer.getInt(8);
        int ip4 = buffer.getInt(12);
        System.arraycopy(Serialization.encodeVi32(ip1), 0, bytes, idx,
            Serialization.getNeedBytes(ip1));
        System.arraycopy(Serialization.encodeVi32(ip2), 0, bytes, idx,
            Serialization.getNeedBytes(ip2));
        System.arraycopy(Serialization.encodeVi32(ip3), 0, bytes, idx,
            Serialization.getNeedBytes(ip3));
        System.arraycopy(Serialization.encodeVi32(ip4), 0, bytes, idx,
            Serialization.getNeedBytes(ip4));

        System.arraycopy(Serialization.encodeVi32(port), 0, bytes, idx,
            Serialization.getNeedBytes(port));
        return bytes;
    }

    @Override
    public ObAddr decode(ByteBuf buf) {
        super.decode(buf);
        this.obAddrVersion = ObAddrVersion.fromValue(Serialization.decodeI8(buf));
        //decode ip addr

        int ip1, ip2, ip3, ip4;
        ip1 = Serialization.decodeVi32(buf);
        ip2 = Serialization.decodeVi32(buf);
        ip3 = Serialization.decodeVi32(buf);
        ip4 = Serialization.decodeVi32(buf);
        ByteBuffer ipBuffer = ByteBuffer.wrap(ip).order(ByteOrder.BIG_ENDIAN);
        ipBuffer.putInt(0, ip1);
        ipBuffer.putInt(4, ip2);
        ipBuffer.putInt(8, ip3);
        ipBuffer.putInt(12, ip4);

        this.port = Serialization.decodeVi32(buf);

        return this;
    }
}

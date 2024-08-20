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

package com.alipay.oceanbase.rpc.bolt.transport;

import com.alipay.oceanbase.rpc.bolt.protocol.ObTableProtocol;
import com.alipay.remoting.ProtocolCode;
import com.alipay.remoting.codec.Codec;
import com.alipay.remoting.codec.ProtocolCodeBasedEncoder;
import com.alipay.remoting.rpc.protocol.RpcProtocolDecoder;
import io.netty.channel.ChannelHandler;

public class ObPacketCodec implements Codec {
    /*
     * New encoder.
     */
    @Override
    public ChannelHandler newEncoder() {
        return new ProtocolCodeBasedEncoder(
            ProtocolCode.fromBytes(ObTableProtocol.MAGIC_HEADER_FLAG));
    }

    /*
     * New decoder.
     */
    @Override
    public ChannelHandler newDecoder() {
        return new RpcProtocolDecoder(ObTableProtocol.MAGIC_HEADER_FLAG.length);
    }
}

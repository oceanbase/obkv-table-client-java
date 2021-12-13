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

package com.alipay.oceanbase.rpc.bolt.protocol;

import com.alipay.oceanbase.rpc.util.Serialization;
import com.alipay.remoting.CommandDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.List;

public class ObTablePacketDecoder implements CommandDecoder {

    /*
     * Decode.
     */
    @Override
    public void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        // the less length between response header and request header
        if (in.readableBytes() > ObTableProtocol.HEADER_LENGTH) {
            in.markReaderIndex();

            /*
             * 4bytes  4bytes  4bytes   4bytes
             * -----------------------------------
             * | flag |  dlen  | chid | reserved |
             * -----------------------------------
             */
            // 1. decode header
            byte b;
            if ((b = in.readByte()) != ObTableProtocol.MAGIC_HEADER_FLAG[0]) {
                throw new RuntimeException("magic header wrong, 0 expect: "
                                           + ObTableProtocol.MAGIC_HEADER_FLAG[0] + ", but: " + b);
            } else if ((b = in.readByte()) != ObTableProtocol.MAGIC_HEADER_FLAG[1]) {
                throw new RuntimeException("magic header wrong, 2 expect: "
                                           + ObTableProtocol.MAGIC_HEADER_FLAG[1] + ", but: " + b);
            } else if ((b = in.readByte()) != ObTableProtocol.MAGIC_HEADER_FLAG[2]) {
                throw new RuntimeException("magic header wrong, 2 expect: "
                                           + ObTableProtocol.MAGIC_HEADER_FLAG[2] + ", but: " + b);
            } else if ((b = in.readByte()) != ObTableProtocol.MAGIC_HEADER_FLAG[3]) {
                throw new RuntimeException("magic header wrong, 3 expect: "
                                           + ObTableProtocol.MAGIC_HEADER_FLAG[3] + ", but: " + b);
            }

            byte[] dlen = new byte[4];
            in.readBytes(dlen);
            int dataLen = Serialization.decodeI32(dlen);

            byte[] chid = new byte[4];
            in.readBytes(chid);
            int channelId = Serialization.decodeI32(chid);

            in.readByte(); // reserved
            in.readByte(); // reserved
            in.readByte(); // reserved
            in.readByte(); // reserved

            // 2. try decode packet
            if (in.readableBytes() >= dataLen) {
                ObTablePacket obCommand = new ObTablePacket();
                obCommand.setId(channelId);
                ByteBuf rpcPacketByteBuf = in.readBytes(dataLen);
                obCommand.setPacketContentBuf(rpcPacketByteBuf);
                out.add(obCommand);
            } else {
                in.resetReaderIndex();
            }
        }
    }

}

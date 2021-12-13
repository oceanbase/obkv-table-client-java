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
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.remoting.CommandEncoder;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

import java.io.Serializable;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

public class ObTablePacketEncoder implements CommandEncoder {

    private static final Logger logger = TableClientLoggerFactory
                                           .getLogger(ObTablePacketEncoder.class);

    /*
     * Encode.
     */
    @Override
    public void encode(ChannelHandlerContext ctx, Serializable msg, ByteBuf out) throws Exception {

        try {
            if (msg instanceof ObTablePacket) {
                /*
                 * 4bytes  4bytes  4bytes   4bytes
                 * -----------------------------------
                 * | flag |  dlen  | chid | reserved |
                 * -----------------------------------
                 */
                ObTablePacket cmd = (ObTablePacket) msg;

                // 1. header
                out.writeBytes(ObTableProtocol.MAGIC_HEADER_FLAG);
                out.writeBytes(Serialization.encodeI32(cmd.getPacketContent().length));
                out.writeBytes(Serialization.encodeI32(cmd.getId()));
                out.writeBytes(ObTableProtocol.RESERVED);

                // 2. payload
                out.writeBytes(cmd.getPacketContent());

            } else {
                String warnMsg = "msg type [" + msg.getClass() + "] is not subclass of ObCommand";
                logger.warn(warnMsg);
            }
        } catch (Exception e) {
            logger.error(LCD.convert("01-00003"), e);
            throw e;
        }

    }

}

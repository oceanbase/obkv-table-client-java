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

import com.alipay.oceanbase.rpc.bolt.transport.ObTablePacketHandler;
import com.alipay.remoting.*;

public class ObTableProtocol implements Protocol {

    public final static byte   API_VERSION       = 1;
    public final static int    HEADER_LENGTH     = 16;

    /*
     * ob_rpc_net_handler.cpp
     * Magic Header 4 bytes
     * <p>
     * const uint8_t ObRpcNetHandler::MAGIC_HEADER_FLAG[4] =
     * { ObRpcNetHandler::API_VERSION, 0xDB, 0xDB, 0xCE };
     */
    public final static byte[] MAGIC_HEADER_FLAG = new byte[] { API_VERSION, (byte) 0xDB,
            (byte) 0xDB, (byte) 0xCE            };
    public final static byte[] RESERVED          = new byte[4];

    private CommandEncoder     commandEncoder;
    private CommandDecoder     commandDecoder;

    private CommandHandler     commandHandler;

    static {
        ProtocolManager.registerProtocol(new ObTableProtocol(), MAGIC_HEADER_FLAG);
    }

    /*
     * Ob table protocol.
     */
    public ObTableProtocol() {
        this.commandEncoder = new ObTablePacketEncoder();
        this.commandDecoder = new ObTablePacketDecoder();

        this.commandHandler = new ObTablePacketHandler();
    }

    /*
     * Get encoder.
     */
    @Override
    public CommandEncoder getEncoder() {
        return commandEncoder;
    }

    /*
     * Get decoder.
     */
    @Override
    public CommandDecoder getDecoder() {
        return commandDecoder;
    }

    /*
     * Get command handler.
     */
    @Override
    public CommandHandler getCommandHandler() {
        return commandHandler;
    }

    /*
     * Get heartbeat trigger.
     */
    @Override
    public HeartbeatTrigger getHeartbeatTrigger() {
        return null;
    }

    /*
     * Get command factory.
     */
    @Override
    public CommandFactory getCommandFactory() {
        return null;
    }

}

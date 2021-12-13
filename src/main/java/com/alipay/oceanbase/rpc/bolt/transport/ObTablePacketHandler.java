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

import com.alipay.oceanbase.rpc.bolt.protocol.ObTablePacket;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.remoting.CommandCode;
import com.alipay.remoting.CommandHandler;
import com.alipay.remoting.RemotingContext;
import com.alipay.remoting.RemotingProcessor;
import com.alipay.remoting.rpc.RpcCommand;
import org.slf4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

public class ObTablePacketHandler implements CommandHandler {

    private static final Logger          logger = TableClientLoggerFactory
                                                    .getLogger(ObTablePacketHandler.class);

    /*
     * Unlike {@link com.alipay.remoting.rpc.protocol.RpcCommandHandler}, ObTable client only decodes ObNetPacket, it only has 16 bytes,
     * and all content will decode it in biz thread by {@link ObTableRemoting}.
     * So this class has no ProcessorManager logic
     */
    private final ObTablePacketProcessor processor;

    /*
     * Ob table packet handler.
     */
    public ObTablePacketHandler() {
        this.processor = new ObTablePacketProcessor();
    }

    /*
     * Handle command.
     */
    @Override
    public void handleCommand(final RemotingContext ctx, final Object msg) {

        try {
            if (msg instanceof List) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Batch message! size={}", ((List<?>) msg).size());
                }
                for (Object m : (List<?>) msg) {
                    processor.process(ctx, (ObTablePacket) m, null);
                }
            } else {
                processor.process(ctx, (ObTablePacket) msg, null);
            }
        } catch (Throwable e) {
            processException(msg, e);
        }

    }

    private void processException(Object msg, Throwable t) {
        if (msg instanceof List) {
            for (Object m : (List<?>) msg) {
                processExceptionForSingleCommand(m, t);
            }
        } else {
            processExceptionForSingleCommand(msg, t);
        }
    }

    private void processExceptionForSingleCommand(Object msg, Throwable t) {
        final int id = ((RpcCommand) msg).getId();
        logger.error(LCD.convert("01-00021"), id, t);
    }

    /*
     * Register processor.
     */
    @Override
    public void registerProcessor(CommandCode cmd, RemotingProcessor<?> processor) {
        throw new IllegalArgumentException("not support yet");
    }

    /*
     * Register default executor.
     */
    @Override
    public void registerDefaultExecutor(ExecutorService executor) {
        throw new IllegalArgumentException("not support yet");
    }

    /*
     * Get default executor.
     */
    @Override
    public ExecutorService getDefaultExecutor() {
        throw new IllegalArgumentException("not support yet");
    }
}

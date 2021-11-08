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
import com.alipay.remoting.Connection;
import com.alipay.remoting.InvokeFuture;
import com.alipay.remoting.RemotingContext;
import com.alipay.remoting.RemotingProcessor;
import com.alipay.remoting.util.RemotingUtil;
import org.slf4j.Logger;

import java.util.concurrent.ExecutorService;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

public class ObTablePacketProcessor implements RemotingProcessor<ObTablePacket> {

    private static final Logger logger = TableClientLoggerFactory
                                           .getLogger(ObTablePacketProcessor.class);

    /**
     * Process.
     */
    @Override
    public void process(RemotingContext ctx, ObTablePacket msg, ExecutorService defaultExecutor) {

        Connection conn = ctx.getChannelContext().channel().attr(Connection.CONNECTION).get();
        InvokeFuture future = conn.removeInvokeFuture(msg.getId());
        ClassLoader oldClassLoader = null;
        try {
            if (future != null) {
                if (future.getAppClassLoader() != null) {
                    oldClassLoader = Thread.currentThread().getContextClassLoader();
                    Thread.currentThread().setContextClassLoader(future.getAppClassLoader());
                }
                future.putResponse(msg);
                future.cancelTimeout();
                try {
                    future.executeInvokeCallback();
                } catch (Exception e) {
                    logger.error(LCD.convert("01-00022"), msg.getId(), e);
                }
            } else {
                logger
                    .warn("Cannot find InvokeFuture, maybe already timeout, id={}, from={} ",
                        msg.getId(),
                        RemotingUtil.parseRemoteAddress(ctx.getChannelContext().channel()));
            }
        } finally {
            if (null != oldClassLoader) {
                Thread.currentThread().setContextClassLoader(oldClassLoader);
            }
        }

    }

    /**
     * Get executor.
     */
    @Override
    public ExecutorService getExecutor() {
        return null;
    }

    /**
     * Set executor.
     */
    @Override
    public void setExecutor(ExecutorService executor) {

    }

}

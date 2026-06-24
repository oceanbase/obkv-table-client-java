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

import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.remoting.Connection;
import com.alipay.remoting.ConnectionEventHandler;
import com.alipay.remoting.ConnectionEventType;
import com.alipay.remoting.config.switches.GlobalSwitch;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

/**
 * Notify pending RPC futures immediately when the underlying channel becomes inactive.
 * The default {@link ConnectionEventHandler} only calls {@link Connection#onClose()} from
 * {@code close()}, which is not always invoked when the peer disconnects abruptly.
 */
public class ObConnectionEventHandler extends ConnectionEventHandler {

    private static final Logger LOGGER = TableClientLoggerFactory
                                             .getLogger(ObConnectionEventHandler.class);

    public ObConnectionEventHandler(GlobalSwitch globalSwitch) {
        super(globalSwitch);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if (evt == ConnectionEventType.CONNECT) {
            Connection connection = ctx.channel().attr(Connection.CONNECTION).get();
            if (connection != null) {
                LOGGER.info("connection [{}] established", formatConnection(connection));
            }
        }
        super.userEventTriggered(ctx, evt);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Connection connection = ctx.channel().attr(Connection.CONNECTION).get();
        if (connection != null) {
            LOGGER.info("connection [{}] closed", formatConnection(connection));
            connection.onClose();
        }
        super.channelInactive(ctx);
    }

    private static String formatConnection(Connection connection) {
        String localIp = connection.getLocalIP();
        String local = (localIp == null || localIp.isEmpty()) ? "unknown"
                       : localIp + ":" + connection.getLocalPort();
        String remote = "unknown";
        String originUrl = connection.getUrl() != null ? connection.getUrl().getOriginUrl() : null;
        if (originUrl != null && !originUrl.isEmpty()) {
            remote = originUrl;
        } else {
            String remoteIp = connection.getRemoteIP();
            if (remoteIp != null && !remoteIp.isEmpty()) {
                remote = remoteIp + ":" + connection.getRemotePort();
            }
        }
        return local + " - " + remote;
    }
}

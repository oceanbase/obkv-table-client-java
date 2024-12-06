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
import com.alipay.remoting.*;
import com.alipay.remoting.codec.Codec;
import com.alipay.remoting.config.ConfigManager;
import com.alipay.remoting.config.ConfigurableInstance;
import com.alipay.remoting.connection.ConnectionFactory;
import com.alipay.remoting.rpc.RpcHandler;
import com.alipay.remoting.rpc.protocol.UserProcessor;
import com.alipay.remoting.util.NettyEventLoopUtil;
import com.alipay.remoting.util.StringUtils;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class ObConnectionFactory implements ConnectionFactory {

    private static final Logger         logger      = LoggerFactory.getLogger(ObConnectionFactory.class);

    private static final EventLoopGroup workerGroup = NettyEventLoopUtil.newEventLoopGroup(Runtime
                                                        .getRuntime().availableProcessors() + 1,
                                                        new NamedThreadFactory(
                                                            "Rpc-netty-client-worker", true));

    private final ConfigurableInstance  confInstance;
    private final Codec                 codec;
    private final ChannelHandler        handler;

    protected Bootstrap                 bootstrap;

    private ObConnectionFactory(Codec codec, ChannelHandler handler,
                                ConfigurableInstance confInstance) {
        if (codec == null) {
            throw new IllegalArgumentException("null codec");
        }
        if (handler == null) {
            throw new IllegalArgumentException("null handler");
        }

        this.confInstance = confInstance;
        this.codec = codec;
        this.handler = handler;
    }

    /*
     * Init.
     */
    @Override
    public void init(final ConnectionEventHandler connectionEventHandler) {
        bootstrap = new Bootstrap();
        bootstrap.group(workerGroup).channel(NettyEventLoopUtil.getClientSocketChannelClass())
            .option(ChannelOption.TCP_NODELAY, ConfigManager.tcp_nodelay())
            .option(ChannelOption.SO_REUSEADDR, ConfigManager.tcp_so_reuseaddr())
            .option(ChannelOption.SO_KEEPALIVE, ConfigManager.tcp_so_keepalive());

        // init netty write buffer water mark
        initWriteBufferWaterMark();

        // init byte buf allocator
        if (ConfigManager.netty_buffer_pooled()) {
            this.bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        } else {
            this.bootstrap.option(ChannelOption.ALLOCATOR, UnpooledByteBufAllocator.DEFAULT);
        }

        bootstrap.handler(new ChannelInitializer<SocketChannel>() {

            @Override
            protected void initChannel(SocketChannel channel) {
                ChannelPipeline pipeline = channel.pipeline();
                pipeline.addLast("decoder", codec.newDecoder());
                pipeline.addLast("encoder", codec.newEncoder());
                pipeline.addLast("connectionEventHandler", connectionEventHandler);
                pipeline.addLast("handler", handler);
            }
        });
    }

    /*
     * Create connection.
     */
    @Override
    public Connection createConnection(Url url) throws Exception {
        throw new IllegalArgumentException("not support yet");
    }

    /*
     * @see com.alipay.remoting.connection.ConnectionFactory#createConnection(java.lang.String, int, int)
     */
    @Override
    public Connection createConnection(String targetIP, int targetPort, int connectTimeout)
                                                                                           throws Exception {
        Channel channel = doCreateConnection(targetIP, targetPort, connectTimeout);
        Connection conn = new Connection(channel,
            ProtocolCode.fromBytes(ObTableProtocol.MAGIC_HEADER_FLAG), ObTableProtocol.API_VERSION,
            new Url(targetIP, targetPort));
        channel.pipeline().fireUserEventTriggered(ConnectionEventType.CONNECT);
        return conn;
    }

    /*
     * Create connection.
     */
    @Override
    public Connection createConnection(String targetIP, int targetPort, byte version,
                                       int connectTimeout) throws Exception {
        throw new IllegalArgumentException("not support yet");
    }

    protected Channel doCreateConnection(String targetIP, int targetPort, int connectTimeout)
                                                                                             throws Exception {
        // prevent unreasonable value, at least 1000
        connectTimeout = Math.max(connectTimeout, 1000);
        String address = targetIP + ":" + targetPort;
        if (logger.isDebugEnabled()) {
            logger.debug("connectTimeout of address [{}] is [{}].", address, connectTimeout);
        }
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, connectTimeout);
        ChannelFuture future = bootstrap.connect(new InetSocketAddress(targetIP, targetPort));

        if (!future.await(connectTimeout + 10)) {
            String errMsg = "Create connection to " + address + " future await timeout!";
            logger.warn(errMsg);
            boolean isCanceled = future.cancel(false);
            logger.warn("Create connection future canceled: {}", isCanceled);
            throw new Exception(errMsg, future.cause());
        }
        if (!future.isDone()) {
            String errMsg = "Create connection to " + address + " timeout!";
            logger.warn(errMsg);
            throw new Exception(errMsg);
        }
        if (future.isCancelled()) {
            String errMsg = "Create connection to " + address + " cancelled by user!";
            logger.warn(errMsg);
            throw new Exception(errMsg);
        }
        if (!future.isSuccess()) {
            String errMsg = "Create connection to " + address + " error!";
            logger.warn(errMsg);
            throw new Exception(errMsg, future.cause());
        }
        return future.channel();
    }

    /*
     * init netty write buffer water mark
     */
    private void initWriteBufferWaterMark() {
        int lowWaterMark = this.confInstance.netty_buffer_low_watermark();
        int highWaterMark = this.confInstance.netty_buffer_high_watermark();
        if (lowWaterMark > highWaterMark) {
            throw new IllegalArgumentException(
                String
                    .format(
                        "[client side] bolt netty high water mark {%s} should not be smaller than low water mark {%s} bytes)",
                        highWaterMark, lowWaterMark));
        } else {
            logger.warn(
                "[client side] bolt netty low water mark is {} bytes, high water mark is {} bytes",
                lowWaterMark, highWaterMark);
        }
        this.bootstrap.option(ChannelOption.WRITE_BUFFER_WATER_MARK, new WriteBufferWaterMark(
            lowWaterMark, highWaterMark));
    }

    /*
     * New builder.
     */
    public static ObConnectionFactoryBuilder newBuilder() {
        return new ObConnectionFactoryBuilder();
    }

    public static class ObConnectionFactoryBuilder {
        private Codec                                       codec                = new ObPacketCodec();
        private ConcurrentHashMap<String, UserProcessor<?>> userProcessors       = new ConcurrentHashMap<String, UserProcessor<?>>(
                                                                                     4);
        private ChannelHandler                              rpcHandler           = new RpcHandler(
                                                                                     userProcessors);
        private ConfigurableInstance                        configurableInstance = new ObConfigurableInstance();

        /*
         * Register user processor.
         */
        public ObConnectionFactoryBuilder registerUserProcessor(UserProcessor<?> processor) {
            if (processor == null || StringUtils.isBlank(processor.interest())) {
                throw new RuntimeException(
                    "User processor or processor interest should not be blank!");
            }
            UserProcessor<?> preProcessor = this.userProcessors.putIfAbsent(processor.interest(),
                processor);
            if (preProcessor != null) {
                String errMsg = "Processor with interest key ["
                                + processor.interest()
                                + "] has already been registered to rpc client, can not register again!";
                throw new RuntimeException(errMsg);
            }
            return this;
        }

        /*
         * Config write buffer water mark.
         */
        public ObConnectionFactoryBuilder configWriteBufferWaterMark(int low, int high) {
            configurableInstance.initWriteBufferWaterMark(low, high);
            return this;
        }

        /*
         * Build.
         */
        public ObConnectionFactory build() {
            return new ObConnectionFactory(codec, rpcHandler, configurableInstance);
        }
    }
}

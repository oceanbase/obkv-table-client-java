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
import com.alipay.oceanbase.rpc.bolt.protocol.ObTablePacketCode;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.protocol.packet.ObCompressType;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Credentialable;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ObRpcResultCode;
import com.alipay.oceanbase.rpc.util.ObPureCrc32C;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.oceanbase.rpc.util.TraceUtil;
import com.alipay.remoting.*;
import com.alipay.remoting.exception.RemotingException;
import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;

import static com.alipay.oceanbase.rpc.protocol.packet.ObCompressType.INVALID_COMPRESSOR;
import static com.alipay.oceanbase.rpc.protocol.packet.ObCompressType.NONE_COMPRESSOR;

public class ObTableRemoting extends BaseRemoting {

    private static final Logger logger = TableClientLoggerFactory.getLogger(ObTableRemoting.class);

    /*
     * Ob table remoting.
     */
    public ObTableRemoting(CommandFactory commandFactory) {
        super(commandFactory);
    }

    /*
     * Invoke sync.
     */
    public ObPayload invokeSync(final ObTableConnection conn, final ObPayload request,
                                final int timeoutMillis) throws RemotingException,
                                                        InterruptedException {

        request.setSequence(conn.getNextSequence());
        request.setUniqueId(conn.getUniqueId());

        if (request instanceof Credentialable) {
            if (conn.getCredential() == null) {
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "credential is null");
                logger.warn(errMessage);
                throw new ObTableUnexpectedException(errMessage);
            }
            ((Credentialable) request).setCredential(conn.getCredential());
        }

        if (request instanceof AbstractPayload) {
            ((AbstractPayload) request).setTenantId(conn.getTenantId());
        }

        ObTablePacket obRequest = this.getCommandFactory().createRequestCommand(request);

        ObTablePacket response = (ObTablePacket) super.invokeSync(conn.getConnection(), obRequest,
            timeoutMillis);

        if (response == null) {
            String errMessage = TraceUtil.formatTraceMessage(conn, request, "get null response");
            logger.warn(errMessage);
            ExceptionUtil.throwObTableTransportException(errMessage,
                TransportCodes.BOLT_RESPONSE_NULL);
            return null;
        } else if (!response.isSuccess()) {
            String errMessage = TraceUtil.formatTraceMessage(conn, request,
                "get an error response: " + response.getMessage());
            logger.warn(errMessage);
            response.releaseByteBuf();
            ExceptionUtil.throwObTableTransportException(errMessage, response.getTransportCode());
            return null;
        }

        try {
            // decode packet header first
            response.decodePacketHeader();
            ObCompressType compressType = response.getHeader().getObCompressType();
            if (compressType != INVALID_COMPRESSOR && compressType != NONE_COMPRESSOR) {
                String errMessage = TraceUtil.formatTraceMessage(
                    conn,
                    request,
                    "Rpc Result is compressed. Java Client is not supported. msg:"
                            + response.getMessage());
                logger.warn(errMessage);
                throw new FeatureNotSupportedException(errMessage);
            }
            ByteBuf buf = response.getPacketContentBuf();
            // If response indicates the request is routed to wrong server, we should refresh the routing meta.
            if (response.getHeader().isRoutingWrong()) {
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "routed to the wrong server: " + response.getMessage());
                logger.warn(errMessage);
                throw new ObTableRoutingWrongException(errMessage);
            }

            // verify checksum
            long expected_checksum = response.getHeader().getChecksum();
            byte[] content = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), content);
            if (ObPureCrc32C.calculate(content) != expected_checksum) {
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "get response with checksum error: " + response.getMessage());
                logger.warn(errMessage);
                ExceptionUtil.throwObTableTransportException(errMessage,
                    TransportCodes.BOLT_CHECKSUM_ERR);
                return null;
            }

            // decode ResultCode for response packet
            ObRpcResultCode resultCode = new ObRpcResultCode();
            resultCode.decode(buf);

            if (resultCode.getRcode() != 0) {
                ExceptionUtil.throwObTableException(conn.getObTable().getIp(), conn.getObTable()
                    .getPort(), response.getHeader().getTraceId1(), response.getHeader()
                    .getTraceId0(), resultCode.getRcode(), resultCode.getErrMsg());
                return null;
            }

            // decode payload itself
            ObPayload payload;
            if (response.getCmdCode() instanceof ObTablePacketCode) {
                payload = ((ObTablePacketCode) response.getCmdCode()).newPayload(response
                    .getHeader());
                payload.setSequence(response.getHeader().getTraceId1());
                payload.setUniqueId(response.getHeader().getTraceId0());
            } else {
                String errMessage = TraceUtil.formatTraceMessage(conn, response,
                    "receive unexpected command code: " + response.getCmdCode().value());
                throw new ObTableUnexpectedException(errMessage);
            }

            payload.decode(buf);
            return payload;
        } finally {
            // Very important to release ByteBuf memory
            response.releaseByteBuf();
        }
    }

    @Override
    protected InvokeFuture createInvokeFuture(RemotingCommand request, InvokeContext invokeContext) {
        return new ObClientFuture(request.getId());
    }

    @Override
    protected InvokeFuture createInvokeFuture(Connection conn, RemotingCommand request,
                                              InvokeContext invokeContext,
                                              InvokeCallback invokeCallback) {
        return new ObClientFuture(request.getId());
    }

}

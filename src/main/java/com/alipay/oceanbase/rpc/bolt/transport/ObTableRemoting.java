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
import com.alipay.oceanbase.rpc.protocol.payload.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.login.ObTableLoginRequest;
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
        if (request instanceof ObTableLoginRequest) {
            // setting sys tenant in rpc header when login
            ((ObTableLoginRequest) request).setTenantId(1);
        } else if (request instanceof AbstractPayload) {
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
            // If response indicates the request is routed to wrong server, we should refresh the routing meta.
            if (!conn.getObTable().isEnableRerouting() && response.getHeader().isRoutingWrong()) {
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "routed to the wrong server: [error code:" + resultCode.getRcode() + "]"
                            + resultCode.getErrMsg());
                logger.debug(errMessage);
                if (needFetchMeta(resultCode.getRcode(), resultCode.getPcode())) {
                    throw new ObTableNeedFetchMetaException(errMessage, resultCode.getRcode());
                } else if (needFetchPartitionLocation(resultCode.getRcode())) {
                    throw new ObTableRoutingWrongException(errMessage, resultCode.getRcode());
                } else {
                    // Encountered an unexpected RoutingWrong error code, 
                    // possibly due to the client error code version being behind the observer's version.  
                    // Attempting a full refresh here
                    // and delegating to the upper-level call to determine whether to throw the exception to the user based on the retry result.
                    logger.warn("get unexpected error code: {}", errMessage);
                    throw new ObTableNeedFetchMetaException(errMessage, resultCode.getRcode());
                }
            }
            if (resultCode.getRcode() != 0
                && response.getHeader().getPcode() != Pcodes.OB_TABLE_API_MOVE) {
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "routed to the wrong server:  [error code:" + resultCode.getRcode() + "]"
                            + resultCode.getErrMsg());
                logger.debug(errMessage);
                if (needFetchMeta(resultCode.getRcode(), resultCode.getPcode())) {
                    throw new ObTableNeedFetchMetaException(errMessage, resultCode.getRcode());
                } else if (needFetchPartitionLocation(resultCode.getRcode())) {
                    throw new ObTableRoutingWrongException(errMessage, resultCode.getRcode());
                } else {
                    logger.warn(errMessage);
                    ExceptionUtil.throwObTableException(conn.getObTable().getIp(), conn
                        .getObTable().getPort(), response.getHeader().getTraceId1(), response
                        .getHeader().getTraceId0(), resultCode.getRcode(), resultCode.getErrMsg());
                }
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
                throw new ObTableUnexpectedException(errMessage, resultCode.getRcode());
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

    // schema changed
    private boolean needFetchMeta(int errorCode, int pcode) {
        return errorCode == ResultCodes.OB_SCHEMA_ERROR.errorCode
               || errorCode == ResultCodes.OB_TABLE_NOT_EXIST.errorCode
               || errorCode == ResultCodes.OB_TABLET_NOT_EXIST.errorCode
               || errorCode == ResultCodes.OB_LS_NOT_EXIST.errorCode
               || errorCode == ResultCodes.OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST.errorCode
               || errorCode == ResultCodes.OB_SNAPSHOT_DISCARDED.errorCode
               || errorCode == ResultCodes.OB_SCHEMA_EAGAIN.errorCode
               || errorCode == ResultCodes.OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH.errorCode
               || errorCode == ResultCodes.OB_GTS_NOT_READY.errorCode
               || errorCode == ResultCodes.OB_ERR_OPERATION_ON_RECYCLE_OBJECT.errorCode
               || (pcode == Pcodes.OB_TABLE_API_LS_EXECUTE && errorCode == ResultCodes.OB_NOT_MASTER.errorCode);
    }

    private boolean needFetchPartitionLocation(int errorCode) {
        return errorCode == ResultCodes.OB_LOCATION_LEADER_NOT_EXIST.errorCode
               || errorCode == ResultCodes.OB_NOT_MASTER.errorCode
               || errorCode == ResultCodes.OB_RS_NOT_MASTER.errorCode
               || errorCode == ResultCodes.OB_RS_SHUTDOWN.errorCode
               || errorCode == ResultCodes.OB_RPC_SEND_ERROR.errorCode
               || errorCode == ResultCodes.OB_RPC_POST_ERROR.errorCode
               || errorCode == ResultCodes.OB_PARTITION_NOT_EXIST.errorCode
               || errorCode == ResultCodes.OB_LOCATION_NOT_EXIST.errorCode
               || errorCode == ResultCodes.OB_PARTITION_IS_STOPPED.errorCode
               || errorCode == ResultCodes.OB_PARTITION_IS_BLOCKED.errorCode
               || errorCode == ResultCodes.OB_SERVER_IS_INIT.errorCode
               || errorCode == ResultCodes.OB_SERVER_IS_STOPPING.errorCode
               || errorCode == ResultCodes.OB_TRANS_RPC_TIMEOUT.errorCode
               || errorCode == ResultCodes.OB_NO_READABLE_REPLICA.errorCode;
    }
}

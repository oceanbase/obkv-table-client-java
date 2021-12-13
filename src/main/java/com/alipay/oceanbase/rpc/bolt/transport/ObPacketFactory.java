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
import com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacket;
import com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableStreamRequest;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObPureCrc32C;
import com.alipay.remoting.CommandFactory;
import com.alipay.remoting.RemotingCommand;
import com.alipay.remoting.ResponseStatus;

import java.net.InetSocketAddress;

public class ObPacketFactory implements CommandFactory {

    /*
     * Create request command.
     */
    @Override
    public ObTablePacket createRequestCommand(Object requestObject) {
        if (requestObject instanceof ObPayload) {
            ObPayload request = (ObPayload) requestObject;

            ObTablePacket obRequest = new ObTablePacket();
            obRequest.setCmdCode(ObTablePacketCode.valueOf((short) request.getPcode()));
            obRequest.setId(request.getChannelId());
            obRequest.setPacketContent(encodePayload(request));
            return obRequest;
        }
        return null;
    }

    private byte[] encodePayload(ObPayload payload) {
        // serialize payload & header
        // 1. encode payload
        byte[] payloadContent = payload.encode();

        /*
         * NetPacket -> RpcPacket(HeaderPacket + Payload)
         */
        // 2. assemble rpc packet header
        ObRpcPacketHeader rpcHeaderPacket = new ObRpcPacketHeader();
        rpcHeaderPacket.setPcode(payload.getPcode());
        // us
        rpcHeaderPacket.setTimeout(payload.getTimeout() * 1000);
        rpcHeaderPacket.setTenantId(payload.getTenantId());
        // trace
        rpcHeaderPacket.setTraceId0(payload.getSequence());
        rpcHeaderPacket.setTraceId1(payload.getUniqueId());

        if (payload instanceof ObTableStreamRequest) {
            rpcHeaderPacket.setSessionId(((ObTableStreamRequest) payload).getSessionId());
            rpcHeaderPacket.setFlag(((ObTableStreamRequest) payload).getFlag());
        }
        rpcHeaderPacket.setPriority(ThreadLocalMap.getProcessPriority());

        // compute checksum
        rpcHeaderPacket.setChecksum(ObPureCrc32C.calculate(payloadContent));

        // 3. assemble and encode rpc packet
        ObRpcPacket rpcPacket = new ObRpcPacket();
        rpcPacket.setRpcPacketHeader(rpcHeaderPacket);
        rpcPacket.setPayloadContent(payloadContent);
        return rpcPacket.encode();
    }

    /*
     * Create timeout response.
     */
    @Override
    public ObTablePacket createTimeoutResponse(InetSocketAddress address) {
        return ObTablePacket.createTransportErrorPacket(TransportCodes.BOLT_TIMEOUT,
            "connection {" + address.toString() + "} timeout", null);
    }

    /*
     * Create send failed response.
     */
    @Override
    public ObTablePacket createSendFailedResponse(InetSocketAddress address, Throwable throwable) {
        return ObTablePacket.createTransportErrorPacket(TransportCodes.BOLT_SEND_FAILED,
            "connection {" + address.toString() + "} send failed", throwable);
    }

    /*
     * TODO tell client this connection has been closed
     */
    // TODO for server processor
    @Override
    public ObTablePacket createExceptionResponse(int id, Throwable t, String errMsg) {
        return null;
    }

    /*
     * Create exception response.
     */
    @Override
    public ObTablePacket createExceptionResponse(int id, ResponseStatus status) {
        return null;
    }

    /*
     * Create exception response.
     */
    @Override
    public ObTablePacket createExceptionResponse(int id, String errMsg) {
        return null;
    }

    /*
     * Create response.
     */
    @Override
    public ObTablePacket createResponse(Object responseObject, RemotingCommand requestCmd) {
        return null;
    }

    /*
     * Create exception response.
     */
    @Override
    public ObTablePacket createExceptionResponse(int id, ResponseStatus status, Throwable t) {
        return null;
    }

    /*
     * Create connection closed response.
     */
    @Override
    public ObTablePacket createConnectionClosedResponse(InetSocketAddress address, String message) {
        return null;
    }
}

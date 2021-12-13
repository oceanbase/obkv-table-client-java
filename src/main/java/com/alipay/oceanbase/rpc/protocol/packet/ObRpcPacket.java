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

package com.alipay.oceanbase.rpc.protocol.packet;

public class ObRpcPacket {

    private ObRpcPacketHeader rpcPacketHeader;
    private byte[]            payloadContent;

    /*
     * Get rpc packet header.
     */
    public ObRpcPacketHeader getRpcPacketHeader() {
        return rpcPacketHeader;
    }

    /*
     * Set rpc packet header.
     */
    public void setRpcPacketHeader(ObRpcPacketHeader rpcPacketHeader) {
        this.rpcPacketHeader = rpcPacketHeader;
    }

    /*
     * Get payload content.
     */
    public byte[] getPayloadContent() {
        return payloadContent;
    }

    /*
     * Set payload content.
     */
    public void setPayloadContent(byte[] payloadContent) {
        this.payloadContent = payloadContent;
    }

    /*
     * useless, only for reference
     */
    @Deprecated
    /*
     * Encode.
     */
    public byte[] encode() {
        byte[] rpcPacketHeaderContent = rpcPacketHeader.encode();

        int idx = 0;
        byte[] bytes = new byte[rpcPacketHeaderContent.length + payloadContent.length];
        // 1. header
        System.arraycopy(rpcPacketHeaderContent, 0, bytes, 0, rpcPacketHeader.getHlen());
        idx += rpcPacketHeaderContent.length;

        // 2. payload
        System.arraycopy(payloadContent, 0, bytes, idx, payloadContent.length);
        return bytes;
    }

}

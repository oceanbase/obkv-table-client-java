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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;

import static com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader.STREAM_FLAG;
import static com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader.STREAM_LAST_FLAG;
import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableStreamRequest extends AbstractPayload {

    private long  sessionId;
    private short flag = 0x7; // let ObServer determine the ob log level.

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE_QUERY;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        // 0. encode header
        // ver + plen + payload
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            0, headerLen);
        return bytes;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return 0;
    }

    /*
     * Set stream next.
     */
    public void setStreamNext() {
        flag &= ~STREAM_LAST_FLAG;
        flag |= STREAM_FLAG;
    }

    /*
     * Set stream last.
     */
    public void setStreamLast() {
        flag |= STREAM_LAST_FLAG;
        flag |= STREAM_FLAG;
    }

    /*
     * Set timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /*
     * Get session id.
     */
    public long getSessionId() {
        return sessionId;
    }

    /*
     * Set session id.
     */
    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    /*
     * Get flag.
     */
    public short getFlag() {
        return flag;
    }
}

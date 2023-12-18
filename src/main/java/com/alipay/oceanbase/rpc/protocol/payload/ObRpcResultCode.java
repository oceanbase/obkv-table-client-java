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

package com.alipay.oceanbase.rpc.protocol.payload;

import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObRpcResultCode extends AbstractPayload {

    private int                         rcode;
    private byte[]                      msg         = new byte[0];
    private List<ObRpcResultWarningMsg> warningMsgs = new ArrayList<ObRpcResultWarningMsg>();

    /*
     * Ob rpc result code.
     */
    public ObRpcResultCode() {
    }

    /*
     * Encode.
     */
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. header: ver + plen
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        // 1. fields
        int len = Serialization.getNeedBytes(rcode);
        System.arraycopy(Serialization.encodeVi32(rcode), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(msg.length);
        System.arraycopy(Serialization.encodeVi32(msg.length), 0, bytes, idx, len);
        idx += len;
        System.arraycopy(msg, 0, bytes, idx, msg.length);
        idx += msg.length;

        // 2. warningMsg: ver + plen + cdata
        len = Serialization.getNeedBytes(warningMsgs.size());
        System.arraycopy(Serialization.encodeVi32(warningMsgs.size()), 0, bytes, idx, len);
        idx += len;
        for (ObRpcResultWarningMsg msg : warningMsgs) {
            System.arraycopy(msg.encode(), 0, bytes, idx, (int) msg.getPayloadSize());
        }

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        this.rcode = Serialization.decodeVi32(buf);

        int len = Serialization.decodeVi32(buf);
        msg = new byte[len];
        buf.readBytes(msg);

        len = Serialization.decodeVi32(buf);
        if (len > 0) {
            ObRpcResultWarningMsg msg = new ObRpcResultWarningMsg();
            msg.decode(buf);//.readBytes((int) msg.getDecodePayloadLength()));
            warningMsgs.add(msg);
        }

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        int size = Serialization.getNeedBytes(rcode) + Serialization.getNeedBytes(msg.length)
                   + msg.length + Serialization.getNeedBytes(warningMsgs.size());
        if (warningMsgs.size() > 0) {
            for (ObRpcResultWarningMsg msg : warningMsgs) {
                size += msg.getPayloadSize();
            }
        }
        return size;
    }

    /*
     * Get rcode.
     */
    public int getRcode() {
        return rcode;
    }

    /*
     * Set rcode.
     */
    public void setRcode(int rcode) {
        this.rcode = rcode;
    }

    /*
     * Get msg.
     */
    public byte[] getMsg() {
        return msg;
    }

    /*
     * Get error msg.
     */
    public String getErrMsg() {
        if (msg != null && msg.length > 0) {
            return new String(msg, 0, msg.length - 1);
        }
        return new String(msg);
    }

    /*
     * Set msg.
     */
    public void setMsg(byte[] msg) {
        this.msg = msg;
    }

    /*
     * Get warning msgs.
     */
    public List<ObRpcResultWarningMsg> getWarningMsgs() {
        return warningMsgs;
    }

    /*
     * Set warning msgs.
     */
    public void setWarningMsgs(List<ObRpcResultWarningMsg> warningMsgs) {
        this.warningMsgs = warningMsgs;
    }
}

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

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObRpcResultWarningMsg extends AbstractPayload {

    private byte[] msg = new byte[0];
    private long   timestamp;
    private int    logLevel;
    private int    lineNo;
    private int    code;

    /**
     * Ob rpc result warning msg.
     */
    public ObRpcResultWarningMsg() {
    }

    /**
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return 0;
    }

    /**
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        int len = Serialization.getNeedBytes(msg.length);
        System.arraycopy(Serialization.encodeVi32(msg.length), 0, bytes, idx, len);
        idx += len;
        System.arraycopy(msg, 0, bytes, idx, msg.length);
        idx += msg.length;
        len = Serialization.getNeedBytes(timestamp);
        System.arraycopy(Serialization.encodeVi64(timestamp), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(logLevel);
        System.arraycopy(Serialization.encodeVi32(logLevel), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(lineNo);
        System.arraycopy(Serialization.encodeVi32(lineNo), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(code);
        System.arraycopy(Serialization.encodeVi32(code), 0, bytes, idx, len);

        return bytes;
    }

    /**
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        int len = Serialization.decodeVi32(buf);
        msg = new byte[len];
        buf.readBytes(msg);

        this.timestamp = Serialization.decodeVi64(buf);
        this.logLevel = Serialization.decodeVi32(buf);
        this.lineNo = Serialization.decodeVi32(buf);
        this.code = Serialization.decodeVi32(buf);

        return this;
    }

    /**
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(msg.length) + msg.length
               + Serialization.getNeedBytes(timestamp) + Serialization.getNeedBytes(logLevel)
               + Serialization.getNeedBytes(lineNo) + Serialization.getNeedBytes(code);
    }

    /**
     * Get msg.
     */
    public byte[] getMsg() {
        return msg;
    }

    /**
     * Set msg.
     */
    public void setMsg(byte[] msg) {
        this.msg = msg;
    }

    /**
     * Get timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Set timestamp.
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Get log level.
     */
    public int getLogLevel() {
        return logLevel;
    }

    /**
     * Set log level.
     */
    public void setLogLevel(int logLevel) {
        this.logLevel = logLevel;
    }

    /**
     * Get line no.
     */
    public int getLineNo() {
        return lineNo;
    }

    /**
     * Set line no.
     */
    public void setLineNo(int lineNo) {
        this.lineNo = lineNo;
    }

    /**
     * Get code.
     */
    public int getCode() {
        return code;
    }

    /**
     * Set code.
     */
    public void setCode(int code) {
        this.code = code;
    }
}

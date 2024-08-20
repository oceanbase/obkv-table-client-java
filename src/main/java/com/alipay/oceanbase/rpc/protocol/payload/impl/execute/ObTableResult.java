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
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

public class ObTableResult extends AbstractPayload {

    // -5024: duplicate key
    private int    errno;
    private byte[] sqlState;
    private byte[] msg;

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];

        int idx = 0;

        int len = (int) Serialization.getObUniVersionHeaderLength(getVersion(), getPayloadSize());
        byte[] header = Serialization.encodeObUniVersionHeader(getVersion(), getPayloadSize());
        System.arraycopy(header, 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(errno);
        System.arraycopy(Serialization.encodeVi32(errno), 0, bytes, idx, len);
        idx += len;

        System.arraycopy(Serialization.encodeBytes(sqlState), 0, bytes, idx,
            Serialization.getNeedBytes(sqlState));
        idx += Serialization.getNeedBytes(sqlState);
        System.arraycopy(Serialization.encodeBytes(msg), 0, bytes, idx,
            Serialization.getNeedBytes(msg));

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode version
        super.decode(buf);

        // 1. decode itself
        this.errno = Serialization.decodeVi32(buf);
        this.sqlState = Serialization.decodeBytes(buf);
        this.msg = Serialization.decodeBytes(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(errno) //
               + Serialization.getNeedBytes(sqlState) //
               + Serialization.getNeedBytes(msg);
    }

    /*
     * Get errno.
     */
    public int getErrno() {
        return errno;
    }

    /*
     * Set errno.
     */
    public void setErrno(int errno) {
        this.errno = errno;
    }

    /*
     * Get sql state.
     */
    public byte[] getSqlState() {
        return sqlState;
    }

    /*
     * Set sql state.
     */
    public void setSqlState(byte[] sqlState) {
        this.sqlState = sqlState;
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
}

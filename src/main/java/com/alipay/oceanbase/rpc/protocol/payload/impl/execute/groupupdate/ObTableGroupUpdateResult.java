/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.groupupdate;

import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationResult;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/**
 * @author xuanchao.xc
 * @since  2022-12-28
 */
public class ObTableGroupUpdateResult extends ObTableOperationResult {
    private boolean done;
    private String  reqNo;
    private String  leaderReqNo;

    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE_GROUP_UPDATE;
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        int len = (int) Serialization.getObUniVersionHeaderLength(getVersion(), getPayloadSize());
        byte[] header = Serialization.encodeObUniVersionHeader(getVersion(), getPayloadSize());
        System.arraycopy(header, 0, bytes, idx, len);
        idx += len;

        // 1. encode ObTableResult
        len = (int) this.getHeader().getPayloadSize();
        System.arraycopy(this.getHeader().encode(), 0, bytes, idx, len);
        idx += len;

        // 2. encode ObTableOperationResult
        System.arraycopy(Serialization.encodeI8(getOperationType().getByteValue()), 0, bytes, idx,
            1);
        idx += 1;

        // 3. encode entity
        len = (int) getEntity().getPayloadSize();
        System.arraycopy(getEntity().encode(), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(getAffectedRows());
        System.arraycopy(Serialization.encodeVi64(getAffectedRows()), 0, bytes, idx, len);
        idx += len;

        System.arraycopy(Serialization.encodeI8(done ? (byte) 1 : (byte) 0), 0, bytes, idx, 1);
        idx += 1;

        byte[] strbytes = Serialization.encodeVString(reqNo);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        strbytes = Serialization.encodeVString(leaderReqNo);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);

        return bytes;
    }

    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);
        this.done = Serialization.decodeI8(buf) != 0;
        this.reqNo = Serialization.decodeVString(buf);
        this.leaderReqNo = Serialization.decodeVString(buf);
        return this;
    }

    @Override
    public long getPayloadContentSize() {
        return super.getPayloadContentSize() + 1 + Serialization.getNeedBytes(reqNo)
               + Serialization.getNeedBytes(leaderReqNo);
    }

    public String getReqNo() {
        return reqNo;
    }

    public void setReqNo(String reqNo) {
        this.reqNo = reqNo;
    }

    public String getLeaderReqNo() {
        return leaderReqNo;
    }

    public void setLeaderReqNo(String leaderReqNo) {
        this.leaderReqNo = leaderReqNo;
    }

    public boolean isDone() {
        return done;
    }

    public void setDone(boolean done) {
        this.done = done;
    }
}

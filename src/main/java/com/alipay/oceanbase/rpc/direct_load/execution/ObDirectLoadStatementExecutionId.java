/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.direct_load.execution;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadIllegalArgumentException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObAddr;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class ObDirectLoadStatementExecutionId {

    private long                tableId = 0;
    private long                taskId  = 0;
    private ObAddr              svrAddr = new ObAddr();

    private ObDirectLoadTraceId traceId = null;

    public ObDirectLoadStatementExecutionId() {
    }

    public ObDirectLoadStatementExecutionId(long tableId, long taskId, ObAddr svrAddr)
                                                                                      throws ObDirectLoadException {
        if (tableId < 0 || taskId <= 0 || svrAddr == null) {
            throw new ObDirectLoadIllegalArgumentException(String.format(
                "invalid args, tableId:%d, taskId:%d, svrAddr:%s", tableId, taskId, svrAddr));
        }
        this.tableId = tableId;
        this.taskId = taskId;
        this.svrAddr = svrAddr;
    }

    public ObDirectLoadStatementExecutionId(long tableId, long taskId, ObAddr svrAddr,
                                            ObDirectLoadTraceId traceId)
                                                                        throws ObDirectLoadException {
        if (tableId < 0 || taskId <= 0 || svrAddr == null || traceId == null) {
            throw new ObDirectLoadIllegalArgumentException(String.format(
                "invalid args, tableId:%d, taskId:%d, svrAddr:%s, traceId:%s", tableId, taskId,
                svrAddr, traceId));
        }
        this.tableId = tableId;
        this.taskId = taskId;
        this.svrAddr = svrAddr;
        this.traceId = traceId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getTaskId() {
        return taskId;
    }

    public ObAddr getSvrAddr() {
        return svrAddr;
    }

    public ObDirectLoadTraceId getTraceId() {
        return traceId;
    }

    public boolean isValid() {
        return tableId >= 0 && taskId > 0 && svrAddr.isValid();
    }

    public String toString() {
        return String.format("{tableId:%d, taskId:%d, svrAddr:%s, traceId:%s}", tableId, taskId,
            svrAddr, traceId);
    }

    public byte[] encode() {
        int needBytes = (int) getEncodedSize();
        ObByteBuf buf = new ObByteBuf(needBytes);
        encode(buf);
        return buf.bytes;
    }

    public void encode(ObByteBuf buf) {
        Serialization.encodeVi64(buf, tableId);
        Serialization.encodeVi64(buf, taskId);
        svrAddr.encode(buf);
        if (traceId != null) {
            traceId.encode(buf);
        }
    }

    public ObDirectLoadStatementExecutionId decode(ByteBuf buf) {
        tableId = Serialization.decodeVi64(buf);
        taskId = Serialization.decodeVi64(buf);
        svrAddr.decode(buf);
        if (buf.readableBytes() > 0) {
            traceId = ObDirectLoadTraceId.decode(buf);
        }
        return this;
    }

    public ObDirectLoadStatementExecutionId decode(byte[] bytes) {
        ByteBuf buf = Unpooled.wrappedBuffer(bytes);
        return decode(buf);
    }

    public int getEncodedSize() {
        int len = 0;
        len += Serialization.getNeedBytes(tableId);
        len += Serialization.getNeedBytes(taskId);
        len += svrAddr.getEncodedSize();
        if (traceId != null) {
            len += traceId.getEncodedSize();
        }
        return len;
    }

}

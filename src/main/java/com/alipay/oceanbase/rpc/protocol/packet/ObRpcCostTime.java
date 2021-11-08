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

import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

public class ObRpcCostTime {

    static final int ENCODED_SIZE = 40;          // 8 is clusterId

    private int      len          = ENCODED_SIZE;
    private int      arrivalPushDiff;
    private int      pushPopDiff;
    private int      popProcessStartDiff;
    private int      processStartEndDiff;
    private int      processEndResponseDiff;
    private long     packetId;
    private long     requestArrivalTime;

    /**
     * Ob rpc cost time.
     */
    public ObRpcCostTime() {
    }

    /**
     * Encode.
     */
    public byte[] encode() {
        byte[] bytes = new byte[ENCODED_SIZE];
        int idx = 0;

        System.arraycopy(Serialization.encodeI32(len), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI32(arrivalPushDiff), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI32(pushPopDiff), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI32(popProcessStartDiff), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI32(processStartEndDiff), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI32(processEndResponseDiff), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI64(packetId), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI64(requestArrivalTime), 0, bytes, idx, 8);
        return bytes;
    }

    /**
     * Decode.
     */
    public Object decode(ByteBuf buf) {
        this.len = Serialization.decodeI32(buf);
        this.arrivalPushDiff = Serialization.decodeI32(buf);
        this.pushPopDiff = Serialization.decodeI32(buf);
        this.popProcessStartDiff = Serialization.decodeI32(buf);
        this.processStartEndDiff = Serialization.decodeI32(buf);
        this.processEndResponseDiff = Serialization.decodeI32(buf);
        this.packetId = Serialization.decodeI64(buf);
        this.requestArrivalTime = Serialization.decodeI64(buf);

        return this;
    }

    /**
     * Get arrival push diff.
     */
    public int getArrivalPushDiff() {
        return arrivalPushDiff;
    }

    /**
     * Set arrival push diff.
     */
    public void setArrivalPushDiff(int arrivalPushDiff) {
        this.arrivalPushDiff = arrivalPushDiff;
    }

    /**
     * Get push pop diff.
     */
    public int getPushPopDiff() {
        return pushPopDiff;
    }

    /**
     * Set push pop diff.
     */
    public void setPushPopDiff(int pushPopDiff) {
        this.pushPopDiff = pushPopDiff;
    }

    /**
     * Get pop process start diff.
     */
    public int getPopProcessStartDiff() {
        return popProcessStartDiff;
    }

    /**
     * Set pop process start diff.
     */
    public void setPopProcessStartDiff(int popProcessStartDiff) {
        this.popProcessStartDiff = popProcessStartDiff;
    }

    /**
     * Get process start end diff.
     */
    public int getProcessStartEndDiff() {
        return processStartEndDiff;
    }

    /**
     * Set process start end diff.
     */
    public void setProcessStartEndDiff(int processStartEndDiff) {
        this.processStartEndDiff = processStartEndDiff;
    }

    /**
     * Get process end response diff.
     */
    public int getProcessEndResponseDiff() {
        return processEndResponseDiff;
    }

    /**
     * Set process end response diff.
     */
    public void setProcessEndResponseDiff(int processEndResponseDiff) {
        this.processEndResponseDiff = processEndResponseDiff;
    }

    /**
     * Get packet id.
     */
    public long getPacketId() {
        return packetId;
    }

    /**
     * Set packet id.
     */
    public void setPacketId(long packetId) {
        this.packetId = packetId;
    }

    /**
     * Get request arrival time.
     */
    public long getRequestArrivalTime() {
        return requestArrivalTime;
    }

    /**
     * Set request arrival time.
     */
    public void setRequestArrivalTime(long requestArrivalTime) {
        this.requestArrivalTime = requestArrivalTime;
    }

}

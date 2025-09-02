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

import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static com.alipay.oceanbase.rpc.property.Property.RPC_OPERATION_TIMEOUT;
import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/*
 *
 *  varlong  varlong    plain bytes
 * -----------------------------------
 * |  ver  |  plen  | payload content |
 * -----------------------------------
 *
 */
public abstract class AbstractPayload implements ObPayload {

    private static final AtomicInteger CHANNELID = new AtomicInteger(1);
    protected static final long        INVALID_PAYLOAD_CONTENT_SIZE = -1;
    private long                       uniqueId;
    private long                       sequence;
    private Integer                    channelId = null;
    private boolean                    isRoutingWrong = false; // flag means tableEntry location or meta need to be refreshed
    private boolean                    isNeedRefreshMeta = false; // flag means tableEntry meta need to be refreshed
    protected long                     tenantId  = 1;
    private long                       version   = 1;
    protected long                     timeout   = RPC_OPERATION_TIMEOUT.getDefaultLong();
    protected int                      groupId   = 0;
    // for perf opt
    protected long                     payLoadContentSize = INVALID_PAYLOAD_CONTENT_SIZE;
    protected static volatile byte[]   defaultEncodeBytes = null;
    protected static volatile long 	   defaultPayLoadSize = INVALID_PAYLOAD_CONTENT_SIZE;
    // debug
    protected ArrayList<Long>          memberEncodeLengths = new ArrayList<>();
    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_ERROR_PACKET;
    }

    /*
     * Get timeout.
     */
    @Override
    public long getTimeout() {
        return timeout;
    }

    /*
    * Get isRoutingWrong
    * */
    @Override
    public boolean isRoutingWrong() {
        return this.isRoutingWrong;
    }

    /*
    * Set isRoutingWrong
    * */
    @Override
    public void setIsRoutingWrong(boolean isRoutingWrong) {
        this.isRoutingWrong = isRoutingWrong;
    }

    /*
    * Get isNeedRefreshMeta
    * */
    @Override
    public boolean isNeedRefreshMeta() {
        return this.isNeedRefreshMeta;
    }

    /*
    * Set isNeedRefreshMeta
    * */
    @Override
    public void setIsNeedRefreshMeta(boolean isNeedRefreshMeta) {
        this.isNeedRefreshMeta = isNeedRefreshMeta;
    }

    /*
     * Get version.
     */
    @Override
    public long getVersion() {
        return version;
    }

    /*
     * Set version.
     */
    public void setVersion(long version) {
        this.version = version;
    }

    /*
     * Set channel id.
     */
    public void setChannelId(Integer channelId) {
        this.channelId = channelId;
    }

    /*
     * Get payload size.
     */
    @Override
    public long getPayloadSize() {
        long payloadContentSize = getPayloadContentSize();
        return getObUniVersionHeaderLength(getVersion(), payloadContentSize) + payloadContentSize;
    }

    /*
     * Get channel id.
     */
    @Override
    public int getChannelId() {
        if (channelId == null) { // can only be init once
            channelId = CHANNELID.getAndIncrement();
        }
        return channelId;
    }

    /*
     * Get tenant id.
     */
    @Override
    public long getTenantId() {
        return tenantId;
    }

    /*
     * Set tenant id.
     */
    public void setTenantId(long tenantId) {
        this.tenantId = tenantId;
    }

    /*
     * Get group id.
     */
    @Override
    public int getGroupId() {
        return groupId;
    }

    /*
     * Set group id.
     */
    public void setGroupId(int groupId) {
        this.groupId = groupId;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        this.version = Serialization.decodeVi64(buf);
        Serialization.decodeVi64(buf); // get payload length, useless now

        return this;
    }

    /*
     * Get unique id.
     */
    public long getUniqueId() {
        return uniqueId;
    }

    /*
     * Set unique id.
     */
    public void setUniqueId(long uniqueId) {
        this.uniqueId = uniqueId;
    }

    /*
     * Get sequence.
     */
    public long getSequence() {
        return sequence;
    }

    /*
     * Set sequence.
     */
    public void setSequence(long sequence) {
        this.sequence = sequence;
    }

    /*
     * encode unis header
     */
    protected int encodeHeader(byte[] bytes, int idx) {
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;
        return idx;
    }

    protected void encodeHeader(ObByteBuf buf) {
        int posStart = buf.pos;
        encodeObUniVersionHeader(buf, getVersion(), getPayloadContentSize());
        int writeBufferLength = buf.pos - posStart;
        if (writeBufferLength != getObUniVersionHeaderLength(getVersion(), getPayloadContentSize())) {
            throw new IllegalArgumentException("error in encode Header (" +
                    "writeBufferLength:" + writeBufferLength + ", payLoadContentSize:" + this.payLoadContentSize + getVersion() + ")"
                    );
        }
    }

    // for perf opt
    protected byte[] encodeDefaultBytes() {
        if (defaultEncodeBytes == null) {
            synchronized (this.getClass()) {
                if (defaultEncodeBytes == null) {
                    defaultEncodeBytes = encode();
                }
            }
        }
        return defaultEncodeBytes;
    }

    protected boolean isUseDefaultEncode() { return false; }

}

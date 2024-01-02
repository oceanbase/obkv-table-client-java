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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.property.Property.RPC_OPERATION_TIMEOUT;
import static com.alipay.oceanbase.rpc.protocol.packet.ObCompressType.INVALID_COMPRESSOR;

public class ObRpcPacketHeader {

    /*
    *
    * pcode          (4 bytes) {@code ObTablePacketCode}
    * hlen           (1  byte) unsigned byte
    * priority       (1  byte) unsigned byte
    * flag           (2  byte) unsigned short
    * tenantId       (8  byte) unsigned long
    * prvTenantId    (8  byte) unsigned long
    * sessionId      (8  byte) unsigned long
    * traceId0       (8  byte) unsigned long
    * traceId1       (8  byte) unsigned long
    * timeout        (8  byte) unsigned long
    * timestamp      (8  byte) long
    * */
    private static final int HEADER_SIZE                               = 72;

    private static final int ENCODE_SIZE_WITH_COST_TIME                = HEADER_SIZE //
                                                                         + ObRpcCostTime.ENCODED_SIZE;

    private static final int ENCODE_SIZE_WITH_COST_TIME_AND_CLUSTER_ID = HEADER_SIZE //
                                                                         + ObRpcCostTime.ENCODED_SIZE //
                                                                         + 8;                              // clusterId

    private static final int ENCODE_SIZE                               = HEADER_SIZE //
                                                                         + ObRpcCostTime.ENCODED_SIZE //
                                                                         + 8 // 8 is clusterId
                                                                         + 4 // obCompressType
                                                                         + 4;                              // originalLen
    private static final int ENCODE_SIZE_V4                            = HEADER_SIZE
                                                                         + ObRpcCostTime.ENCODED_SIZE //
                                                                         + 8 // 8 is dst clusterId
                                                                         + 4 // obCompressType
                                                                         + 4 // originalLen
                                                                         + 8 // src clusterId
                                                                         + 8 // unis version
                                                                         + 4 // request level
                                                                         + 8 // seq no
                                                                         + 4 // group id
                                                                         + 8 // trace id2
                                                                         + 8 // trace id3
                                                                         + 8;                              //clusterNameHash
    public static final int  RESP_FLAG                                 = 1 << 15;

    public static final int  STREAM_FLAG                               = 1 << 14;

    public static final int  STREAM_LAST_FLAG                          = 1 << 13;

    public static final int  DISABLE_DEBUGSYNC_FLAG                    = 1 << 12;
    public static final int  CONTEXT_FLAG                              = 1 << 11;
    public static final int  UNNEED_RESPONSE_FLAG                      = 1 << 10;
    public static final int  REQUIRE_REROUTING_FLAG                    = 1 << 9;
    public static final int  IS_KV_REQUEST_FALG                        = 1 << 5;

    private int              pcode;
    private short            hlen                                      = 0;
    private short            priority                                  = 5;

    private short            flag                                      = 0;
    private long             checksum;
    private long             tenantId                                  = 1;
    private long             prvTenantId                               = 1;
    private long             sessionId;
    private long             traceId0;
    private long             traceId1;
    private long             timeout                                   = RPC_OPERATION_TIMEOUT
                                                                           .getDefaultLong() * 1000;       // OB server timeout (us)
    private long             timestamp                                 = System.currentTimeMillis() * 1000; // us
    private ObRpcCostTime    obRpcCostTime                             = new ObRpcCostTime();
    private long             dstClusterId                              = -1;                               // FIXME

    private ObCompressType   obCompressType                            = INVALID_COMPRESSOR;

    private int              originalLen                               = 0;
    private long             srcClusterId                              = -1;
    private long             unisVersion                               = 0;
    private int              requestLevel                              = 0;
    private long             seqNo                                     = 0;
    private int              groupId                                   = 0;
    private long             traceId2;
    private long             traceId3;
    private long             clusterNameHash;

    /*
     * Ob rpc packet header.
     */
    public ObRpcPacketHeader() {
        /*
        #define OB_LOG_LEVEL_NONE 7
        #define OB_LOG_LEVEL_NP -1  //set this level, would not print log
        #define OB_LOG_LEVEL_ERROR 0
        //#define OB_LOG_LEVEL_USER_ERROR  1
        #define OB_LOG_LEVEL_WARN  2
        #define OB_LOG_LEVEL_INFO  3
        #define OB_LOG_LEVEL_TRACE 4
        #define OB_LOG_LEVEL_DEBUG 5
         */
        flag = 0x7 | IS_KV_REQUEST_FALG; // 0x7 means let ObServer determine the ob log level.
    }

    /*
     * Encode.
     */
    public byte[] encode() {
        byte[] bytes = null;
        if (hlen != 0) {
            bytes = new byte[ENCODE_SIZE];
        } else if (ObGlobal.obVsnMajor() >= 4) {
            bytes = new byte[ENCODE_SIZE_V4];
            hlen = (short) ENCODE_SIZE_V4;
        } else {
            bytes = new byte[ENCODE_SIZE];
            hlen = (short) ENCODE_SIZE;
        }
        int idx = 0;

        System.arraycopy(Serialization.encodeI32(pcode), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI8(hlen), 0, bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeI8(priority), 0, bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeI16(flag), 0, bytes, idx, 2);
        idx += 2;
        System.arraycopy(Serialization.encodeI64(checksum), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI64(tenantId), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI64(prvTenantId), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI64(sessionId), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI64(traceId0), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI64(traceId1), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI64(timeout), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI64(timestamp), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(obRpcCostTime.encode(), 0, bytes, idx, ObRpcCostTime.ENCODED_SIZE);
        idx += ObRpcCostTime.ENCODED_SIZE;
        System.arraycopy(Serialization.encodeI64(dstClusterId), 0, bytes, idx, 8);
        idx += 8;
        System.arraycopy(Serialization.encodeI32(obCompressType.getCode()), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI32(originalLen), 0, bytes, idx, 4);
        if (ObGlobal.obVsnMajor() >= 4 && hlen >= ENCODE_SIZE_V4) {
            idx += 4;
            System.arraycopy(Serialization.encodeI64(srcClusterId), 0, bytes, idx, 8);
            idx += 8;
            System.arraycopy(Serialization.encodeI64(unisVersion), 0, bytes, idx, 8);
            idx += 8;
            System.arraycopy(Serialization.encodeI32(requestLevel), 0, bytes, idx, 4);
            idx += 4;
            System.arraycopy(Serialization.encodeI64(seqNo), 0, bytes, idx, 8);
            idx += 8;
            System.arraycopy(Serialization.encodeI32(groupId), 0, bytes, idx, 4);
            idx += 4;
            System.arraycopy(Serialization.encodeI64(traceId2), 0, bytes, idx, 8);
            idx += 8;
            System.arraycopy(Serialization.encodeI64(traceId3), 0, bytes, idx, 8);
            idx += 8;
            System.arraycopy(Serialization.encodeI64(clusterNameHash), 0, bytes, idx, 8);
        }

        return bytes;
    }

    /*
     * Decode.
     */
    public Object decode(ByteBuf buf) {
        this.pcode = Serialization.decodeI32(buf);
        this.hlen = Serialization.decodeUI8(buf);
        this.priority = Serialization.decodeUI8(buf);
        this.flag = Serialization.decodeI16(buf);
        this.checksum = Serialization.decodeI64(buf);
        this.tenantId = Serialization.decodeI64(buf);
        this.prvTenantId = Serialization.decodeI64(buf);
        this.sessionId = Serialization.decodeI64(buf);
        this.traceId0 = Serialization.decodeI64(buf);
        this.traceId1 = Serialization.decodeI64(buf);
        this.timeout = Serialization.decodeI64(buf);
        this.timestamp = Serialization.decodeI64(buf);

        if (hlen >= ENCODE_SIZE_V4) {
            obRpcCostTime.decode(buf);
            this.dstClusterId = Serialization.decodeI64(buf);
            this.obCompressType = ObCompressType.valueOf(Serialization.decodeI32(buf));
            this.originalLen = Serialization.decodeI32(buf);
            this.srcClusterId = Serialization.decodeI64(buf);
            this.unisVersion = Serialization.decodeI64(buf);
            this.requestLevel = Serialization.decodeI32(buf);
            this.seqNo = Serialization.decodeI64(buf);
            this.groupId = Serialization.decodeI32(buf);
            this.traceId2 = Serialization.decodeI64(buf);
            this.traceId3 = Serialization.decodeI64(buf);
            this.clusterNameHash = Serialization.decodeI64(buf);
            ignoreUnresolvedBytes(buf, hlen, ENCODE_SIZE_V4);
        } else if (hlen >= ENCODE_SIZE) {
            obRpcCostTime.decode(buf);
            this.dstClusterId = Serialization.decodeI64(buf);
            this.obCompressType = ObCompressType.valueOf(Serialization.decodeI32(buf));
            this.originalLen = Serialization.decodeI32(buf);
            ignoreUnresolvedBytes(buf, hlen, ENCODE_SIZE);
        } else if (hlen >= ENCODE_SIZE_WITH_COST_TIME_AND_CLUSTER_ID) {
            obRpcCostTime.decode(buf);
            this.dstClusterId = Serialization.decodeI64(buf);
            ignoreUnresolvedBytes(buf, hlen, ENCODE_SIZE_WITH_COST_TIME_AND_CLUSTER_ID);
        } else if (hlen >= ENCODE_SIZE_WITH_COST_TIME) {
            obRpcCostTime.decode(buf);
            ignoreUnresolvedBytes(buf, hlen, ENCODE_SIZE_WITH_COST_TIME);
        } else {
            ignoreUnresolvedBytes(buf, hlen, HEADER_SIZE);
        }

        return this;
    }

    /*
     * Ignore unresolved bytes.
     */
    public void ignoreUnresolvedBytes(ByteBuf buf, int hlen, int encodeSize) {
        for (int i = 0; i < hlen - encodeSize; i++) {
            buf.readByte();// ignore
        }
    }

    /*
     * Is response.
     */
    public boolean isResponse() {
        return (flag & RESP_FLAG) != 0;
    }

    /*
     * Is stream.
     */
    public boolean isStream() {
        return (flag & STREAM_FLAG) != 0;
    }

    /*
     * Is stream next.
     */
    public boolean isStreamNext() {
        return isStream() && (flag & STREAM_LAST_FLAG) == 0;
    }

    /*
     * Is stream last.
     */
    public boolean isStreamLast() {
        return isStream() && (flag & STREAM_LAST_FLAG) != 0;
    }

    /*
     * Is routing wrong.
     */
    public boolean isRoutingWrong() {
        return (flag & REQUIRE_REROUTING_FLAG) != 0;
    }

    /*
     * Set routing wrong flag bit.
     */
    public void setRoutingWrong() {
        flag |= REQUIRE_REROUTING_FLAG;
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
     * Set routing flag bit.
     */
    public void enableRerouting() {
        flag |= REQUIRE_REROUTING_FLAG;
    }

    public void disableRerouting() {
        flag &= ~REQUIRE_REROUTING_FLAG;
    }

    /*
     * Get pcode.
     */
    public int getPcode() {
        return pcode;
    }

    /*
     * Set pcode.
     */
    public void setPcode(int pcode) {
        this.pcode = pcode;
    }

    /*
     * Get hlen.
     */
    public short getHlen() {
        return hlen;
    }

    /*
     * Set hlen.
     */
    public void setHlen(byte hlen) {
        this.hlen = hlen;
    }

    /*
     * Get priority.
     */
    public short getPriority() {
        return priority;
    }

    /*
     * Set priority.
     */
    public void setPriority(short priority) {
        this.priority = priority;
    }

    /*
     * Get flag.
     */
    public short getFlag() {
        return flag;
    }

    /*
     * Set flag.
     */
    public void setFlag(short flag) {
        this.flag = flag;
    }

    /*
     * Get checksum.
     */
    public long getChecksum() {
        return checksum;
    }

    /*
     * Set checksum.
     */
    public void setChecksum(long checksum) {
        this.checksum = checksum;
    }

    /*
     * Get tenant id.
     */
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
     * Get prv tenant id.
     */
    public long getPrvTenantId() {
        return prvTenantId;
    }

    /*
     * Set prv tenant id.
     */
    public void setPrvTenantId(long prvTenantId) {
        this.prvTenantId = prvTenantId;
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
     * Get trace id0.
     */
    public long getTraceId0() {
        return traceId0;
    }

    /*
     * Set trace id0.
     */
    public void setTraceId0(long traceId0) {
        this.traceId0 = traceId0;
    }

    /*
     * Get trace id1.
     */
    public long getTraceId1() {
        return traceId1;
    }

    /*
     * Set trace id1.
     */
    public void setTraceId1(long traceId1) {
        this.traceId1 = traceId1;
    }

    /*
     * Get timeout.
     */
    public long getTimeout() {
        return timeout;
    }

    /*
     * Set timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /*
     * Get timestamp.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /*
     * Set timestamp.
     */
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    /*
     * Get ob rpc cost time.
     */
    public ObRpcCostTime getObRpcCostTime() {
        return obRpcCostTime;
    }

    /*
     * Set ob rpc cost time.
     */
    public void setObRpcCostTime(ObRpcCostTime obRpcCostTime) {
        this.obRpcCostTime = obRpcCostTime;
    }

    /*
     * Get cluster id.
     */
    public long getDstClusterId() {
        return dstClusterId;
    }

    /*
     * Set cluster id.
     */
    public void setDstClusterId(long dstClusterId) {
        this.dstClusterId = dstClusterId;
    }

    /*
     * Get ob compress type.
     */
    public ObCompressType getObCompressType() {
        return obCompressType;
    }

    /*
     * Set ob compress type.
     */
    public void setObCompressType(ObCompressType obCompressType) {
        this.obCompressType = obCompressType;
    }

    /*
     * Get original len.
     */
    public int getOriginalLen() {
        return originalLen;
    }

    /*
     * Set original len.
     */
    public void setOriginalLen(int originalLen) {
        this.originalLen = originalLen;
    }
}

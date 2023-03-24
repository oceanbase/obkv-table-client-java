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

package com.alipay.oceanbase.rpc.protocol.codec;

import com.alipay.oceanbase.rpc.protocol.packet.ObCompressType;
import com.alipay.oceanbase.rpc.protocol.packet.ObRpcCostTime;
import com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

import static com.alipay.oceanbase.rpc.bolt.protocol.ObTablePacketCode.OB_TABLE_API_LOGIN;
import static com.alipay.oceanbase.rpc.protocol.packet.ObCompressType.INVALID_COMPRESSOR;

public class ObRpcPacketHeaderTest {

    Random                 r                      = new Random();
    //ObRpcPacketHeader
    private int            pcode                  = OB_TABLE_API_LOGIN.value();
    private short          hlen                   = 128;
    private short          priority               = (short) (r.nextInt() & 0xff);
    private short          flag                   = (short) (r.nextInt() & 0xff);
    private long           checksum               = r.nextLong();
    private long           tenantId               = r.nextLong();
    private long           prvTenantId            = r.nextLong();
    private long           sessionId              = r.nextLong();
    private long           traceId0               = r.nextLong();
    private long           traceId1               = r.nextLong();
    private long           timeout                = r.nextLong();
    private long           timestamp              = System.currentTimeMillis();
    private ObRpcCostTime  obRpcCostTime          = new ObRpcCostTime();
    private long           clusterId              = r.nextLong();
    private ObCompressType obCompressType         = INVALID_COMPRESSOR;
    private int            originalLen            = r.nextInt();

    //ObRpcCostTime
    private int            len                    = 40;
    private int            arrivalPushDiff        = r.nextInt();
    private int            pushPopDiff            = r.nextInt();
    private int            popProcessStartDiff    = r.nextInt();
    private int            processStartEndDiff    = r.nextInt();
    private int            processEndResponseDiff = r.nextInt();
    private long           packetId               = r.nextLong();
    private long           requestArrivalTime     = System.currentTimeMillis();

    @Test
    public void testCodec() {

        byte[] obRpcCostTimeDecodeBytes = new byte[40];
        fillObRpcCostTime(obRpcCostTimeDecodeBytes, 0);
        ByteBuf obRpcCostTimeDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcCostTimeDecodeBuf.writeBytes(obRpcCostTimeDecodeBytes);
        ObRpcCostTime obRpcCostTime = new ObRpcCostTime();
        obRpcCostTime.decode(obRpcCostTimeDecodeBuf);
        checkObRpcCostTime(obRpcCostTime, obRpcCostTimeDecodeBytes, obRpcCostTimeDecodeBuf);

        byte[] obRpcPacketDecodeBytes = new byte[72];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 72);
        ByteBuf obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        ObRpcPacketHeader obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 72);

        obRpcPacketDecodeBytes = new byte[80];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 80);
        fillUnlessBytes(obRpcPacketDecodeBytes, r.nextLong(), 72, 8);
        obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 80);

        obRpcPacketDecodeBytes = new byte[100];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 100);
        fillUnlessBytes(obRpcPacketDecodeBytes, r.nextLong(), 72, 8);
        fillUnlessBytes(obRpcPacketDecodeBytes, 0, 80, 8);
        fillUnlessBytes(obRpcPacketDecodeBytes, r.nextLong(), 88, 8);
        obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 100);

        obRpcPacketDecodeBytes = new byte[112];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 112);
        fillObRpcCostTime(obRpcPacketDecodeBytes, 72);
        obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 112);

        obRpcPacketDecodeBytes = new byte[116];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 116);
        fillObRpcCostTime(obRpcPacketDecodeBytes, 72);
        fillUnlessBytes(obRpcPacketDecodeBytes, r.nextLong(), 112, 4);
        obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 116);

        obRpcPacketDecodeBytes = new byte[120];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 120);
        fillObRpcCostTime(obRpcPacketDecodeBytes, 72);
        fillClusterId(obRpcPacketDecodeBytes, 112);
        obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 120);

        obRpcPacketDecodeBytes = new byte[124];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 124);
        fillObRpcCostTime(obRpcPacketDecodeBytes, 72);
        fillClusterId(obRpcPacketDecodeBytes, 112);
        fillUnlessBytes(obRpcPacketDecodeBytes, r.nextLong(), 120, 4);
        obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 124);

        obRpcPacketDecodeBytes = new byte[128];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 124);
        fillObRpcCostTime(obRpcPacketDecodeBytes, 72);
        fillClusterId(obRpcPacketDecodeBytes, 112);
        fillCompress(obRpcPacketDecodeBytes, 120);
        obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 124);

        obRpcPacketDecodeBytes = new byte[136];
        fillObRpcPacketFixedHeader(obRpcPacketDecodeBytes, (short) 136);
        fillObRpcCostTime(obRpcPacketDecodeBytes, 72);
        fillClusterId(obRpcPacketDecodeBytes, 112);
        fillCompress(obRpcPacketDecodeBytes, 120);
        fillUnlessBytes(obRpcPacketDecodeBytes, r.nextLong(), 128, 4);
        obRpcPacketDecodeBuf = PooledByteBufAllocator.DEFAULT.buffer();
        obRpcPacketDecodeBuf.writeBytes(obRpcPacketDecodeBytes);
        obRpcPacketHeader = new ObRpcPacketHeader();
        obRpcPacketHeader.decode(obRpcPacketDecodeBuf);
        checkObRpcPacketHeader(obRpcPacketHeader, obRpcPacketDecodeBytes, obRpcPacketDecodeBuf,
            (short) 136);

        // check routing wrong flag setting.
        Assert.assertFalse(obRpcPacketHeader.isRoutingWrong());
        obRpcPacketHeader.setRoutingWrong();
        Assert.assertTrue(obRpcPacketHeader.isRoutingWrong());

    }

    private int fillObRpcCostTime(byte[] bytes, int idx) {
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
        idx += 8;
        return idx;
    }

    private void checkObRpcCostTime(ObRpcCostTime obRpcCostTime, byte[] obRpcCostTimeDecodeBytes,
                                    ByteBuf obRpcCostTimeDecodeBuf) {
        Assert.assertEquals(obRpcCostTime.getArrivalPushDiff(), arrivalPushDiff);
        Assert.assertEquals(obRpcCostTime.getPushPopDiff(), pushPopDiff);
        Assert.assertEquals(obRpcCostTime.getPopProcessStartDiff(), popProcessStartDiff);
        Assert.assertEquals(obRpcCostTime.getProcessStartEndDiff(), processStartEndDiff);
        Assert.assertEquals(obRpcCostTime.getProcessEndResponseDiff(), processEndResponseDiff);
        Assert.assertEquals(obRpcCostTime.getPacketId(), packetId);
        Assert.assertEquals(obRpcCostTime.getRequestArrivalTime(), requestArrivalTime);
        Assert.assertEquals(obRpcCostTimeDecodeBuf.readerIndex(), 40);
        byte[] obRpcCostTimeEncodeBytes = obRpcCostTime.encode();

        for (int i = 0; i < 40; i++) {
            Assert.assertEquals(obRpcCostTimeDecodeBytes[i], obRpcCostTimeEncodeBytes[i]);
        }
    }

    private int fillObRpcPacketFixedHeader(byte[] bytes, short hlen) {
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

        return idx;
    }

    private int fillClusterId(byte[] bytes, int idx) {
        System.arraycopy(Serialization.encodeI64(clusterId), 0, bytes, idx, 8);
        idx += 8;
        return idx;
    }

    private int fillCompress(byte[] bytes, int idx) {
        System.arraycopy(Serialization.encodeI32(obCompressType.getCode()), 0, bytes, idx, 4);
        idx += 4;
        System.arraycopy(Serialization.encodeI32(originalLen), 0, bytes, idx, 4);
        idx += 4;
        return idx;
    }

    private int fillUnlessBytes(byte[] bytes, long unless, int idx, int len) {
        System.arraycopy(Serialization.encodeI64(unless), 0, bytes, idx, len);
        idx += len;
        return idx;
    }

    private void checkObRpcPacketHeader(ObRpcPacketHeader obRpcPacketHeader,
                                        byte[] obRpcPacketDecodeBytes,
                                        ByteBuf obRpcPacketDecodeBuf, short hlen) {
        Assert.assertEquals(obRpcPacketHeader.getPcode(), pcode);
        Assert.assertEquals(obRpcPacketHeader.getHlen(), hlen);
        Assert.assertEquals(obRpcPacketHeader.getPriority(), priority);
        Assert.assertEquals(obRpcPacketHeader.getFlag(), flag);
        Assert.assertEquals(obRpcPacketHeader.getChecksum(), checksum);
        Assert.assertEquals(obRpcPacketHeader.getTenantId(), tenantId);
        Assert.assertEquals(obRpcPacketHeader.getPrvTenantId(), prvTenantId);
        Assert.assertEquals(obRpcPacketHeader.getSessionId(), sessionId);
        Assert.assertEquals(obRpcPacketHeader.getTraceId0(), traceId0);
        Assert.assertEquals(obRpcPacketHeader.getTraceId1(), traceId1);
        Assert.assertEquals(obRpcPacketHeader.getTimeout(), timeout);
        Assert.assertEquals(obRpcPacketHeader.getTimestamp(), timestamp);

        if (hlen >= 128) {
            checkObRpcPacketHeaderObRpcCostTime(obRpcPacketHeader);
            checkObRpcPacketHeaderClusterId(obRpcPacketHeader);
            checkObRpcPacketHeaderCompress(obRpcPacketHeader);
        } else if (hlen >= 120) {
            checkObRpcPacketHeaderObRpcCostTime(obRpcPacketHeader);
            checkObRpcPacketHeaderClusterId(obRpcPacketHeader);
        } else if (hlen >= 112) {
            checkObRpcPacketHeaderObRpcCostTime(obRpcPacketHeader);
        }

        Assert.assertEquals(hlen, obRpcPacketDecodeBuf.readerIndex());

        byte[] obRpcPacketEncodeBytes = obRpcPacketHeader.encode();

        int checkLen;

        if (hlen >= 128) {
            checkLen = 128;
        } else if (hlen >= 120) {
            checkLen = 120;
        } else if (hlen >= 112) {
            checkLen = 112;
        } else {
            checkLen = 72;
        }

        for (int i = 0; i < checkLen; i++) {
            Assert.assertEquals(obRpcPacketDecodeBytes[i], obRpcPacketEncodeBytes[i]);
        }
    }

    private void checkObRpcPacketHeaderObRpcCostTime(ObRpcPacketHeader obRpcPacketHeader) {
        Assert.assertEquals(obRpcPacketHeader.getObRpcCostTime().getArrivalPushDiff(),
            arrivalPushDiff);
        Assert.assertEquals(obRpcPacketHeader.getObRpcCostTime().getPushPopDiff(), pushPopDiff);
        Assert.assertEquals(obRpcPacketHeader.getObRpcCostTime().getPopProcessStartDiff(),
            popProcessStartDiff);
        Assert.assertEquals(obRpcPacketHeader.getObRpcCostTime().getProcessStartEndDiff(),
            processStartEndDiff);
        Assert.assertEquals(obRpcPacketHeader.getObRpcCostTime().getProcessEndResponseDiff(),
            processEndResponseDiff);
        Assert.assertEquals(obRpcPacketHeader.getObRpcCostTime().getPacketId(), packetId);
        Assert.assertEquals(obRpcPacketHeader.getObRpcCostTime().getRequestArrivalTime(),
            requestArrivalTime);
    }

    private void checkObRpcPacketHeaderClusterId(ObRpcPacketHeader obRpcPacketHeader) {
        Assert.assertEquals(obRpcPacketHeader.getDstClusterId(), clusterId);
    }

    private void checkObRpcPacketHeaderCompress(ObRpcPacketHeader obRpcPacketHeader) {
        Assert.assertEquals(obRpcPacketHeader.getObCompressType(), obCompressType);
        Assert.assertEquals(obRpcPacketHeader.getOriginalLen(), originalLen);
    }
}

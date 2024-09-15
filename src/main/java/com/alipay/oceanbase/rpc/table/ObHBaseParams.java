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

package com.alipay.oceanbase.rpc.table;

import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;

public class ObHBaseParams extends ObKVParamsBase {
    int     caching             = -1;   // 限制scan返回的行的数量
    int     callTimeout         = -1;   // scannerLeasePeriodTimeout，代表客户端scan的单个rpc超时时间以及服务端的scan的超时时间的一部分
    boolean allowPartialResults = true; // 是否允许行内部分返回
    boolean isCacheBlock        = false; // 是否启用缓存
    boolean checkExistenceOnly  = false; // 查看是否存在不返回数据

    public ObHBaseParams() {
        pType = paramType.HBase;
    }

    public ObKVParamsBase.paramType getType() {
        return pType;
    }

    public void setCaching(int caching) {
        this.caching = caching;
    }

    public void setCallTimeout(int callTimeout) {
        this.callTimeout = callTimeout;
    }

    public void setAllowPartialResults(boolean allowPartialResults) {
        this.allowPartialResults = allowPartialResults;
    }

    public void setCacheBlock(boolean isCacheBlock) {
        this.isCacheBlock = isCacheBlock;
    }

    public void setCheckExistenceOnly(boolean checkExistenceOnly) {
        this.checkExistenceOnly = checkExistenceOnly;
    }

    private int getContentSize() {
        return 4 + Serialization.getNeedBytes(caching) + Serialization.getNeedBytes(callTimeout)
               + 1;
    }

    public int getCaching() {
        return caching;
    }

    public int getCallTimeout() {
        return callTimeout;
    }

    public boolean getAllowPartialResults() {
        return allowPartialResults;
    }

    public boolean getCacheBlock() {
        return isCacheBlock;
    }

    public boolean isCheckExistenceOnly() {
        return checkExistenceOnly;
    }

    // encode all boolean type to one byte
    public byte[] booleansToByteArray() {
        byte[] bytes = new byte[1]; // 1 byte for 4 booleans

        if (allowPartialResults)
            bytes[0] |= 0x01; // 00000010
        if (isCacheBlock)
            bytes[0] |= 0x02; // 00000100
        if (checkExistenceOnly)
            bytes[0] |= 0x04; // 00001000

        return bytes;
    }

    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadContentSize()];
        int idx = 0;

        byte[] b = new byte[] { (byte) pType.ordinal() };
        System.arraycopy(b, 0, bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeVi32(caching), 0, bytes, idx,
            Serialization.getNeedBytes(caching));
        idx += Serialization.getNeedBytes(caching);
        System.arraycopy(Serialization.encodeVi32(callTimeout), 0, bytes, idx,
            Serialization.getNeedBytes(callTimeout));
        idx += Serialization.getNeedBytes(callTimeout);
        System.arraycopy(booleansToByteArray(), 0, bytes, idx, 1);
        idx += 1;

        return bytes;
    }

    public void byteArrayToBooleans(ByteBuf bytes) {
        byte b = bytes.readByte();
        allowPartialResults = (b & 0x01) != 0;
        isCacheBlock = (b & 0x02) != 0;
        checkExistenceOnly = (b & 0x04) != 0;
    }

    public Object decode(ByteBuf buf) {
        caching = Serialization.decodeVi32(buf);
        callTimeout = Serialization.decodeVi32(buf);
        byteArrayToBooleans(buf);
        return this;
    }

    public long getPayloadContentSize() {
        return 1 + Serialization.getNeedBytes(caching) + Serialization.getNeedBytes(callTimeout)
               + 1; // all boolean to one byte
    }

    public String toString() {
        return "ObParams: {\n pType = " + pType + ", \n caching = " + caching
               + ", \n callTimeout = " + callTimeout + ", \n allowPartialResult = "
               + allowPartialResults + ", \n isCacheBlock = " + isCacheBlock
               + ", \n checkExistenceOnly = " + checkExistenceOnly + "\n}\n";
    }

}

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
    int                      caching                    = -1;     // limit the number of for each rpc call
    int                      callTimeout                = -1;     // scannerLeasePeriodTimeout in hbase, client rpc timeout
    boolean                  allowPartialResults        = false;  // whether allow partial row return or not
    boolean                  isCacheBlock               = false;  // whether enable server block cache and row cache or not
    boolean                  checkExistenceOnly         = false;  // check the existence only
    String                   hbaseVersion               = "1.3.6";

    private static final int FLAG_ALLOW_PARTIAL_RESULTS = 1 << 0;
    private static final int FLAG_IS_CACHE_BLOCK        = 1 << 1;
    private static final int FLAG_CHECK_EXISTENCE_ONLY  = 1 << 2;

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

    public void setHbaseVersion(String version) {
        this.hbaseVersion = version;
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

    public String getHbaseVersion() {
        return hbaseVersion;
    }

    // encode all boolean type to one byte
    public byte[] booleansToByteArray() {
        byte[] bytes = new byte[1]; // 1 byte for 4 booleans

        if (allowPartialResults)
            bytes[0] |= FLAG_ALLOW_PARTIAL_RESULTS;
        if (isCacheBlock)
            bytes[0] |= FLAG_IS_CACHE_BLOCK;
        if (checkExistenceOnly)
            bytes[0] |= FLAG_CHECK_EXISTENCE_ONLY;

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
        System.arraycopy(Serialization.encodeVString(hbaseVersion), 0, bytes, idx,
            Serialization.getNeedBytes(hbaseVersion));
        idx += Serialization.getNeedBytes(hbaseVersion);

        return bytes;
    }

    public void byteArrayToBooleans(ByteBuf bytes) {
        byte b = bytes.readByte();
        allowPartialResults = (b & FLAG_ALLOW_PARTIAL_RESULTS) != 0;
        isCacheBlock = (b & FLAG_IS_CACHE_BLOCK) != 0;
        checkExistenceOnly = (b & FLAG_CHECK_EXISTENCE_ONLY) != 0;
    }

    public Object decode(ByteBuf buf) {
        caching = Serialization.decodeVi32(buf);
        callTimeout = Serialization.decodeVi32(buf);
        byteArrayToBooleans(buf);
        hbaseVersion = Serialization.decodeVString(buf);
        return this;
    }

    public long getPayloadContentSize() {
        return 1 + Serialization.getNeedBytes(caching) + Serialization.getNeedBytes(callTimeout)
               + Serialization.getNeedBytes(hbaseVersion) + 1; // all boolean to one byte
    }

    public String toString() {
        return "ObParams: {\n pType = " + pType + ", \n caching = " + caching
               + ", \n callTimeout = " + callTimeout + ", \n allowPartialResult = "
               + allowPartialResults + ", \n isCacheBlock = " + isCacheBlock
               + ", \n checkExistenceOnly = " + checkExistenceOnly + ", \n hbaseVersion = "
               + hbaseVersion + "\n}\n";
    }

}

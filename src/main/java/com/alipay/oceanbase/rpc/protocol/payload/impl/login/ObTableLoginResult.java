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

package com.alipay.oceanbase.rpc.protocol.payload.impl.login;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableLoginResult extends AbstractPayload {

    private int           serverCapabilities;
    private int           reversed1 = 0;
    private long          reversed2 = 0;

    private String        serverVersion;
    private ObBytesString credential;        // TODO byte[] 类型，内部的证书结构，后面的rpc请求里面都带着这个字段过来做认证
    private long          tenantId;          // 除了login的请求其他请求都要带上tenantId否则都会用系统租户的CPU来运行
    private long          userId;
    private long          databaseId;

    /*
     * Ob table login result.
     */
    public ObTableLoginResult() {
    }

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_LOGIN;
    }

    /*
     * Encode.
     */
    public byte[] encode() {
        byte[] bytes = new byte[(int) (getPayloadSize())];
        int idx = 0;

        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        int len = Serialization.getNeedBytes(serverCapabilities);
        System.arraycopy(Serialization.encodeVi32(serverCapabilities), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(reversed1);
        System.arraycopy(Serialization.encodeVi32(reversed1), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(reversed2);
        System.arraycopy(Serialization.encodeVi64(reversed2), 0, bytes, idx, len);
        idx += len;

        byte[] strbytes = Serialization.encodeVString(serverVersion);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        strbytes = Serialization.encodeBytesString(credential);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        len = Serialization.getNeedBytes(tenantId);
        System.arraycopy(Serialization.encodeVi64(tenantId), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(userId);
        System.arraycopy(Serialization.encodeVi64(userId), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(databaseId);
        System.arraycopy(Serialization.encodeVi64(databaseId), 0, bytes, idx, len);

        return bytes;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(serverCapabilities)
               + Serialization.getNeedBytes(reversed1) + Serialization.getNeedBytes(reversed2)
               + Serialization.getNeedBytes(serverVersion) + Serialization.getNeedBytes(credential)
               + Serialization.getNeedBytes(tenantId) + Serialization.getNeedBytes(userId)
               + Serialization.getNeedBytes(databaseId);
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        this.serverCapabilities = Serialization.decodeVi32(buf);
        Serialization.decodeVi32(buf); // reserved
        Serialization.decodeVi64(buf); // reserved

        this.serverVersion = Serialization.decodeVString(buf);
        this.credential = Serialization.decodeBytesString(buf);

        this.tenantId = Serialization.decodeVi64(buf);
        this.userId = Serialization.decodeVi64(buf);
        this.databaseId = Serialization.decodeVi64(buf);

        return this;
    }

    /*
     * Get server capabilities.
     */
    public int getServerCapabilities() {
        return serverCapabilities;
    }

    /*
     * Set server capabilities.
     */
    public void setServerCapabilities(int serverCapabilities) {
        this.serverCapabilities = serverCapabilities;
    }

    /*
     * Get reversed1.
     */
    public int getReversed1() {
        return reversed1;
    }

    /*
     * Set reversed1.
     */
    public void setReversed1(int reversed1) {
        this.reversed1 = reversed1;
    }

    /*
     * Get reversed2.
     */
    public long getReversed2() {
        return reversed2;
    }

    /*
     * Set reversed2.
     */
    public void setReversed2(long reversed2) {
        this.reversed2 = reversed2;
    }

    /*
     * Get server version.
     */
    public String getServerVersion() {
        return serverVersion;
    }

    /*
     * Set server version.
     */
    public void setServerVersion(String serverVersion) {
        this.serverVersion = serverVersion;
    }

    /*
     * Get credential.
     */
    public ObBytesString getCredential() {
        return credential;
    }

    /*
     * Set credential.
     */
    public void setCredential(ObBytesString credential) {
        this.credential = credential;
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
     * Get user id.
     */
    public long getUserId() {
        return userId;
    }

    /*
     * Set user id.
     */
    public void setUserId(long userId) {
        this.userId = userId;
    }

    /*
     * Get database id.
     */
    public long getDatabaseId() {
        return databaseId;
    }

    /*
     * Set database id.
     */
    public void setDatabaseId(long databaseId) {
        this.databaseId = databaseId;
    }
}

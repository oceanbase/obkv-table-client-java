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
import io.netty.buffer.ByteBufAllocator;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableLoginRequest extends AbstractPayload {

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_LOGIN;
    }

    private byte          authMethod    = (byte) 0x01;
    private byte          clientType    = (byte) 0x02;
    private byte          clientVersion = (byte) 0x01;
    private byte          reversed1;

    private int           clientCapabilities;
    private int           maxPacketSize;
    private int           reversed2     = 0;
    private long          reversed3     = 0;

    private String        tenantName;
    private String        userName;
    private ObBytesString passSecret;                 // hash 后的密码
    private ObBytesString passScramble;               // 20 字节随机字符串
    private String        databaseName;
    private long          ttlUs;

    /*
     * Ob table login request.
     */
    public ObTableLoginRequest() {
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 1. encode header
        // ver + plen + payload
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        // 2. encode payload
        bytes[idx++] = authMethod;
        bytes[idx++] = clientType;
        bytes[idx++] = clientVersion;
        bytes[idx++] = reversed1;

        int len = Serialization.getNeedBytes(clientCapabilities);
        System.arraycopy(Serialization.encodeVi32(clientCapabilities), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(maxPacketSize);
        System.arraycopy(Serialization.encodeVi32(maxPacketSize), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(reversed2);
        System.arraycopy(Serialization.encodeVi32(reversed2), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(reversed3);
        System.arraycopy(Serialization.encodeVi64(reversed3), 0, bytes, idx, len);
        idx += len;

        byte[] strbytes = Serialization.encodeVString(tenantName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        strbytes = Serialization.encodeVString(userName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        strbytes = Serialization.encodeBytesString(passSecret);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        strbytes = Serialization.encodeBytesString(passScramble);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        strbytes = Serialization.encodeVString(databaseName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        len = Serialization.getNeedBytes(ttlUs);
        System.arraycopy(Serialization.encodeVi64(ttlUs), 0, bytes, idx, len);

        return bytes;
    }

    /*
     * Decode.
     */
    public Object decode(byte[] bytes) {
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer();
        try {
            buf.writeBytes(bytes);
            return decode(buf);
        } finally {
            buf.release();
        }
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        this.authMethod = buf.readByte();
        this.clientType = buf.readByte();
        this.clientVersion = buf.readByte();
        buf.readByte();

        this.clientCapabilities = Serialization.decodeVi32(buf);
        this.maxPacketSize = Serialization.decodeVi32(buf);
        Serialization.decodeVi32(buf);
        Serialization.decodeVi64(buf);

        this.tenantName = Serialization.decodeVString(buf);
        this.userName = Serialization.decodeVString(buf);
        this.passSecret = Serialization.decodeBytesString(buf);
        this.passScramble = Serialization.decodeBytesString(buf);
        this.databaseName = Serialization.decodeVString(buf);
        this.ttlUs = Serialization.decodeVi64(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return 4 + Serialization.getNeedBytes(clientCapabilities)
               + Serialization.getNeedBytes(maxPacketSize) + Serialization.getNeedBytes(reversed2)
               + Serialization.getNeedBytes(reversed3) + Serialization.getNeedBytes(tenantName)
               + Serialization.getNeedBytes(userName) + Serialization.getNeedBytes(passSecret)
               + Serialization.getNeedBytes(passScramble)
               + Serialization.getNeedBytes(databaseName) + Serialization.getNeedBytes(ttlUs);
    }

    /*
     * Get auth method.
     */
    public byte getAuthMethod() {
        return authMethod;
    }

    /*
     * Set auth method.
     */
    public void setAuthMethod(byte authMethod) {
        this.authMethod = authMethod;
    }

    /*
     * Get client type.
     */
    public byte getClientType() {
        return clientType;
    }

    /*
     * Set client type.
     */
    public void setClientType(byte clientType) {
        this.clientType = clientType;
    }

    /*
     * Get client version.
     */
    public byte getClientVersion() {
        return clientVersion;
    }

    /*
     * Set client version.
     */
    public void setClientVersion(byte clientVersion) {
        this.clientVersion = clientVersion;
    }

    /*
     * Get reversed1.
     */
    public byte getReversed1() {
        return reversed1;
    }

    /*
     * Set reversed1.
     */
    public void setReversed1(byte reversed1) {
        this.reversed1 = reversed1;
    }

    /*
     * Get client capabilities.
     */
    public int getClientCapabilities() {
        return clientCapabilities;
    }

    /*
     * Set client capabilities.
     */
    public void setClientCapabilities(int clientCapabilities) {
        this.clientCapabilities = clientCapabilities;
    }

    /*
     * Get max packet size.
     */
    public int getMaxPacketSize() {
        return maxPacketSize;
    }

    /*
     * Set max packet size.
     */
    public void setMaxPacketSize(int maxPacketSize) {
        this.maxPacketSize = maxPacketSize;
    }

    /*
     * Get reversed2.
     */
    public int getReversed2() {
        return reversed2;
    }

    /*
     * Set reversed2.
     */
    public void setReversed2(int reversed2) {
        this.reversed2 = reversed2;
    }

    /*
     * Get reversed3.
     */
    public long getReversed3() {
        return reversed3;
    }

    /*
     * Set reversed3.
     */
    public void setReversed3(long reversed3) {
        this.reversed3 = reversed3;
    }

    /*
     * Get tenant name.
     */
    public String getTenantName() {
        return tenantName;
    }

    /*
     * Set tenant name.
     */
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    /*
     * Get user name.
     */
    public String getUserName() {
        return userName;
    }

    /*
     * Set user name.
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /*
     * Get pass secret.
     */
    public ObBytesString getPassSecret() {
        return passSecret;
    }

    /*
     * Set pass secret.
     */
    public void setPassSecret(ObBytesString passSecret) {
        this.passSecret = passSecret;
    }

    /*
     * Set pass secret.
     */
    public void setPassSecret(String passSecret) {
        this.passSecret = new ObBytesString(passSecret);
    }

    /*
     * Get pass scramble.
     */
    public ObBytesString getPassScramble() {
        return passScramble;
    }

    /*
     * Set pass scramble.
     */
    public void setPassScramble(ObBytesString passScramble) {
        this.passScramble = passScramble;
    }

    /*
     * Get database name.
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /*
     * Set database name.
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /*
     * Get ttl us.
     */
    public long getTtlUs() {
        return ttlUs;
    }

    /*
     * Set ttl us.
     */
    public void setTtlUs(long ttlUs) {
        this.ttlUs = ttlUs;
    }

}

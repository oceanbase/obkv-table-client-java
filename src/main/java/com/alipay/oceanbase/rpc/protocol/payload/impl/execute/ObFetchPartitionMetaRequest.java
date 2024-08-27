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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Credentialable;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/**
 *
 *  OB_SERIALIZE_MEMBER(ObFetchPartitionMetaRequest
 *   header_,
 *   private String credential_,
 *   private String tableName_,
 *   private String clusterName_,
 *   private String tenantName_,
 *   private String databaseName_,
 *   boolean forceRenew_
 *  );
 *
 */
public class ObFetchPartitionMetaRequest extends AbstractPayload implements Credentialable {
    private ObFetchPartitionMetaType obFetchPartitionMetaType;
    private ObBytesString credential;
    private String        tableName;
    private String        clusterName;
    private String        tenantName;
    private String        databaseName;
    private boolean       forceRenew = false;

    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_PART_META_QUERY;
    }

    public void setObFetchPartitionMetaType(ObFetchPartitionMetaType type) {
        this.obFetchPartitionMetaType = type;
    }

    public void setObFetchPartitionMetaType(int index) {
        this.obFetchPartitionMetaType = ObFetchPartitionMetaType.valueOf(index);
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    public void setForceRenew(boolean forceRenew) {
        this.forceRenew = forceRenew;
    }

    public void setTableName(String tableName) { this.tableName = tableName; }

    public void setTimeout(long timeout) { this.timeout = timeout; }

    /*
     * Set credential.
     */
    @Override
    public void setCredential(ObBytesString credential) {
        this.credential = credential;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;
        // 0. encode ObTableOperationRequest header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        // encode type
        int len = Serialization.getNeedBytes(obFetchPartitionMetaType.getIndex());
        System.arraycopy(Serialization.encodeVi32(obFetchPartitionMetaType.getIndex()), 0, bytes, idx, len);
        idx += len;

        // encode credential
        idx = encodeCredential(bytes, idx);

        // encode tableName
        byte[] strbytes = Serialization.encodeVString(tableName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        // encode clusterName
        strbytes = Serialization.encodeVString(clusterName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        // encode tenantName
        strbytes = Serialization.encodeVString(tenantName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        // encode databaseName
        strbytes = Serialization.encodeVString(databaseName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        // encode force_renew for ODP route
        System.arraycopy(Serialization.encodeI8(forceRenew ? ((byte) 1) : ((byte) 0)), 0, bytes, idx, 1);

        return bytes;
    }

    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        this.credential = Serialization.decodeBytesString(buf);
        this.tableName = Serialization.decodeVString(buf);
        this.clusterName = Serialization.decodeVString(buf);
        this.tenantName = Serialization.decodeVString(buf);
        this.databaseName = Serialization.decodeVString(buf);
        this.forceRenew = Serialization.decodeI8(buf) == 1;

        return this;
    }

    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(obFetchPartitionMetaType.getIndex())
                    + Serialization.getNeedBytes(credential)
                    + Serialization.getNeedBytes(tableName)
                    + Serialization.getNeedBytes(clusterName)
                    + Serialization.getNeedBytes(tenantName)
                    + Serialization.getNeedBytes(databaseName) + 1;
    }

    private int encodeCredential(byte[] bytes, int idx) {
        byte[] strbytes = Serialization.encodeBytesString(credential);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        return idx;
    }

    public static ObFetchPartitionMetaRequest getInstance(int typeIdx, String tableName, String clusterName,
                                                          String tenantName, String databaseName,
                                                          boolean forceRenew, long timeout) {
        ObFetchPartitionMetaRequest request = new ObFetchPartitionMetaRequest();
        request.setObFetchPartitionMetaType(typeIdx);
        request.setTableName(tableName);
        if (clusterName == null) {
            clusterName = "";
        }
        request.setClusterName(clusterName);
        if (tenantName == null) {
            tenantName = "";
        }
        request.setTenantName(tenantName);
        request.setDatabaseName(databaseName);
        request.setForceRenew(forceRenew);
        request.setTimeout(timeout);
        return request;
    }
}

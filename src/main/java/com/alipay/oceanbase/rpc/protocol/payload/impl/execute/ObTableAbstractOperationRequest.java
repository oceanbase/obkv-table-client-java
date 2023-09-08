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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.Credentialable;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public abstract class ObTableAbstractOperationRequest extends AbstractPayload implements
                                                                             Credentialable {

    protected ObBytesString           credential;                                              // the credential returned when login. 登陆时返回的证书
    protected String                  tableName;                                               // table name. 待访问的表名
    protected long                    tableId                 = Constants.OB_INVALID_ID;       // table id. 如果知道表id，可以用于优化，如果不知道，设定为OB_INVALID_ID
    protected long                    partitionId             = Constants.INVALID_TABLET_ID;   // Constants.OB_INVALID_ID; // partition id / tabletId. 如果知道表分区id，可以用于优化，如果不知道，设定为OB_INVALID_ID
    protected ObTableEntityType       entityType              = ObTableEntityType.DYNAMIC;     // entity type. 如果明确entity类型，可以用于优化，如果不知道，设定为ObTableEntityType::DYNAMIC
    protected ObTableConsistencyLevel consistencyLevel        = ObTableConsistencyLevel.STRONG; // read consistency level. 读一致性，是否要强一致性等（必须读到刚写入的数据）. 目前只支持STRONG.
    protected boolean                 returningRowKey         = false;
    protected boolean                 returningAffectedEntity = false;
    protected boolean                 returningAffectedRows   = false;

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        if (ObGlobal.OB_VERSION.majorVersion >= 4)
            return Serialization.getNeedBytes(credential) + Serialization.getNeedBytes(tableName)
                   + Serialization.getNeedBytes(tableId) + 8 + 2 + 3;
        else
            return Serialization.getNeedBytes(credential) + Serialization.getNeedBytes(tableName)
                   + Serialization.getNeedBytes(tableId) + Serialization.getNeedBytes(partitionId)
                   + 2 + 3;
    }

    protected int encodeHeader(byte[] bytes, int idx) {
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;
        return idx;
    }

    protected int encodeCredential(byte[] bytes, int idx) {
        byte[] strbytes = Serialization.encodeBytesString(credential);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        return idx;
    }

    /*
     * tabletId also be treated as patitionId.
     */
    protected int encodeTableMetaWithPartitionId(byte[] bytes, int idx) {
        byte[] strbytes = Serialization.encodeVString(tableName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        int len = Serialization.getNeedBytes(tableId);
        System.arraycopy(Serialization.encodeVi64(tableId), 0, bytes, idx, len);
        idx += len;
        if (ObGlobal.OB_VERSION.majorVersion >= 4) {
            System.arraycopy(Serialization.encodeI64(partitionId), 0, bytes, idx, 8);
            idx += 8;
        } else {
            len = Serialization.getNeedBytes(partitionId);
            System.arraycopy(Serialization.encodeVi64(partitionId), 0, bytes, idx, len);
            idx += len;
        }
        System.arraycopy(Serialization.encodeI8(entityType.getByteValue()), 0, bytes, idx, 1);
        idx += 1;
        return idx;
    }

    protected int encodeTableMetaWithoutPartitionId(byte[] bytes, int idx) {
        byte[] strbytes = Serialization.encodeVString(tableName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        int len = Serialization.getNeedBytes(tableId);
        System.arraycopy(Serialization.encodeVi64(tableId), 0, bytes, idx, len);
        idx += len;
        System.arraycopy(Serialization.encodeI8(entityType.getByteValue()), 0, bytes, idx, 1);
        idx += 1;
        return idx;
    }

    protected int encodeConsistencyLevel(byte[] bytes, int idx) {
        System.arraycopy(Serialization.encodeI8(consistencyLevel.getByteValue()), 0, bytes, idx, 1);
        idx++;
        return idx;
    }

    /*
     * Get credential.
     */
    public ObBytesString getCredential() {
        return credential;
    }

    /*
     * Set timeout.
     */
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    /*
     * Set tenant id.
     */
    public void setTenantId(long tenantId) {
        this.tenantId = tenantId;
    }

    /*
     * Set credential.
     */
    @Override
    public void setCredential(ObBytesString credential) {
        this.credential = credential;
    }

    /*
     * Get table name.
     */
    public String getTableName() {
        return tableName;
    }

    /*
     * Set table name.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /*
     * Get table id.
     */
    public long getTableId() {
        return tableId;
    }

    /*
     * Set table id.
     */
    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    /*
     * Get partition id.
     */
    public long getPartitionId() {
        return partitionId;
    }

    /*
     * Set partition id.
     */
    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    /*
     * Get entity type.
     */
    public ObTableEntityType getEntityType() {
        return entityType;
    }

    /*
     * Set entity type.
     */
    public void setEntityType(ObTableEntityType entityType) {
        this.entityType = entityType;
    }

    /*
     * Get consistency level.
     */
    public ObTableConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    /*
     * Set consistency level.
     */
    public void setConsistencyLevel(ObTableConsistencyLevel consistencyLevel) {
        this.consistencyLevel = consistencyLevel;
    }

    /*
     * Is returning row key.
     */
    public boolean isReturningRowKey() {
        return returningRowKey;
    }

    /*
     * Set returning row key.
     */
    public void setReturningRowKey(boolean returningRowKey) {
        this.returningRowKey = returningRowKey;
    }

    /*
     * Is returning affected entity.
     */
    public boolean isReturningAffectedEntity() {
        return returningAffectedEntity;
    }

    /*
     * Set returning affected entity.
     */
    public void setReturningAffectedEntity(boolean returningAffectedEntity) {
        this.returningAffectedEntity = returningAffectedEntity;
    }

    /*
     * Is returning affected rows.
     */
    public boolean isReturningAffectedRows() {
        return returningAffectedRows;
    }

    /*
     * Set returning affected rows.
     */
    public void setReturningAffectedRows(boolean returningAffectedRows) {
        this.returningAffectedRows = returningAffectedRows;
    }
}

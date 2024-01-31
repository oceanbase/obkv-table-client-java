/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2024 OceanBase
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
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class ObTableSingleOpResult extends AbstractPayload {
    private ObTableResult         header        = new ObTableResult();
    private ObTableOperationType  operationType = ObTableOperationType.GET;
    private ObTableSingleOpEntity entity        = new ObTableSingleOpEntity();
    private long                  affectedRows  = 0;
    private String                executeHost;
    private int                   executePort;
    private List<String>          propertiesColumnNames = new ArrayList<>();
    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_LS_EXECUTE;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        int len = (int) Serialization.getObUniVersionHeaderLength(getVersion(), getPayloadSize());
        byte[] header = Serialization.encodeObUniVersionHeader(getVersion(), getPayloadSize());
        System.arraycopy(header, 0, bytes, idx, len);
        idx += len;

        // 1. encode ObTableResult
        len = (int) this.header.getPayloadSize();
        System.arraycopy(this.header.encode(), 0, bytes, idx, len);
        idx += len;

        // 2. encode ObTableOperationResult
        System.arraycopy(Serialization.encodeI8(operationType.getByteValue()), 0, bytes, idx, 1);
        idx += 1;

        // 3. encode entity
        len = (int) entity.getPayloadSize();
        System.arraycopy(entity.encode(), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(affectedRows);
        System.arraycopy(Serialization.encodeVi64(affectedRows), 0, bytes, idx, len);

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode version
        super.decode(buf);

        // 1. decode ObTableResult
        this.header.decode(buf);

        // 2. decode types
        this.operationType = ObTableOperationType.valueOf(Serialization.decodeI8(buf.readByte()));

        // 3. decode Entity
        this.entity.setAggPropertiesNames(propertiesColumnNames);
        this.entity.decode(buf);

        // 4. decode affected rows
        this.affectedRows = Serialization.decodeVi64(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        return header.getPayloadSize()
                + entity.getPayloadSize()
                + Serialization.getNeedBytes(affectedRows)
                + 1; // operation type
    }

    /*
     * Get header.
     */
    public ObTableResult getHeader() {
        return header;
    }

    /*
     * Set header.
     */
    public void setHeader(ObTableResult header) {
        this.header = header;
    }

    /*
     * Get operation type.
     */
    public ObTableOperationType getOperationType() {
        return operationType;
    }

    /*
     * Set operation type.
     */
    public void setOperationType(ObTableOperationType operationType) {
        this.operationType = operationType;
    }

    /*
     * Get entity.
     */
    public ObTableSingleOpEntity getEntity() {
        return entity;
    }

    /*
     * Set entity.
     */
    public void setEntity(ObTableSingleOpEntity entity) {
        this.entity = entity;
    }

    /*
     * Get affected rows.
     */
    public long getAffectedRows() {
        return affectedRows;
    }

    /*
     * Set affected rows.
     */
    public void setAffectedRows(long affectedRows) {
        this.affectedRows = affectedRows;
    }

    /*
     * Get execute host.
     */
    public String getExecuteHost() {
        return executeHost;
    }

    /*
     * Set execute host.
     */
    public void setExecuteHost(String executeHost) {
        this.executeHost = executeHost;
    }

    /*
     * Get execute port.
     */
    public int getExecutePort() {
        return executePort;
    }

    /*
     * Set execute port.
     */
    public void setExecutePort(int executePort) {
        this.executePort = executePort;
    }

    public void setPropertiesColumnNames(List<String> columnNames) {
        this.propertiesColumnNames = columnNames;
    }
}

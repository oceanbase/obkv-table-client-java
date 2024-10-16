/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

package com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload.impl;

import java.util.Arrays;

import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;

// OB_SERIALIZE_MEMBER_SIMPLE(ObTableDirectLoadBeginArg,
//                            table_name_,
//                            parallel_,
//                            max_error_row_count_,
//                            dup_action_,
//                            timeout_,
//                            heartbeat_timeout_,
//                            force_create_,
//                            async_begin_,
//                            load_method_,
//                            column_names_,
//                            partition_names_                 
// );

public class ObTableDirectLoadBeginArg implements ObSimplePayload {

    private String              tableName;
    private long                parallel         = 0;
    private long                maxErrorRowCount = 0;
    private ObLoadDupActionType dupAction        = ObLoadDupActionType.INVALID_MODE;
    private long                timeout          = 0;
    private long                heartBeatTimeout = 0;
    private boolean             forceCreate      = false;
    private boolean             asyncBegin       = false;
    private String              loadMethod       = new String();
    private String[]            columnNames      = new String[0];
    private String[]            partitionNames   = new String[0];

    public ObTableDirectLoadBeginArg() {
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public long getParallel() {
        return parallel;
    }

    public void setParallel(long parallel) {
        this.parallel = parallel;
    }

    public long getMaxErrorRowCount() {
        return maxErrorRowCount;
    }

    public void setMaxErrorRowCount(long maxErrorRowCount) {
        this.maxErrorRowCount = maxErrorRowCount;
    }

    public ObLoadDupActionType getDupAction() {
        return dupAction;
    }

    public void setDupAction(ObLoadDupActionType dupAction) {
        this.dupAction = dupAction;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public long getHeartBeatTimeout() {
        return heartBeatTimeout;
    }

    public void setHeartBeatTimeout(long heartBeatTimeout) {
        this.heartBeatTimeout = heartBeatTimeout;
    }

    public boolean getForceCreate() {
        return forceCreate;
    }

    public void setForceCreate(boolean forceCreate) {
        this.forceCreate = forceCreate;
    }

    public boolean getAsyncBegin() {
        return asyncBegin;
    }

    public void setAsyncBegin(boolean asyncBegin) {
        this.asyncBegin = asyncBegin;
    }

    public String getLoadMethod() {
        return loadMethod;
    }

    public void setLoadMethod(String loadMethod) {
        this.loadMethod = loadMethod;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
    }

    public String[] getPartitionNames() {
        return partitionNames;
    }

    public void setPartitionNames(String[] partitionNames) {
        this.partitionNames = partitionNames;
    }

    public String toString() {
        return String
            .format(
                "{tableName:%s, parallel:%d, maxErrorRowCount:%d, dupAction:%s, timeout:%d, heartBeatTimeout:%d, forceCreate:%s, asyncBegin:%s, loadMethod:%s, columnNames:%s, partitionNames:%s}",
                tableName, parallel, maxErrorRowCount, dupAction, timeout, heartBeatTimeout,
                forceCreate, asyncBegin, loadMethod, Arrays.toString(columnNames),
                Arrays.toString(partitionNames));
    }

    /**
     * Encode.
     */
    @Override
    public byte[] encode() {
        int needBytes = (int) getEncodedSize();
        ObByteBuf buf = new ObByteBuf(needBytes);
        encode(buf);
        return buf.bytes;
    }

    /**
     * Encode.
     */
    @Override
    public void encode(ObByteBuf buf) {
        Serialization.encodeVString(buf, tableName);
        Serialization.encodeVi64(buf, parallel);
        Serialization.encodeVi64(buf, maxErrorRowCount);
        Serialization.encodeI8(buf, dupAction.getByteValue());
        Serialization.encodeVi64(buf, timeout);
        Serialization.encodeVi64(buf, heartBeatTimeout);
        Serialization.encodeI8(buf, (byte) (forceCreate ? 1 : 0));
        Serialization.encodeI8(buf, (byte) (asyncBegin ? 1 : 0));
        Serialization.encodeVString(buf, loadMethod);
        Serialization.encodeVStringArray(buf, columnNames);
        Serialization.encodeVStringArray(buf, partitionNames);
    }

    /**
     * Decode.
     */
    @Override
    public ObTableDirectLoadBeginArg decode(ByteBuf buf) {
        tableName = Serialization.decodeVString(buf);
        parallel = Serialization.decodeVi64(buf);
        maxErrorRowCount = Serialization.decodeVi64(buf);
        dupAction = ObLoadDupActionType.valueOf(Serialization.decodeI8(buf));
        timeout = Serialization.decodeVi64(buf);
        heartBeatTimeout = Serialization.decodeVi64(buf);
        forceCreate = (Serialization.decodeI8(buf) != 0);
        asyncBegin = (Serialization.decodeI8(buf) != 0);
        loadMethod = Serialization.decodeVString(buf);
        columnNames = Serialization.decodeVStringArray(buf);
        partitionNames = Serialization.decodeVStringArray(buf);
        return this;
    }

    /**
     * Get encoded size.
     */
    @Override
    public int getEncodedSize() {
        int len = 0;
        len += Serialization.getNeedBytes(tableName);
        len += Serialization.getNeedBytes(parallel);
        len += Serialization.getNeedBytes(maxErrorRowCount);
        len += 1; /*dupAction*/
        len += Serialization.getNeedBytes(timeout);
        len += Serialization.getNeedBytes(heartBeatTimeout);
        len += 1; /*forceCreate*/
        len += 1; /*asyncBegin*/
        len += Serialization.getNeedBytes(loadMethod);
        len += Serialization.getNeedBytes(columnNames);
        len += Serialization.getNeedBytes(partitionNames);
        return len;
    }

}

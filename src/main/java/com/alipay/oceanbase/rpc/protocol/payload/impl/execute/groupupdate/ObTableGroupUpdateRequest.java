/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2022 Ant Financial Services Group
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.groupupdate;

import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/**
 *
 OB_SERIALIZE_MEMBER((ObTableGroupUpdateRequest, ObTableOperationRequest),
 group_update_sid_,
 group_phase_,
 req_no_
 );
 *
 * @author xuanchao.xc
 * @since 2022-11-02
 */
public class ObTableGroupUpdateRequest extends ObTableOperationRequest {
    private String  reqNo;
    private String  cmpStr;
    private boolean onlyCheck;

    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE_GROUP_UPDATE;
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode ObTableOperationRequest header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        // 1. encode ObTableOperationRequest payload
        idx = encodeCredential(bytes, idx);
        idx = encodeTableMetaWithPartitionId(bytes, idx);

        int len = (int) tableOperation.getPayloadSize();
        System.arraycopy(tableOperation.encode(), 0, bytes, idx, len);
        idx += len;

        System.arraycopy(Serialization.encodeI8(consistencyLevel.getByteValue()), 0, bytes, idx, 1);
        idx++;

        System.arraycopy(Serialization.encodeI8(returningRowKey ? (byte) 1 : (byte) 0), 0, bytes,
            idx, 1);
        idx++;

        System.arraycopy(Serialization.encodeI8(returningAffectedEntity ? (byte) 1 : (byte) 0), 0,
            bytes, idx, 1);
        idx++;

        System.arraycopy(Serialization.encodeI8(returningAffectedRows ? (byte) 1 : (byte) 0), 0,
            bytes, idx, 1);
        idx++;

        byte[] strbytes = Serialization.encodeVString(reqNo);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        strbytes = Serialization.encodeVString(cmpStr);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        System.arraycopy(Serialization.encodeI8(onlyCheck ? (byte) 1 : (byte) 0), 0, bytes, idx, 1);

        return bytes;
    }

    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);
        this.reqNo = Serialization.decodeVString(buf);
        this.cmpStr = Serialization.decodeVString(buf);
        this.onlyCheck = Serialization.decodeI8(buf) != 0;
        return this;
    }

    @Override
    public long getPayloadContentSize() {
        return super.getPayloadContentSize() + Serialization.getNeedBytes(reqNo)
               + Serialization.getNeedBytes(cmpStr) + 1;
    }

    public String getReqNo() {
        return reqNo;
    }

    public void setReqNo(String reqNo) {
        this.reqNo = reqNo;
    }

    public String getCmpStr() {
        return cmpStr;
    }

    public void setCmpStr(String cmpStr) {
        this.cmpStr = cmpStr;
    }

    public boolean isOnlyCheck() {
        return onlyCheck;
    }

    public void setOnlyCheck(boolean check) {
        this.onlyCheck = check;
    }

    public static ObTableGroupUpdateRequest getInstance(String tableName,
                                                        ObTableOperationType type,
                                                        Object[] rowKeys, String[] columns,
                                                        Object[] properties, long timeout,
                                                        String reqNo) {
        return getInstance(tableName, type, rowKeys, columns, properties, timeout, reqNo, "", false);
    }

    public static ObTableGroupUpdateRequest getInstance(String tableName,
                                                        ObTableOperationType type,
                                                        Object[] rowKeys, String[] columns,
                                                        Object[] properties, long timeout,
                                                        String reqNo, String cmpStr,
                                                        boolean onlyCheck) {
        ObTableGroupUpdateRequest request = new ObTableGroupUpdateRequest();
        request.setTableName(tableName);
        // ms
        request.setTimeout(timeout);
        request.setReturningAffectedRows(true);
        ObTableOperation obTableOperation = ObTableOperation.getInstance(type, rowKeys, columns,
            properties);
        request.setTableOperation(obTableOperation);
        request.setReqNo(reqNo);
        request.setCmpStr(cmpStr);
        request.setOnlyCheck(onlyCheck);

        return request;
    }
}

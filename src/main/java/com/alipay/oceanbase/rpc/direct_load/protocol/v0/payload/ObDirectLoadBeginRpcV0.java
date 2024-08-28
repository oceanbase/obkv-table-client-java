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

package com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadBeginRpc;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload.impl.ObTableDirectLoadBeginArg;
import com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload.impl.ObTableDirectLoadBeginRes;
import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObAddr;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadOperationType;

public class ObDirectLoadBeginRpcV0 extends ObDirectLoadDefaultRpcV0 implements
                                                                    ObDirectLoadBeginRpc {

    public static final ObTableDirectLoadOperationType OP_TYPE = ObTableDirectLoadOperationType.BEGIN;

    private ObTableDirectLoadBeginArg                  arg     = new ObTableDirectLoadBeginArg();
    private ObTableDirectLoadBeginRes                  res     = new ObTableDirectLoadBeginRes();

    public ObDirectLoadBeginRpcV0(ObDirectLoadTraceId traceId) {
        super(traceId);
        // set default value
        arg.setForceCreate(true);
        arg.setAsyncBegin(true);
    }

    @Override
    public ObTableDirectLoadOperationType getOpType() {
        return OP_TYPE;
    }

    @Override
    public ObSimplePayload getArg() {
        return arg;
    }

    @Override
    public ObSimplePayload getRes() {
        return res;
    }

    // arg

    @Override
    public void setTableName(String tableName) {
        arg.setTableName(tableName);
    }

    @Override
    public void setParallel(long parallel) {
        arg.setParallel(parallel);
    }

    @Override
    public void setMaxErrorRowCount(long maxErrorRowCount) {
        arg.setMaxErrorRowCount(maxErrorRowCount);
    }

    @Override
    public void setDupAction(ObLoadDupActionType dupAction) {
        arg.setDupAction(dupAction);
    }

    @Override
    public void setTimeout(long timeout) {
        arg.setTimeout(timeout);
    }

    @Override
    public void setHeartBeatTimeout(long heartBeatTimeout) {
        arg.setHeartBeatTimeout(heartBeatTimeout);
    }

    @Override
    public void setLoadMethod(String loadMethod) {
        arg.setLoadMethod(loadMethod);
    }

    @Override
    public void setColumnNames(String[] columnNames) {
        arg.setColumnNames(columnNames);
    }

    @Override
    public void setPartitionNames(String[] partitionNames) {
        arg.setPartitionNames(partitionNames);
    }

    // res

    @Override
    public ObAddr getSvrAddr() {
        return this.result.getHeader().getAddr();
    }

    @Override
    public long getTableId() {
        return res.getTableId();
    }

    @Override
    public long getTaskId() {
        return res.getTaskId();
    }

    @Override
    public ObTableLoadClientStatus getStatus() {
        return res.getStatus();
    }

    @Override
    public int getErrorCode() {
        return res.getErrorCode();
    }

}
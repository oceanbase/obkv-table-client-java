/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadDetachRpc;
import com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload.impl.ObTableDirectLoadDetachArg;
import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObAddr;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadOperationType;

public class ObDirectLoadDetachRpcV0 extends ObDirectLoadDefaultRpcV0 implements
                                                                     ObDirectLoadDetachRpc {

    public static final ObTableDirectLoadOperationType OP_TYPE = ObTableDirectLoadOperationType.DETACH;

    private ObTableDirectLoadDetachArg                 arg     = new ObTableDirectLoadDetachArg();

    public ObDirectLoadDetachRpcV0(ObDirectLoadTraceId traceId) {
        super(traceId);
        request.setGroupId(22/*OBCG_DIRECT_LOAD_HIGH_PRIO*/);
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
        return null;
    }

    @Override
    public void setSvrAddr(ObAddr addr) {
        request.getHeader().setAddr(addr);
    }

    @Override
    public void setTableId(long tableId) {
        arg.setTableId(tableId);
    }

    @Override
    public void setTaskId(long taskId) {
        arg.setTaskId(taskId);
    }

}

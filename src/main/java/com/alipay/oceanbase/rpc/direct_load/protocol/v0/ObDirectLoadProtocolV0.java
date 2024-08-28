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

package com.alipay.oceanbase.rpc.direct_load.protocol.v0;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.exception.*;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;
import com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload.*;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.*;

public class ObDirectLoadProtocolV0 implements ObDirectLoadProtocol {

    private static final int PROTOCOL_VERSION = 0;

    public ObDirectLoadProtocolV0() {
    }

    @Override
    public void init() throws ObDirectLoadException {
    }

    @Override
    public int getProtocolVersion() {
        return PROTOCOL_VERSION;
    }

    @Override
    public ObDirectLoadBeginRpc getBeginRpc(ObDirectLoadTraceId traceId) {
        return new ObDirectLoadBeginRpcV0(traceId);
    }

    @Override
    public ObDirectLoadCommitRpc getCommitRpc(ObDirectLoadTraceId traceId) {
        return new ObDirectLoadCommitRpcV0(traceId);
    }

    @Override
    public ObDirectLoadAbortRpc getAbortRpc(ObDirectLoadTraceId traceId) {
        return new ObDirectLoadAbortRpcV0(traceId);
    }

    @Override
    public ObDirectLoadGetStatusRpc getGetStatusRpc(ObDirectLoadTraceId traceId) {
        return new ObDirectLoadGetStatusRpcV0(traceId);
    }

    @Override
    public ObDirectLoadInsertRpc getInsertRpc(ObDirectLoadTraceId traceId) {
        return new ObDirectLoadInsertRpcV0(traceId);
    }

    @Override
    public ObDirectLoadHeartBeatRpc getHeartBeatRpc(ObDirectLoadTraceId traceId) {
        return new ObDirectLoadHeartBeatRpcV0(traceId);
    }

}

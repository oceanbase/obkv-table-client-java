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

package com.alipay.oceanbase.rpc.direct_load.protocol;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.*;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;

public interface ObDirectLoadProtocol {

    void init() throws ObDirectLoadException;

    int getProtocolVersion();

    void checkIsSupported(ObDirectLoadStatement statement) throws ObDirectLoadException;

    // rpc
    ObDirectLoadBeginRpc getBeginRpc(ObDirectLoadTraceId traceId);

    ObDirectLoadCommitRpc getCommitRpc(ObDirectLoadTraceId traceId);

    ObDirectLoadAbortRpc getAbortRpc(ObDirectLoadTraceId traceId);

    ObDirectLoadGetStatusRpc getGetStatusRpc(ObDirectLoadTraceId traceId);

    ObDirectLoadInsertRpc getInsertRpc(ObDirectLoadTraceId traceId);

    ObDirectLoadHeartBeatRpc getHeartBeatRpc(ObDirectLoadTraceId traceId);

}

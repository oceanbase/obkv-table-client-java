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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadLogger;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.exception.*;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;
import com.alipay.oceanbase.rpc.direct_load.protocol.v0.payload.*;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.*;

public class ObDirectLoadProtocolV0 implements ObDirectLoadProtocol {

    private static final int         PROTOCOL_VERSION = 0;
    private final ObDirectLoadLogger logger;
    private final long               obVersion;

    public ObDirectLoadProtocolV0(ObDirectLoadTraceId traceId, long obVersion) {
        this.logger = ObDirectLoadLogger.getLogger(traceId);
        this.obVersion = obVersion;
    }

    @Override
    public void init() throws ObDirectLoadException {
    }

    @Override
    public int getProtocolVersion() {
        return PROTOCOL_VERSION;
    }

    @Override
    public void checkIsSupported(ObDirectLoadStatement statement) throws ObDirectLoadException {
        if (obVersion < ObGlobal.OB_VERSION_4_3_2_0) {
            // 432以下不支持inc|inc_replace
            String loadMethod = statement.getLoadMethod();
            if (!loadMethod.isEmpty() && !loadMethod.equalsIgnoreCase("full")) {
                logger.warn("load method in ob version " + ObGlobal.getObVsnString(obVersion)
                            + "is not supported, minimum version required is "
                            + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_3_2_0));
                throw new ObDirectLoadNotSupportedException(
                    "load method in ob version " + ObGlobal.getObVsnString(obVersion)
                            + " is not supported, minimum version required is "
                            + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_3_2_0));
            }
        } else if (obVersion < ObGlobal.OB_VERSION_4_3_5_0
                   && statement.getPartitionNames().length > 0) {
            logger.warn("partition names in ob version " + ObGlobal.getObVsnString(obVersion)
                        + "is not supported, minimum version required is "
                        + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_3_5_0));
            throw new ObDirectLoadNotSupportedException(
                "partition names in ob version " + ObGlobal.getObVsnString(obVersion)
                        + " is not supported, minimum version required is "
                        + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_3_5_0));
        }
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

    @Override
    public ObDirectLoadDetachRpc getDetachRpc(ObDirectLoadTraceId traceId)
                                                                          throws ObDirectLoadException {
        if (obVersion < ObGlobal.OB_VERSION_4_3_0_0) {
            if (obVersion < ObGlobal.OB_VERSION_4_2_5_3) {
                logger.warn("detach in ob version " + ObGlobal.getObVsnString(obVersion)
                            + "is not supported, minimum version required is "
                            + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_2_5_3));
                throw new ObDirectLoadNotSupportedException(
                    "detach in ob version " + ObGlobal.getObVsnString(obVersion)
                            + " is not supported, minimum version required is "
                            + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_2_5_3));
            }
        } else if (obVersion < ObGlobal.OB_VERSION_4_5_0_0) {
            if (obVersion < ObGlobal.OB_VERSION_4_4_2_0) {
                logger.warn("detach in ob version " + ObGlobal.getObVsnString(obVersion)
                            + "is not supported, minimum version required is "
                            + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_4_2_0));
                throw new ObDirectLoadNotSupportedException(
                    "detach in ob version " + ObGlobal.getObVsnString(obVersion)
                            + " is not supported, minimum version required is "
                            + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_4_2_0));
            }
        } else if (obVersion < ObGlobal.OB_VERSION_4_5_1_0) {
            logger.warn("detach in ob version " + ObGlobal.getObVsnString(obVersion)
                        + "is not supported, minimum version required is "
                        + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_5_1_0));
            throw new ObDirectLoadNotSupportedException(
                "detach in ob version " + ObGlobal.getObVsnString(obVersion)
                        + " is not supported, minimum version required is "
                        + ObGlobal.getObVsnString(ObGlobal.OB_VERSION_4_5_1_0));
        }
        return new ObDirectLoadDetachRpcV0(traceId);
    }

}

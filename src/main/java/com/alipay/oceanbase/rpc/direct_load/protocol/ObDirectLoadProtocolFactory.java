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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadLogger;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.exception.*;
import com.alipay.oceanbase.rpc.direct_load.protocol.v0.ObDirectLoadProtocolV0;

public class ObDirectLoadProtocolFactory {

    private static final ObDirectLoadLogger logger = ObDirectLoadLogger.getLogger();

    public static long getSupportedMinimumObVersion(long obVersion) {
        long minimumObVersion = 0;
        if (obVersion < ObGlobal.OB_VERSION_4_2_1_0) { // < 421
            minimumObVersion = ObGlobal.OB_VERSION_4_2_1_7;
        } else if (obVersion < ObGlobal.OB_VERSION_4_2_2_0) { // 421
            minimumObVersion = ObGlobal.OB_VERSION_4_2_1_7;
        } else if (obVersion < ObGlobal.OB_VERSION_4_3_0_0) { // 42x
            minimumObVersion = ObGlobal.OB_VERSION_4_2_4_0;
        } else { // master
            minimumObVersion = ObGlobal.OB_VERSION_4_3_0_1;
        }
        return minimumObVersion;
    }

    public static boolean checkIsSupported(long obVersion) {
        final long minimumObVersion = getSupportedMinimumObVersion(obVersion);
        return (obVersion >= minimumObVersion);
    }

    public static ObDirectLoadProtocol getProtocol(ObDirectLoadTraceId traceId, long obVersion)
                                                                                               throws ObDirectLoadException {
        final long minimumObVersion = getSupportedMinimumObVersion(obVersion);
        if (obVersion < minimumObVersion) {
            logger.warn("direct load in ob version " + ObGlobal.getObVsnString(obVersion)
                        + "is not supported, minimum version required is "
                        + ObGlobal.getObVsnString(minimumObVersion));
            throw new ObDirectLoadNotSupportedException(
                "direct load in ob version " + ObGlobal.getObVsnString(obVersion)
                        + " is not supported, minimum version required is "
                        + ObGlobal.getObVsnString(minimumObVersion));
        }
        return new ObDirectLoadProtocolV0(traceId, obVersion);
    }

}

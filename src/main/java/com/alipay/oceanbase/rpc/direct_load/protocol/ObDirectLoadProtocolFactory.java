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

    private static final ObDirectLoadLogger logger             = ObDirectLoadLogger.getLogger();

    // 起始版本
    // 4_2_1_release
    public static final long                OB_VERSION_4_2_1_0 = ObGlobal.calcVersion(4, (short) 2,
                                                                   (byte) 1, (byte) 0);
    // 4_2_x_release
    public static final long                OB_VERSION_4_2_2_0 = ObGlobal.calcVersion(4, (short) 2,
                                                                   (byte) 2, (byte) 0);
    // master
    public static final long                OB_VERSION_4_3_0_0 = ObGlobal.calcVersion(4, (short) 3,
                                                                   (byte) 0, (byte) 0);

    // 最低支持版本
    // 4_2_1_release
    public static final long                OB_VERSION_4_2_1_7 = ObGlobal.calcVersion(4, (short) 2,
                                                                   (byte) 1, (byte) 7);
    // 4_2_x_release
    public static final long                OB_VERSION_4_2_4_0 = ObGlobal.calcVersion(4, (short) 2,
                                                                   (byte) 4, (byte) 0);
    // master
    public static final long                OB_VERSION_4_3_0_1 = ObGlobal.calcVersion(4, (short) 3,
                                                                   (byte) 0, (byte) 1);

    public static long getSupportedMinimumObVersion(long obVersion) {
        long minimumObVersion = 0;
        if (obVersion < OB_VERSION_4_2_1_0) { // < 421
            minimumObVersion = OB_VERSION_4_2_1_7;
        } else if (obVersion < OB_VERSION_4_2_2_0) { // 421
            minimumObVersion = OB_VERSION_4_2_1_7;
        } else if (obVersion < OB_VERSION_4_3_0_0) { // 42x
            minimumObVersion = OB_VERSION_4_2_4_0;
        } else { // master
            minimumObVersion = OB_VERSION_4_3_0_1;
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

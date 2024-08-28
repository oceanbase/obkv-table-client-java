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

package com.alipay.oceanbase.rpc.direct_load.exception;

import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableTransportException;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;

public class ObDirectLoadExceptionUtil {

    public static ObDirectLoadException convertException(Exception e) {
        if (e instanceof ObDirectLoadException) {
            return (ObDirectLoadException) e;
        }
        // runtime exception
        if (e instanceof NullPointerException) {
            return new ObDirectLoadNullPointerException(e);
        }
        if (e instanceof IllegalArgumentException) {
            return new ObDirectLoadIllegalArgumentException(e);
        }
        if (e instanceof IllegalStateException) {
            return new ObDirectLoadIllegalStateException(e);
        }
        if (e instanceof java.util.concurrent.TimeoutException) {
            return new ObDirectLoadTimeoutException(e);
        }
        if (e instanceof io.netty.handler.timeout.TimeoutException) {
            return new ObDirectLoadTimeoutException(e);
        }
        // interrupted exception
        if (e instanceof InterruptedException) {
            return new ObDirectLoadInterruptedException(e);
        }
        // rpc exception
        if (e instanceof ObTableTransportException) {
            int errorCode = ((ObTableTransportException) e).getErrorCode();
            if (errorCode == TransportCodes.BOLT_TIMEOUT) {
                return new ObDirectLoadRpcTimeoutException(e);
            } else if (errorCode == TransportCodes.BOLT_SEND_FAILED) {
                return new ObDirectLoadRpcSendFailedException(e);
            } else if (errorCode == TransportCodes.BOLT_RESPONSE_NULL) {
                return new ObDirectLoadRpcResponseNullException(e);
            } else if (errorCode == TransportCodes.BOLT_CHECKSUM_ERR) {
                return new ObDirectLoadRpcChecksumErrorException(e);
            } else {
                return new ObDirectLoadRpcException(e);
            }
        }
        // server exception
        if (e instanceof ObTableException) {
            int errorCode = ((ObTableException) e).getErrorCode();
            return new ObDirectLoadServerException(errorCode, e);
        }
        // unknow exception
        return new ObDirectLoadException(e);
    }

    public static ObDirectLoadInterruptedException convertException(InterruptedException e) {
        return new ObDirectLoadInterruptedException(e);
    }

    public static ObDirectLoadServerStatusException convertException(ObTableLoadClientStatus status,
                                                                     int errorCode) {
        ResultCodes resultCodes = ResultCodes.valueOf(errorCode);
        if (resultCodes != null) {
            return new ObDirectLoadServerStatusException(status, resultCodes);
        }
        return new ObDirectLoadServerStatusException(status, errorCode);
    }

}

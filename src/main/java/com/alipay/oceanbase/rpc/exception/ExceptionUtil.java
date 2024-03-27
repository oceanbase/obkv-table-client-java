/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
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

package com.alipay.oceanbase.rpc.exception;

import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;

import java.util.Objects;

import static com.alipay.oceanbase.rpc.protocol.payload.ResultCodes.OB_DESERIALIZE_ERROR;
import static com.alipay.oceanbase.rpc.protocol.payload.ResultCodes.OB_ERR_KV_GLOBAL_INDEX_ROUTE;

public class ExceptionUtil {

    /*
     * throw ObTableException based on error code
     * TODO 错误信息这里应该要加上表名/字段等，但是报错的地方没有表名上下文，比较麻烦
     *
     * @param errorCode error code return from Ob Server
     */
    public static void throwObTableException(int errorCode) {
        if (errorCode != ResultCodes.OB_SUCCESS.errorCode) {
            throw convertToObTableException("", 0, 0, 0, errorCode, "");
        }
    }

    /*
     * throw ObTableException based on error code
     * TODO 错误信息这里应该要加上表名/字段等，但是报错的地方没有表名上下文，比较麻烦
     *
     * @param errorCode error code return from Ob Server
     */
    public static void throwObTableException(String host, int port, long sequence, long uniqueId,
                                             int errorCode, String errorMessage) {
        if (errorCode != ResultCodes.OB_SUCCESS.errorCode) {
            throw convertToObTableException(host, port, sequence, uniqueId, errorCode, errorMessage);
        }
    }

    /*
     * throw ObTableException based on error code
     * TODO 错误信息这里应该要加上表名/字段等，但是报错的地方没有表名上下文，比较麻烦
     *
     * @param resultCodes error code return from Ob Server
     */
    public static ObTableException convertToObTableException(String host, int port, long sequence,
                                                             long uniqueId, int errorCode,
                                                             String errorMessage) {
        String trace = String.format("Y%X-%016X", uniqueId, sequence);
        String server = host + ":" + port;
        String errMsg = Objects.equals(errorMessage, "") ? "error occur in server" : errorMessage;
        ResultCodes resultCodes = ResultCodes.valueOf(errorCode);
        if (resultCodes == null) {
            return new ObTableUnexpectedException("[" + trace + "] [" + "unknown errcode: "
                                                  + errorCode + "] server [" + server + "]",
                errorCode);
        }

        if (resultCodes.errorCode == OB_ERR_KV_GLOBAL_INDEX_ROUTE.errorCode) {
            return new ObTableGlobalIndexRouteException("[" + String.valueOf(resultCodes.errorCode)
                                                        + "]" + "[" + resultCodes.name() + "]"
                                                        + "[" + errMsg + "]" + "[" + server + "]"
                                                        + "[" + trace + "]", resultCodes.errorCode);
        } else {
            // [errCode][errCodeName][errMsg][server][trace]
            return new ObTableException("[" + String.valueOf(resultCodes.errorCode) + "]" + "["
                                        + resultCodes.name() + "]" + "[" + errMsg + "]" + "["
                                        + server + "]" + "[" + trace + "]", resultCodes.errorCode);
        }
    }

    /*
     * throw ObTableTransportException based on transport error code
     * TODO 错误信息这里应该要加上表名/字段等，但是报错的地方没有表名上下文，比较麻烦
     *
     * @param errorCode transport error code return from BOLT framework
     */
    public static void throwObTableTransportException(String message, int errorCode) {
        switch (errorCode) {
            case 0:
                return;
            case TransportCodes.BOLT_TIMEOUT:
            case TransportCodes.BOLT_SEND_FAILED:
            case TransportCodes.BOLT_RESPONSE_NULL:
            case TransportCodes.BOLT_CHECKSUM_ERR:
                throw new ObTableTransportException(message, errorCode);
            default:
                throw new ObTableTransportException("unexpected transport exception: " + errorCode,
                    errorCode);
        }
    }

}

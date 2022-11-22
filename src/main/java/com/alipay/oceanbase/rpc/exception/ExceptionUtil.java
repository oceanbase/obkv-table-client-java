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

import static com.alipay.oceanbase.rpc.protocol.payload.ResultCodes.OB_DESERIALIZE_ERROR;

public class ExceptionUtil {

    /*
     * throw ObTableException based on error code
     * TODO 错误信息这里应该要加上表名/字段等，但是报错的地方没有表名上下文，比较麻烦
     *
     * @param errorCode error code return from Ob Server
     */
    public static void throwObTableException(int errorCode) {
        if (errorCode != ResultCodes.OB_SUCCESS.errorCode) {
            throw convertToObTableException("", 0, 0, 0, errorCode);
        }
    }

    /*
     * throw ObTableException based on error code
     * TODO 错误信息这里应该要加上表名/字段等，但是报错的地方没有表名上下文，比较麻烦
     *
     * @param errorCode error code return from Ob Server
     */
    public static void throwObTableException(String host, int port, long sequence, long uniqueId,
                                             int errorCode) {
        if (errorCode != ResultCodes.OB_SUCCESS.errorCode) {
            throw convertToObTableException(host, port, sequence, uniqueId, errorCode);
        }
    }

    /*
     * throw ObTableException based on error code
     * TODO 错误信息这里应该要加上表名/字段等，但是报错的地方没有表名上下文，比较麻烦
     *
     * @param resultCodes error code return from Ob Server
     */
    public static ObTableException convertToObTableException(String host, int port, long sequence,
                                                             long uniqueId, int errorCode) {
        String trace = String.format("Y%X-%016X", uniqueId, sequence);
        String server = host + ":" + port;
        ResultCodes resultCodes = ResultCodes.valueOf(errorCode);
        if (resultCodes == null) {
            return new ObTableUnexpectedException("[" + trace + "] [" + "unknown errcode: "
                    + errorCode + "] server [" + server + "]", errorCode);
        }
        switch (resultCodes) {
            case OB_ERR_PRIMARY_KEY_DUPLICATE:
                return new ObTableDuplicateKeyException("[" + trace + "] [" + resultCodes.name()
                                                        + "] server [" + server + "] ",
                    resultCodes.errorCode);
            case OB_ERR_UNKNOWN_TABLE:
                return new ObTableNotExistException("[" + trace
                                                    + "] [OB_ERR_UNKNOWN_TABLE] server [" + server
                                                    + "]table not exist", resultCodes.errorCode);
            case OB_ERR_COLUMN_NOT_FOUND:
                return new ObTableException("[" + trace + "] [OB_ERR_COLUMN_NOT_FOUND] server ["
                                            + server + "]column not found", resultCodes.errorCode);
            case OB_OBJ_TYPE_ERROR:
                return new ObTableException("[" + trace + "] [OB_OBJ_TYPE_ERROR] server [" + server
                                            + "]rowkey/column type not match",
                    resultCodes.errorCode);
            case OB_BAD_NULL_ERROR:
                return new ObTableException("[" + trace + "] [OB_BAD_NULL_ERROR] server [" + server
                                            + "]column doesn't have a default value",
                    resultCodes.errorCode);
            case OB_INVALID_ARGUMENT:
                return new ObTableException("[" + trace + "] [OB_INVALID_ARGUMENT] server ["
                                            + server + "]ob table arguments invalid",
                    resultCodes.errorCode);
            case OB_DESERIALIZE_ERROR: // 用户名错误是这个异常码
                return new ObTableAuthException("[" + trace + "] [OB_DESERIALIZE_ERROR] server ["
                                                + server + "]maybe invalid username, errorCode: "
                                                + OB_DESERIALIZE_ERROR, resultCodes.errorCode);
            case OB_PASSWORD_WRONG:
                return new ObTableAuthException("[" + trace + "] [OB_PASSWORD_WRONG] server ["
                                                + server + "]access deny", resultCodes.errorCode);
            case OB_LOCATION_LEADER_NOT_EXIST:
            case OB_NOT_MASTER:
            case OB_RS_NOT_MASTER:
            case OB_RS_SHUTDOWN:
                return new ObTableMasterChangeException("[" + trace + "] [" + resultCodes.name()
                                                        + "] server [" + server + "]",
                    resultCodes.errorCode);
            case OB_RPC_CONNECT_ERROR:
                return new ObTableServerDownException("[" + trace + "] [" + resultCodes.name()
                                                      + "] server [" + server + "]",
                    resultCodes.errorCode);
            case OB_PARTITION_NOT_EXIST:
            case OB_PARTITION_IS_STOPPED:
            case OB_LOCATION_NOT_EXIST:
                return new ObTablePartitionChangeException("[" + trace + "] [" + resultCodes.name()
                                                           + "] server [" + server + "]",
                    resultCodes.errorCode);
            case OB_SERVER_IS_INIT:
            case OB_SERVER_IS_STOPPING:
                return new ObTableServerStatusChangeException("[" + trace + "] ["
                                                              + resultCodes.name() + "] server ["
                                                              + server + "]", resultCodes.errorCode);
            case OB_TENANT_NOT_IN_SERVER:
                return new ObTableUnitMigrateException("[" + trace + "] [" + resultCodes.name()
                                                       + "] server [" + server + "]",
                    resultCodes.errorCode);
            case OB_TRANS_RPC_TIMEOUT:
                return new ObTableTransactionRpcTimeout("[" + trace + "] [" + resultCodes.name()
                                                        + "] server [" + server + "]",
                    resultCodes.errorCode);
            case OB_NO_READABLE_REPLICA:
                return new ObTableNoReadableReplicaException("[" + trace + "] ["
                                                             + resultCodes.name() + "] server ["
                                                             + server + "]", resultCodes.errorCode);
            case OB_REPLICA_NOT_READABLE:
                return new ObTableReplicaNotReadableException("[" + trace + "] ["
                                                              + resultCodes.name() + "] server ["
                                                              + server + "]", resultCodes.errorCode);
            case OB_TIMEOUT:
            case OB_TRANS_TIMEOUT:
            case OB_WAITQUEUE_TIMEOUT:
                return new ObTableServerTimeoutException("[" + trace + "] [" + resultCodes.name()
                                                         + "] server [" + server + "]",
                    resultCodes.errorCode);
            default:
                return new ObTableUnexpectedException("[" + trace + "] [" + resultCodes.name()
                                                      + "] server [" + server + "]",
                    resultCodes.errorCode);
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

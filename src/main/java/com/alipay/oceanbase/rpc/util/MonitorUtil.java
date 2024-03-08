/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

package com.alipay.oceanbase.rpc.util;

import com.alibaba.fastjson.JSON;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateResult;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.MONITOR;
import static com.alipay.oceanbase.rpc.util.TraceUtil.formatTraceMessage;

public class MonitorUtil {
    private static String buildParamsString(List<Object> rowKeys) {
        if (rowKeys == null) {
            return "";
        }
        StringBuilder stringBuilder = new StringBuilder();
        for (Object value : rowKeys) {
            if (value instanceof byte[]) {
                value = new String((byte[]) value);
            }
            if (value instanceof ObVString) {
                value = ((ObVString) value).getStringVal();
            }

            StringBuilder sb = new StringBuilder();
            String str = sb.append(JSON.toJSON(value)).toString();
            if (str.length() > 10) {
                str = str.substring(0, 10);
            }
            stringBuilder.append(str).append("#");
        }

        return stringBuilder.toString();
    }

    /**
     * for QueryAndMutate, i.e. Mutation with Filter
     */
    private static String logMessage(String traceId, String database, String tableName,
                                     String methodName, String type, String endpoint,
                                     List<Object> params, ObTableQueryAndMutateResult result,
                                     long routeTableTime, long executeTime) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(endpoint)) {
            endpoint = endpoint.replaceAll(",", "#");
        }

        String argsValue = buildParamsString(params);

        // TODO: Add error no and change the log message
        String res = String.valueOf(result.getAffectedRows());

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(traceId).append(" - ").append(database).append(",").append(tableName)
            .append(",").append(methodName).append(",").append(type).append(",").append(endpoint)
            .append(",").append(argsValue).append(",").append(res).append(",")
            .append(routeTableTime).append(",").append(executeTime).append(",")
            .append(executeTime + routeTableTime);
        return stringBuilder.toString();
    }

    public static void info(final ObPayload payload, String database, String tableName,
                            String methodName, String type, String endpoint,
                            ObTableQueryAndMutateResult result, ObTableQuery tableQuery,
                            long routeTableTime, long executeTime, long slowQueryMonitorThreshold) {
        if (routeTableTime + executeTime >= slowQueryMonitorThreshold) {
            List<Object> params = new ArrayList<>();
            for (ObNewRange rang : tableQuery.getKeyRanges()) {
                ObRowKey startKey = rang.getStartKey();
                int startKeySize = startKey.getObjs().size();
                ObRowKey endKey = rang.getEndKey();
                int endKeySize = endKey.getObjs().size();
                for (int i = 0; i < startKeySize; i++) {
                    params.add(startKey.getObj(i).getValue());
                }

                for (int i = 0; i < endKeySize; i++) {
                    params.add(endKey.getObj(i).getValue());
                }
            }
            MONITOR.info(logMessage(formatTraceMessage(payload), database, tableName, methodName,
                    type, endpoint, params, result, routeTableTime, executeTime));
        }
    }

    /**
     * for table operation, e.g. insert, update
     */
    private static String logMessage(String traceId, String database, String tableName,
                                     String methodName, String endpoint, Object[] rowKeys,
                                     ObTableOperationResult result, long routeTableTime,
                                     long executeTime) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(endpoint)) {
            endpoint = endpoint.replaceAll(",", "#");
        }
        // if rowkeys is empty point, then append "rowKeys:null" into log message
        String argsValue = (rowKeys == null || rowKeys.length == 0) ? "rowKeys:null"
            : buildParamsString(Arrays.asList(rowKeys));

        ResultCodes resultCode = ResultCodes.valueOf(result.getHeader().getErrno());
        String res = "";
        if (resultCode == ResultCodes.OB_SUCCESS) {
            switch (result.getOperationType()) {
                case GET:
                case INCREMENT:
                case APPEND:
                    res = String.valueOf(result.getEntity().getSimpleProperties().size());
                    break;
                default:
                    res = String.valueOf(result.getAffectedRows());
            }
        }
        String errorCodeStringValue = "UnknownErrorCode";
        if (resultCode != null) {
            errorCodeStringValue = resultCode.toString();
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(traceId).append(" - ").append(database).append(",").append(tableName)
            .append(",").append(methodName).append(",").append(endpoint).append(",")
            .append(argsValue).append(",").append(errorCodeStringValue).append(",").append(res)
            .append(",").append(routeTableTime).append(",").append(executeTime).append(",")
            .append(executeTime + routeTableTime);
        return stringBuilder.toString();
    }

    public static void info(final ObPayload payload, String database, String tableName,
                            String methodName, String endpoint, Object[] rowKeys,
                            ObTableOperationResult result, long routeTableTime, long executeTime,
                            long slowQueryMonitorThreshold) {
        if (routeTableTime + executeTime >= slowQueryMonitorThreshold) {
            MONITOR.info(logMessage(formatTraceMessage(payload), database, tableName, methodName,
                endpoint, rowKeys, result, routeTableTime, executeTime));
        }
    }

    /**
     * for batch operation
     */
    private static String logMessage(String traceId, String database, String tableName,
                                     String methodName, String endpoint, List<Object> rowKeys,
                                     int resultSize, long executeTime) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(endpoint)) {
            endpoint = endpoint.replaceAll(",", "#");
        }
        // if rowkeys is empty point, then append "rowKeys:null" into log message
        String argsValue = (rowKeys == null || rowKeys.isEmpty()) ? "rowKeys:null"
            : buildParamsString(rowKeys);

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(traceId).append(" - ").append(database).append(",").append(tableName)
            .append(",").append(methodName).append(",").append(endpoint).append(",")
            .append(argsValue).append(",").append(resultSize).append(",").append(0).append(",")
            .append(executeTime).append(",").append(executeTime);
        return stringBuilder.toString();
    }

    // for each sub batch opreation
    private static void logMessage0(final ObPayload payload, String database, String tableName, String methodName, String endpoint, ObTableBatchOperation subOperations,
                             long partId, int resultSize, long executeTime, long slowQueryMonitorThreshold) {
        if (executeTime < slowQueryMonitorThreshold) {
            return;
        }
        String traceId = formatTraceMessage(payload);
        List<ObTableOperation> ops = subOperations.getTableOperations();
        for (ObTableOperation op : ops) {
            List<Object> rowKeys = new ArrayList<>();
            ObTableOperationType type = op.getOperationType();
            ObITableEntity entity = op.getEntity();
            if (entity != null) {
                long rowkeySize = entity.getRowKeySize();
                ObRowKey rowKey = entity.getRowKey();
                if (rowKey != null && rowkeySize != 0) {
                    for (int i = 0; i < rowkeySize; i++) {
                        ObObj obObj = entity.getRowKeyValue(i);
                        if (obObj != null) {
                            rowKeys.add(obObj.getValue());
                        }
                    }
                }
            }
            MONITOR.info(logMessage(traceId, database, tableName, methodName+type+"-"+partId, endpoint, rowKeys, resultSize, executeTime));
        }
    }

    // for batch operation result
    private static String logMessage(String traceId, String database, String tableName,
                                     String methodName, String endpoint, int resultSize,
                                     long routeTableTime, long executeTime) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(endpoint)) {
            endpoint = endpoint.replaceAll(",", "#");
        }

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(traceId).append(" - ").append(database).append(",").append(tableName)
            .append(",").append(methodName).append(",").append(endpoint).append(",").append(",")
            .append(resultSize).append(",").append(routeTableTime).append(",").append(executeTime)
            .append(",").append(routeTableTime + executeTime);
        return stringBuilder.toString();
    }

    public static void info(final ObPayload payload, String database, String tableName,
                            String methodName, String endpoint,
                            ObTableBatchOperation subOperations, long partId, int resultSize,
                            long executeTime, long slowQueryMonitorThreshold) {
        logMessage0(payload, database, tableName, methodName, endpoint, subOperations, partId,
            resultSize, executeTime, slowQueryMonitorThreshold);
    }

    public static void info(final ObPayload payload, String database, String tableName,
                            String methodName, String endpoint, int resultSize,
                            long routeTableTime, long executeTime, long slowQueryMonitorThreshold) {
        if (routeTableTime + executeTime >= slowQueryMonitorThreshold) {
            MONITOR.info(logMessage(formatTraceMessage(payload), database, tableName, methodName,
                endpoint, resultSize, routeTableTime, executeTime));
        }
    }

    /**
     * for query
     */
    private static String logMessage(String traceId, String database, String tableName,
                                     String methodName, String endpoint, List<Object> params,
                                     ObTableClientQueryStreamResult result, long routeTableTime,
                                     long executeTime) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(endpoint)) {
            endpoint = endpoint.replaceAll(",", "#");
        }

        String argsValue = buildParamsString(params);

        String res = String.valueOf(result.getCacheRows().size());

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(traceId).append(",").append(database).append(",").append(tableName)
            .append(",").append(methodName).append(",").append(endpoint).append(",")
            .append(argsValue).append(",").append(res).append(",").append(routeTableTime)
            .append(",").append(executeTime).append(",").append(routeTableTime + executeTime);
        return stringBuilder.toString();
    }

    public static void info(final ObPayload payload, String database, String tableName,
                            String methodName, String endpoint, ObTableQuery tableQuery,
                            ObTableClientQueryStreamResult result, long routeTableTime,
                            long executeTime, long slowQueryMonitorThreshold) {
        if (routeTableTime + executeTime >= slowQueryMonitorThreshold) {
            List<Object> params = new ArrayList<>();
            for (ObNewRange rang : tableQuery.getKeyRanges()) {
                ObRowKey startKey = rang.getStartKey();
                int startKeySize = startKey.getObjs().size();
                ObRowKey endKey = rang.getEndKey();
                int endKeySize = endKey.getObjs().size();
                for (int i = 0; i < startKeySize; i++) {
                    params.add(startKey.getObj(i).getValue());
                }

                for (int i = 0; i < endKeySize; i++) {
                    params.add(endKey.getObj(i).getValue());
                }
            }

            MONITOR.info(logMessage(formatTraceMessage(payload), database, tableName, methodName, endpoint,
                params, result, routeTableTime, executeTime));
        }
    }

    /**
     * for tablet op
     */
    private static void logLsOpMessage(final ObPayload payload, String database, String tableName,
                                       String methodName, String endpoint,
                                       ObTableLSOperation lsOperation, int resultSize,
                                       long executeTime, long slowQueryMonitorThreshold) {
        if (executeTime < slowQueryMonitorThreshold) {
            return;
        }
        String traceId = formatTraceMessage(payload);
        MONITOR.info(logMessage(traceId, database, tableName,
            methodName + "-" + lsOperation.getLsId(), endpoint, null, resultSize, executeTime));
    }

    public static void info(final ObPayload payload, String database, String tableName,
                            String methodName, String endpoint, ObTableLSOperation lsOperation,
                            int resultSize, long executeTime, long slowQueryMonitorThreshold) {
        logLsOpMessage(payload, database, tableName, methodName, endpoint, lsOperation, resultSize,
            executeTime, slowQueryMonitorThreshold);
    }
}

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

package com.alipay.oceanbase.rpc.direct_load.execution;

import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadRuntimeInfo;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadExceptionUtil;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadRpcException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadServerException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadServerStatusException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadTimeoutException;
import com.alipay.oceanbase.rpc.direct_load.future.ObDirectLoadStatementAsyncPromiseTask;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadBeginRpc;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadGetStatusRpc;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.direct_load.util.ObDirectLoadIntervalUtil;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.table.ObTable;

public class ObDirectLoadStatementBeginTask extends ObDirectLoadStatementAsyncPromiseTask {

    private final ObDirectLoadConnection                   connection;
    private final ObDirectLoadProtocol                     protocol;
    private final ObDirectLoadStatementExecutor            executor;
    private final ObDirectLoadStatementExecutor.BeginProxy proxy;

    private static final int                               STATE_NONE                = 0;
    private static final int                               STATE_SEND_BEGIN          = 1;
    private static final int                               STATE_WAIT_STATUS_RUNNING = 2;
    private static final int                               STATE_SUCC                = 3;
    private static final int                               STATE_FAIL                = 4;

    private int                                            state                     = STATE_NONE;
    private int                                            retryCount                = 0;
    private int                                            rebeginCount              = 0;
    private ObDirectLoadIntervalUtil                       intervalUtil              = new ObDirectLoadIntervalUtil();
    private ObDirectLoadRuntimeInfo                        runtimeInfo               = new ObDirectLoadRuntimeInfo();

    public ObDirectLoadStatementBeginTask(ObDirectLoadStatement statement,
                                          ObDirectLoadStatementExecutor executor) {
        super(statement);
        this.connection = statement.getConnection();
        this.protocol = connection.getProtocol();
        this.executor = executor;
        this.proxy = executor.getBeginProxy();
    }

    @Override
    public ObDirectLoadRuntimeInfo getRuntimeInfo() {
        return runtimeInfo;
    }

    @Override
    public void run() {
        runtimeInfo.incScheduledCount();
        runtimeInfo.setLastScheduledTime(System.currentTimeMillis());
        try {
            proxy.checkStatus();
            if (state == STATE_NONE) {
                state = STATE_SEND_BEGIN;
            }
            if (state == STATE_SEND_BEGIN) {
                sendBegin();
            }
            if (state == STATE_WAIT_STATUS_RUNNING) {
                waitStatusRunning();
            }
            if (state == STATE_SUCC) {
                proxy.setSuccess();
                setSuccess();
            }
        } catch (ObDirectLoadException e) {
            logger.warn("statement begin task run failed", e);
            state = STATE_FAIL;
            proxy.setFailure(e);
            setFailure(e);
        }
    }

    private static boolean isRetryCode(int errorCode) {
        return (errorCode == ResultCodes.OB_ENTRY_NOT_EXIST.errorCode // -4018
                || errorCode == ResultCodes.OB_SIZE_OVERFLOW.errorCode // -4019
                || errorCode == ResultCodes.OB_EAGAIN.errorCode // -4023
                || errorCode == ResultCodes.OB_NOT_MASTER.errorCode // -4038
                || errorCode == ResultCodes.OB_OP_NOT_ALLOW.errorCode // -4179
                || errorCode == ResultCodes.OB_LS_NOT_EXIST.errorCode // -4719
                || errorCode == ResultCodes.OB_TABLET_NOT_EXIST.errorCode // -4725
                || errorCode == ResultCodes.OB_SCHEMA_EAGAIN.errorCode // -5627
        || errorCode == ResultCodes.OB_ERR_PARALLEL_DDL_CONFLICT.errorCode // -5827
        );
    }

    private boolean canRetry(ObDirectLoadException e) {
        boolean bResult = false;
        if (e instanceof ObDirectLoadRpcException) {
            // 只对异步begin的rpc超时重试, 同步begin重试大概率还是超时
            bResult = (e instanceof ObDirectLoadTimeoutException);
        } else if (e instanceof ObDirectLoadServerException) {
            final int errorCode = ((ObDirectLoadServerException) e).getErrorCode();
            bResult = isRetryCode(errorCode);
        } else if (e instanceof ObDirectLoadServerStatusException) {
            final int errorCode = ((ObDirectLoadServerStatusException) e).getErrorCode();
            bResult = isRetryCode(errorCode);
        }
        // TODO: 细化可重试异常
        return bResult;
    }

    private void sendBegin() throws ObDirectLoadException {
        try {
            ObDirectLoadBeginRpc rpc = null;
            try {
                rpc = doSendBeginRpc();
            } catch (ObDirectLoadException e) {
                logger.warn("statement send begin rpc failed", e);
                throw e;
            }
            ObTableLoadClientStatus status = rpc.getStatus();
            int errorCode = rpc.getErrorCode();
            switch (status) {
                case INITIALIZING:
                case WAITTING:
                    proxy.setSuccess0(rpc.getSvrAddr(), rpc.getTableId(), rpc.getTaskId());
                    state = STATE_WAIT_STATUS_RUNNING;
                    break;
                case RUNNING:
                    proxy.setSuccess0(rpc.getSvrAddr(), rpc.getTableId(), rpc.getTaskId());
                    logger.info("statement server status reach running");
                    state = STATE_SUCC;
                    break;
                case ERROR:
                    logger.warn("statement server status is error, errorCode:" + errorCode);
                    throw ObDirectLoadExceptionUtil.convertException(status, errorCode);
                case ABORT:
                    logger.warn("statement server status is abort, errorCode:" + errorCode);
                    if (errorCode == ResultCodes.OB_SUCCESS.errorCode) {
                        errorCode = ResultCodes.OB_CANCELED.errorCode;
                    }
                    throw ObDirectLoadExceptionUtil.convertException(status, errorCode);
                default:
                    logger.warn("statement server status is unexpected, status:" + status);
                    throw ObDirectLoadExceptionUtil.convertException(status, errorCode);
            }
        } catch (ObDirectLoadException e) {
            if (canRetry(e)) {
                ++retryCount;
                logger.info("statement retry send begin rpc after 1s, retryCount:" + retryCount);
                // retry after 1s
                schedule(1000, TimeUnit.MILLISECONDS);
                return;
            }
            throw e;
        }
    }

    private ObDirectLoadBeginRpc doSendBeginRpc() throws ObDirectLoadException {
        final ObTable table = statement.getObTablePool().getControlObTable();
        final long timeoutMillis = statement.getTimeoutRemain();

        ObDirectLoadBeginRpc rpc = protocol.getBeginRpc(statement.getTraceId());
        rpc.setTableName(statement.getTableName());
        rpc.setParallel(statement.getParallel());
        rpc.setMaxErrorRowCount(statement.getMaxErrorRowCount());
        rpc.setDupAction(statement.getDupAction());
        rpc.setTimeout(statement.getQueryTimeout() * 1000);
        rpc.setHeartBeatTimeout(connection.getHeartBeatTimeout() * 1000);
        rpc.setLoadMethod(statement.getLoadMethod());
        rpc.setColumnNames(statement.getColumnNames());
        rpc.setPartitionNames(statement.getPartitionNames());

        logger.info("statement send begin rpc, arg:" + rpc.getArg());
        connection.executeWithConnection(rpc, table, timeoutMillis);
        logger.info("statement begin rpc response successful, svrAddr:" + rpc.getSvrAddr()
                    + ", res:" + rpc.getRes());

        return rpc;
    }

    private void waitStatusRunning() throws ObDirectLoadException {
        try {
            ObDirectLoadGetStatusRpc rpc = null;
            try {
                rpc = doSendGetStatus();
            } catch (ObDirectLoadException e) {
                logger.warn("statement send get status rpc failed", e);
                throw e;
            }
            ObTableLoadClientStatus status = rpc.getStatus();
            int errorCode = rpc.getErrorCode();
            switch (status) {
                case INITIALIZING:
                case WAITTING:
                    if (intervalUtil.reachTimeInterval(10000)) { // 每隔10s打印一次
                        logger.info("statement waiting server status reach running, status:"
                                    + status);
                    }
                    // retry after 500ms
                    schedule(500, TimeUnit.MILLISECONDS);
                    break;
                case RUNNING:
                    logger.info("statement server status reach running");
                    state = STATE_SUCC;
                    break;
                case ERROR:
                    logger.warn("statement server status is error, errorCode:" + errorCode);
                    throw ObDirectLoadExceptionUtil.convertException(status, errorCode);
                case ABORT:
                    logger.warn("statement server status is abort, errorCode:" + errorCode);
                    if (errorCode == ResultCodes.OB_SUCCESS.errorCode) {
                        errorCode = ResultCodes.OB_CANCELED.errorCode;
                    }
                    throw ObDirectLoadExceptionUtil.convertException(status, errorCode);
                default:
                    logger.warn("statement server status is unexpected, status:" + status);
                    throw ObDirectLoadExceptionUtil.convertException(status, errorCode);
            }
        } catch (ObDirectLoadException e) {
            if (e instanceof ObDirectLoadServerStatusException) {
                if (canRetry(e)) {
                    ++rebeginCount;
                    retryCount = 0;
                    logger.info("statement retry begin after 1s, rebeginCount:" + rebeginCount);
                    proxy.clear();
                    state = STATE_NONE;
                    schedule(1000, TimeUnit.MILLISECONDS);
                    return;
                }
                throw e;
            }
            // retry after 500ms
            schedule(500, TimeUnit.MILLISECONDS);
        }
    }

    private ObDirectLoadGetStatusRpc doSendGetStatus() throws ObDirectLoadException {
        final ObTable table = statement.getObTablePool().getControlObTable();
        final long timeoutMillis = statement.getTimeoutRemain();

        ObDirectLoadGetStatusRpc rpc = protocol.getGetStatusRpc(executor.getTraceId());
        rpc.setSvrAddr(executor.getSvrAddr());
        rpc.setTableId(executor.getTableId());
        rpc.setTaskId(executor.getTaskId());

        logger.debug("statement send get status rpc, arg:" + rpc.getArg());
        connection.executeWithConnection(rpc, table, timeoutMillis);
        logger.debug("statement get status rpc response successful, res:" + rpc.getRes());

        return rpc;
    }

}

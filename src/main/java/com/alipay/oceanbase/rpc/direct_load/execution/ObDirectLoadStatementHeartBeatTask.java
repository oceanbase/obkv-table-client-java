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
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadExceptionUtil;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadRpcException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadRpcTimeoutException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadServerException;
import com.alipay.oceanbase.rpc.direct_load.future.ObDirectLoadStatementAsyncPromiseTask;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadGetStatusRpc;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadHeartBeatRpc;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.table.ObTable;

public class ObDirectLoadStatementHeartBeatTask extends ObDirectLoadStatementAsyncPromiseTask {

    private final ObDirectLoadConnection                       connection;
    private final ObDirectLoadProtocol                         protocol;
    private final ObDirectLoadStatementExecutor                executor;
    private final ObDirectLoadStatementExecutor.HeartBeatProxy proxy;

    private boolean                                            isRunning = false;
    private boolean                                            isCancel  = false;

    public ObDirectLoadStatementHeartBeatTask(ObDirectLoadStatement statement,
                                              ObDirectLoadStatementExecutor executor) {
        super(statement);
        this.connection = statement.getConnection();
        this.protocol = connection.getProtocol();
        this.executor = executor;
        this.proxy = executor.getHeartBeatProxy();
    }

    public synchronized boolean cancel() {
        isCancel = true;
        return !isRunning;
    }

    @Override
    public void run() {
        try {
            synchronized (this) {
                isRunning = true;
                if (isCancel) {
                    setSuccess();
                    return;
                }
            }
            proxy.checkStatus();
            sendHeartBeat();
            synchronized (this) {
                if (isCancel) {
                    setSuccess();
                    return;
                }
            }
        } catch (ObDirectLoadException e) {
            logger.warn("statement heart beat task run failed", e);
            proxy.setFailure(e);
            setFailure(e);
        } finally {
            synchronized (this) {
                isRunning = false;
            }
        }
    }

    private void sendHeartBeat() throws ObDirectLoadException {
        try {
            ObTableLoadClientStatus status = ObTableLoadClientStatus.MAX_STATUS;
            int errorCode = ResultCodes.OB_SUCCESS.errorCode;
            try {
                ObDirectLoadHeartBeatRpc rpc = doSendHeartBeat();
                status = rpc.getStatus();
                errorCode = rpc.getErrorCode();
            } catch (ObDirectLoadException e) {
                logger.warn("statement send heart beat rpc failed", e);
                boolean sendGetStatus = false;
                if (e instanceof ObDirectLoadServerException) {
                    final int ret = ((ObDirectLoadServerException) e).getErrorCode();
                    if (ret == ResultCodes.OB_ENTRY_NOT_EXIST.errorCode) {
                        // 如果心跳找不到任务, 则通过get status获取错误码
                        sendGetStatus = true;
                        try {
                            ObDirectLoadGetStatusRpc rpc2 = doSendGetStatus();
                            status = rpc2.getStatus();
                            errorCode = rpc2.getErrorCode();
                        } catch (ObDirectLoadException e2) {
                            logger.warn("statement send get status rpc failed", e2);
                            throw e2;
                        }
                    }
                }
                if (!sendGetStatus) {
                    throw e;
                }
            }
            switch (status) {
                case INITIALIZING:
                case WAITTING:
                case RUNNING:
                case COMMITTING:
                case COMMIT:
                    schedule(connection.getHeartBeatInterval(), TimeUnit.MILLISECONDS);
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
            logger.warn("statement send heart beat failed", e);
            boolean canRetry = false;
            if (e instanceof ObDirectLoadRpcException) {
                if (e instanceof ObDirectLoadRpcTimeoutException) {
                    canRetry = true;
                    schedule(500, TimeUnit.MILLISECONDS);
                }
            }
            if (!canRetry) {
                throw e;
            }
        }
    }

    private ObDirectLoadHeartBeatRpc doSendHeartBeat() throws ObDirectLoadException {
        final ObTable table = statement.getObTablePool().getHighPrioObTable();
        final long timeoutMillis = statement.getTimeoutRemain();

        ObDirectLoadHeartBeatRpc rpc = protocol.getHeartBeatRpc(executor.getTraceId());
        rpc.setSvrAddr(executor.getSvrAddr());
        rpc.setTableId(executor.getTableId());
        rpc.setTaskId(executor.getTaskId());

        logger.info("statement send heart beat rpc, arg:" + rpc.getArg());
        connection.executeWithConnection(rpc, table, timeoutMillis);
        logger.info("statement heart beat rpc response successful, res:" + rpc.getRes());

        return rpc;
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

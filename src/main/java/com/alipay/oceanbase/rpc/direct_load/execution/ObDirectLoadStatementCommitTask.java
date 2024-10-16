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
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadServerStatusException;
import com.alipay.oceanbase.rpc.direct_load.future.ObDirectLoadStatementAsyncPromiseTask;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadCommitRpc;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadGetStatusRpc;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.direct_load.util.ObDirectLoadIntervalUtil;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.table.ObTable;

public class ObDirectLoadStatementCommitTask extends ObDirectLoadStatementAsyncPromiseTask {

    private final ObDirectLoadConnection                    connection;
    private final ObDirectLoadProtocol                      protocol;
    private final ObDirectLoadStatementExecutor             executor;
    private final ObDirectLoadStatementExecutor.CommitProxy proxy;

    private static final int                                STATE_NONE               = 0;
    private static final int                                STATE_SEND_COMMIT        = 1;
    private static final int                                STATE_WAIT_STATUS_COMMIT = 2;
    private static final int                                STATE_SUCC               = 3;
    private static final int                                STATE_FAIL               = 4;

    private int                                             state                    = STATE_NONE;
    private ObDirectLoadIntervalUtil                        intervalUtil             = new ObDirectLoadIntervalUtil();
    private ObDirectLoadRuntimeInfo                         runtimeInfo              = new ObDirectLoadRuntimeInfo();

    public ObDirectLoadStatementCommitTask(ObDirectLoadStatement statement,
                                           ObDirectLoadStatementExecutor executor) {
        super(statement);
        this.connection = statement.getConnection();
        this.protocol = connection.getProtocol();
        this.executor = executor;
        this.proxy = executor.getCommitProxy();
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
                state = STATE_SEND_COMMIT;
            }
            if (state == STATE_SEND_COMMIT) {
                sendCommit();
            }
            if (state == STATE_WAIT_STATUS_COMMIT) {
                waitStatusCommit();
            }
            if (state == STATE_SUCC) {
                proxy.setSuccess();
                setSuccess();
            }
        } catch (ObDirectLoadException e) {
            logger.warn("statement commit task run failed", e);
            state = STATE_FAIL;
            proxy.setFailure(e);
            setFailure(e);
        }
    }

    private void sendCommit() throws ObDirectLoadException {
        try {
            doSendCommit();
            state = STATE_WAIT_STATUS_COMMIT;
        } catch (ObDirectLoadException e) {
            logger.warn("statement send commit rpc failed", e);
            throw e;
        }
    }

    private ObDirectLoadCommitRpc doSendCommit() throws ObDirectLoadException {
        final ObTable table = statement.getObTablePool().getControlObTable();
        final long timeoutMillis = statement.getTimeoutRemain();

        ObDirectLoadCommitRpc rpc = protocol.getCommitRpc(executor.getTraceId());
        rpc.setSvrAddr(executor.getSvrAddr());
        rpc.setTableId(executor.getTableId());
        rpc.setTaskId(executor.getTaskId());

        logger.info("statement send commit rpc, arg:" + rpc.getArg());
        connection.executeWithConnection(rpc, table, timeoutMillis);
        logger.info("statement commit rpc response successful, res:" + rpc.getRes());

        return rpc;
    }

    private void waitStatusCommit() throws ObDirectLoadException {
        try {
            ObDirectLoadGetStatusRpc rpc = null;
            try {
                rpc = doGetStatus();
            } catch (ObDirectLoadException e) {
                logger.warn("statement send get status rpc failed", e);
                throw e;
            }
            ObTableLoadClientStatus status = rpc.getStatus();
            int errorCode = rpc.getErrorCode();
            switch (status) {
                case COMMITTING:
                    if (intervalUtil.reachTimeInterval(10000)) { // 每隔1s打印一次
                        logger.info("statement waiting server status reach commit, status:"
                                    + status);
                    }
                    // retry after 500ms
                    schedule(500, TimeUnit.MILLISECONDS);
                    break;
                case COMMIT:
                    logger.info("statement server status reach commit");
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
                throw e;
            }
            // retry after 500ms
            schedule(500, TimeUnit.MILLISECONDS);
        }
    }

    private ObDirectLoadGetStatusRpc doGetStatus() throws ObDirectLoadException {
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

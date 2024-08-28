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
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadServerException;
import com.alipay.oceanbase.rpc.direct_load.future.ObDirectLoadStatementAsyncPromiseTask;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadAbortRpc;
import com.alipay.oceanbase.rpc.direct_load.util.ObDirectLoadIntervalUtil;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.table.ObTable;

public class ObDirectLoadStatementAbortTask extends ObDirectLoadStatementAsyncPromiseTask {

    private final ObDirectLoadConnection        connection;
    private final ObDirectLoadProtocol          protocol;
    private final ObDirectLoadStatementExecutor executor;

    private static final int                    STATE_NONE       = 0;
    private static final int                    STATE_SEND_ABORT = 1;
    private static final int                    STATE_SUCC       = 3;
    private static final int                    STATE_FAIL       = 4;

    private int                                 state            = STATE_NONE;
    private ObDirectLoadIntervalUtil            intervalUtil     = new ObDirectLoadIntervalUtil();

    public ObDirectLoadStatementAbortTask(ObDirectLoadStatement statement,
                                          ObDirectLoadStatementExecutor executor) {
        super(statement);
        this.connection = statement.getConnection();
        this.protocol = connection.getProtocol();
        this.executor = executor;
    }

    @Override
    public void run() {
        try {
            if (state == STATE_NONE) {
                state = STATE_SEND_ABORT;
            }
            if (state == STATE_SEND_ABORT) {
                sendAbort();
            }
            if (state == STATE_SUCC) {
                setSuccess();
            }
        } catch (ObDirectLoadException e) {
            logger.warn("statement abort task run failed", e);
            state = STATE_FAIL;
            setFailure(e);
        }
    }

    private void sendAbort() throws ObDirectLoadException {
        try {
            doSendAbort();
            if (intervalUtil.reachTimeInterval(10 * 1000)) {
                logger.info("statement waiting abort");
            }
            schedule(500, TimeUnit.MILLISECONDS);
        } catch (ObDirectLoadException e) {
            if (e instanceof ObDirectLoadServerException) {
                final int errorCode = ((ObDirectLoadServerException) e).getErrorCode();
                if (errorCode == ResultCodes.OB_ENTRY_NOT_EXIST.errorCode) {
                    logger.info("statement is aborted");
                    state = STATE_SUCC;
                    return;
                }
            }
            throw e;
        }

    }

    private ObDirectLoadAbortRpc doSendAbort() throws ObDirectLoadException {
        final ObTable table = statement.getObTablePool().getControlObTable();
        final long timeoutMillis = statement.getTimeoutRemain();

        ObDirectLoadAbortRpc rpc = protocol.getAbortRpc(executor.getTraceId());
        rpc.setSvrAddr(executor.getSvrAddr());
        rpc.setTableId(executor.getTableId());
        rpc.setTaskId(executor.getTaskId());

        logger.debug("statement send abort rpc");
        connection.executeWithConnection(rpc, table, timeoutMillis);
        logger.debug("statement abort rpc response successful");

        return rpc;
    }

}

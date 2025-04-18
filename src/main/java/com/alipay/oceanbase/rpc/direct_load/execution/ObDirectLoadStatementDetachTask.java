/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.future.ObDirectLoadStatementPromiseTask;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadDetachRpc;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;

public class ObDirectLoadStatementDetachTask extends ObDirectLoadStatementPromiseTask {

    private final ObDirectLoadConnection        connection;
    private final ObDirectLoadProtocol          protocol;
    private final ObDirectLoadStatementExecutor executor;

    public ObDirectLoadStatementDetachTask(ObDirectLoadStatement statement,
                                           ObDirectLoadStatementExecutor executor) {
        super(statement);
        this.connection = statement.getConnection();
        this.protocol = connection.getProtocol();
        this.executor = executor;
    }

    @Override
    public void run() {
        try {
            doSendDetach();
            setSuccess();
        } catch (ObDirectLoadException e) {
            logger.warn("statement detach task run failed", e);
            setFailure(e);
        }
    }

    private ObDirectLoadDetachRpc doSendDetach() throws ObDirectLoadException {
        final ObTable table = statement.getObTablePool().getControlObTable();
        final long timeoutMillis = statement.getTimeoutRemain();

        ObDirectLoadDetachRpc rpc = protocol.getDetachRpc(executor.getTraceId());
        rpc.setSvrAddr(executor.getSvrAddr());
        rpc.setTableId(executor.getTableId());
        rpc.setTaskId(executor.getTaskId());

        logger.debug("statement send detach rpc");
        connection.executeWithConnection(rpc, table, timeoutMillis);
        logger.debug("statement detach rpc response successful");

        return rpc;
    }

}

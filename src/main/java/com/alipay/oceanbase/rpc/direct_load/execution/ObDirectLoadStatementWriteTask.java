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

import java.util.List;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadExceptionUtil;
import com.alipay.oceanbase.rpc.direct_load.future.ObDirectLoadStatementPromiseTask;
import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObDirectLoadInsertRpc;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;
import com.alipay.oceanbase.rpc.util.ObByteBuf;

public class ObDirectLoadStatementWriteTask extends ObDirectLoadStatementPromiseTask {

    private final ObDirectLoadConnection                   connection;
    private final ObDirectLoadProtocol                     protocol;
    private final ObDirectLoadStatementExecutor            executor;
    private final ObDirectLoadStatementExecutor.WriteProxy proxy;
    private final ObDirectLoadBucket                       bucket;

    public ObDirectLoadStatementWriteTask(ObDirectLoadStatement statement,
                                          ObDirectLoadStatementExecutor executor,
                                          ObDirectLoadBucket bucket) {
        super(statement);
        this.connection = statement.getConnection();
        this.protocol = connection.getProtocol();
        this.executor = executor;
        this.proxy = executor.getWriteProxy();
        this.bucket = bucket;
    }

    @Override
    public void run() {
        ObTable table = null;
        try {
            List<ObByteBuf> payloadBuffers = bucket.getPayloadBufferList();
            int index = 0;
            table = statement.getObTablePool().takeWriteObTable(statement.getTimeoutRemain());
            while (index < payloadBuffers.size()) {
                ObByteBuf payloadBuffer = payloadBuffers.get(index);
                sendInsert(table, payloadBuffer);
                ++index;
            }
            proxy.setSuccess();
            setSuccess();
        } catch (ObDirectLoadException e) {
            logger.warn("statement write task run failed", e);
            proxy.setFailure(e);
            setFailure(e);
        } finally {
            if (table != null) {
                statement.getObTablePool().putWriteObTable(table);
                table = null;
            }
        }
    }

    private void sendInsert(ObTable table, ObByteBuf payloadBuffer) throws ObDirectLoadException {
        int retryCount = 0;
        int retryInterval = 1;
        while (true) {
            proxy.checkStatus();
            try {
                final long timeoutMillis = statement.getTimeoutRemain();
                doSendInsert(table, payloadBuffer, timeoutMillis);
                break;
            } catch (ObDirectLoadException e) {
                logger.warn("statement send insert failed, retry after " + retryInterval
                            + "s, retryCount:" + retryCount, e);
                // 忽略所有发送失败错误码, 重试到任务状态为FAIL
                ++retryCount;
                try {
                    Thread.sleep(retryInterval * 1000);
                    if (retryInterval == 1) {
                        retryInterval = 2;
                    } else {
                        retryInterval = Math.min(retryInterval * retryInterval, 60); // 最大60s
                    }
                } catch (Exception ex) {
                    throw ObDirectLoadExceptionUtil.convertException(ex);
                }
            }
        }
    }

    private ObDirectLoadInsertRpc doSendInsert(ObTable table, ObByteBuf payloadBuffer,
                                               long timeoutMillis) throws ObDirectLoadException {
        // send insert rpc
        ObDirectLoadInsertRpc rpc = protocol.getInsertRpc(executor.getTraceId());
        rpc.setSvrAddr(executor.getSvrAddr());
        rpc.setTableId(executor.getTableId());
        rpc.setTaskId(executor.getTaskId());
        rpc.setPayloadBuffer(payloadBuffer);

        logger.debug("statement send insert rpc, arg:" + rpc.getArg());
        connection.executeWithConnection(rpc, table, timeoutMillis);
        logger.debug("statement insert rpc response successful, res:" + rpc.getRes());

        return rpc;
    }

}

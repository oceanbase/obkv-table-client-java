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

package com.alipay.oceanbase.rpc.table;

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObAddr;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadAbortArg;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadBeginArg;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadBeginRes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadCommitArg;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadGetStatusArg;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadGetStatusRes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadInsertArg;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadHeartBeatArg;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadHeartBeatRes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.util.ObBytesString;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Timer;
import java.util.TimerTask;

public class ObTableDirectLoad {

    private ObTable                 table       = null;
    private String                  tableName;
    private ObDirectLoadParameter   parameter   = null;
    private boolean                 forceCreate = false;

    private long                    tableId     = 0;
    private long                    taskId      = 0;
    private String[]                columnNames = new String[0];
    private ObTableLoadClientStatus status      = ObTableLoadClientStatus.MAX_STATUS;
    private ResultCodes             errorCode   = ResultCodes.OB_SUCCESS;
    private ObAddr                  srvAddr     = null;

    private Timer                   timer;

    public ObTableDirectLoad(ObTable table, String tableName, ObDirectLoadParameter parameter,
                             boolean forceCreate) {
        if (ObGlobal.OB_VERSION < ObGlobal.OB_VERSION_4_2_1_0)
            throw new ObTableException("not supported ob version " + ObGlobal.obVsnString(),
                ResultCodes.OB_NOT_SUPPORTED.errorCode);
        this.table = table;
        this.tableName = tableName;
        this.parameter = parameter;
        this.forceCreate = forceCreate;
    }

    public ObTable getTable() {
        return table;
    }

    public String getTableName() {
        return tableName;
    }

    public ObDirectLoadParameter getParameter() {
        return parameter;
    }

    public long getTableId() {
        return tableId;
    }

    public long getTaskId() {
        return taskId;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public boolean isAvailable() {
        return status != ObTableLoadClientStatus.MAX_STATUS;
    }

    public boolean isRunning() {
        return status == ObTableLoadClientStatus.RUNNING;
    }

    public boolean isCommitting() {
        return status == ObTableLoadClientStatus.COMMITTING;
    }

    public boolean isCommit() {
        return status == ObTableLoadClientStatus.COMMIT;
    }

    public boolean isError() {
        return status == ObTableLoadClientStatus.ERROR;
    }

    public boolean isAbort() {
        return status == ObTableLoadClientStatus.ABORT;
    }

    public ResultCodes getErrorCode() {
        return errorCode;
    }

    public ObAddr getSrvAddr() {
        return srvAddr;
    }

    public String toString() {
        String str = String.format("{table_id:%d, task_id:%d, column_names:[", tableId, taskId);
        for (int i = 0; i < columnNames.length; ++i) {
            if (i > 0) {
                str += ",";
            }
            str += columnNames[i];
        }
        str += String
            .format("], status:%s, error_code:%d}", status.toString(), errorCode.errorCode);
        return str;
    }

    public void begin() throws Exception {
        if (status != ObTableLoadClientStatus.MAX_STATUS) {
            throw new ObTableException("unexpected status to begin, status:" + status.toString(),
                ResultCodes.OB_STATE_NOT_MATCH.errorCode);
        }
        ObTableDirectLoadBeginArg arg = new ObTableDirectLoadBeginArg();
        ObTableDirectLoadBeginRes res = new ObTableDirectLoadBeginRes();
        arg.setTableName(tableName);
        arg.setParallel(parameter.getParallel());
        arg.setMaxErrorRowCount(parameter.getMaxErrorRowCount());
        arg.setDupAction(parameter.getDupAction());
        arg.setTimeout(parameter.getTimeout());
        arg.setHeartBeatTimeout(parameter.getHeartBeatTimeout());
        arg.setForceCreate(forceCreate);
        execute(ObTableDirectLoadOperationType.BEGIN, arg, res);
        tableId = res.getTableId();
        taskId = res.getTaskId();
        columnNames = res.getColumnNames();
        status = res.getStatus();
        errorCode = res.getErrorCode();
        startHeartBeat();
    }

    public void commit() throws Exception {
        if (!isRunning()) {
            throw new ObTableException("unexpected status to commit, status:" + status.toString(),
                ResultCodes.OB_STATE_NOT_MATCH.errorCode);
        }
        ObTableDirectLoadCommitArg arg = new ObTableDirectLoadCommitArg();
        arg.setTableId(tableId);
        arg.setTaskId(taskId);
        execute(ObTableDirectLoadOperationType.COMMIT, arg);
    }

    public void abort() throws Exception {
        if (status == ObTableLoadClientStatus.MAX_STATUS) {
            throw new ObTableException("unexpected status to abort, status:" + status.toString(),
                ResultCodes.OB_STATE_NOT_MATCH.errorCode);
        }
        if (isAbort()) {
            return;
        }
        ObTableDirectLoadAbortArg arg = new ObTableDirectLoadAbortArg();
        arg.setTableId(tableId);
        arg.setTaskId(taskId);
        execute(ObTableDirectLoadOperationType.ABORT, arg);
        stopHeartBeat();
    }

    public ObTableLoadClientStatus getStatus() throws Exception {
        if (status == ObTableLoadClientStatus.MAX_STATUS) {
            throw new ObTableException("unexpected status to get status, status:"
                                       + status.toString(),
                ResultCodes.OB_STATE_NOT_MATCH.errorCode);
        }
        ObTableDirectLoadGetStatusArg arg = new ObTableDirectLoadGetStatusArg();
        ObTableDirectLoadGetStatusRes res = new ObTableDirectLoadGetStatusRes();
        arg.setTableId(tableId);
        arg.setTaskId(taskId);
        execute(ObTableDirectLoadOperationType.GET_STATUS, arg, res);
        status = res.getStatus();
        errorCode = res.getErrorCode();
        return status;
    }

    public void insert(ObDirectLoadBucket bucket) throws Exception {
        if (!isRunning()) {
            throw new ObTableException("unexpected status to insert, status:" + status.toString(),
                ResultCodes.OB_STATE_NOT_MATCH.errorCode);
        }
        ObTableDirectLoadInsertArg arg = new ObTableDirectLoadInsertArg();
        arg.setTableId(tableId);
        arg.setTaskId(taskId);
        arg.setPayload(new ObBytesString(bucket.encode()));
        execute(ObTableDirectLoadOperationType.INSERT, arg);
    }

    private void heartBeat() throws Exception {
        ObTableDirectLoadHeartBeatArg arg = new ObTableDirectLoadHeartBeatArg();
        ObTableDirectLoadHeartBeatRes res = new ObTableDirectLoadHeartBeatRes();
        arg.setTableId(tableId);
        arg.setTaskId(taskId);
        execute(ObTableDirectLoadOperationType.HEART_BEAT, arg, res);
        status = res.getStatus();
        errorCode = res.getErrorCode();
    }

    private void startHeartBeat() {
        timer = new Timer();
        TimerTask task = new TimerTask() {
            public void run() {
                try {
                    heartBeat();
                } catch (Exception e) {
                    stopHeartBeat();
                }
            }
        };
        timer.schedule(task, 0, 10000);
    }

    private void stopHeartBeat() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }

    private void execute(ObTableDirectLoadOperationType operationType, ObSimplePayload arg,
                         ObSimplePayload res) throws Exception {
        ObTableDirectLoadRequest request = new ObTableDirectLoadRequest();
        ObTableDirectLoadRequest.Header header = new ObTableDirectLoadRequest.Header();
        if (operationType != ObTableDirectLoadOperationType.BEGIN) {
            header.setAddr(srvAddr);
        }
        header.setOperationType(operationType);
        request.setHeader(header);
        request.setArgContent(new ObBytesString(arg.encode()));
        ObTableDirectLoadResult result = (ObTableDirectLoadResult) table.execute(request);
        if (result.getHeader().getOperationType() != operationType) {
            throw new ObTableException("unexpected result operation type:"
                                       + result.getHeader().getOperationType().toString()
                                       + ", request operation type:" + operationType.toString(),
                ResultCodes.OB_ERR_UNEXPECTED.errorCode);
        }
        if (result.getResContent().length() == 0) {
            throw new ObTableException("unexpected empty res content",
                ResultCodes.OB_ERR_UNEXPECTED.errorCode);
        }
        if (operationType == ObTableDirectLoadOperationType.BEGIN) {
            srvAddr = result.getHeader().getAddr();
        }
        ByteBuf buf = ByteBufAllocator.DEFAULT.buffer(result.getResContent().length());
        try {
            buf.writeBytes(result.getResContent().bytes);
            res.decode(buf);
        } finally {
            buf.release();
        }
    }

    private void execute(ObTableDirectLoadOperationType operationType, ObSimplePayload arg)
                                                                                           throws Exception {
        ObTableDirectLoadRequest request = new ObTableDirectLoadRequest();
        ObTableDirectLoadRequest.Header header = new ObTableDirectLoadRequest.Header();
        if (operationType != ObTableDirectLoadOperationType.BEGIN) {
            header.setAddr(srvAddr);
        }
        header.setOperationType(operationType);
        request.setHeader(header);
        request.setArgContent(new ObBytesString(arg.encode()));
        ObTableDirectLoadResult result = (ObTableDirectLoadResult) table.execute(request);
        if (result.getHeader().getOperationType() != operationType) {
            throw new ObTableException("unexpected result operation type:"
                                       + result.getHeader().getOperationType().toString()
                                       + ", request operation type:" + operationType.toString(),
                ResultCodes.OB_ERR_UNEXPECTED.errorCode);
        }
        if (result.getResContent().length() != 0) {
            throw new ObTableException("unexpected res content not empty",
                ResultCodes.OB_ERR_UNEXPECTED.errorCode);
        }
    }

}

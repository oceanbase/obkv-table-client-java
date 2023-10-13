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
import com.alipay.oceanbase.rpc.property.AbstractPropertyAware;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
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
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import java.util.Timer;
import java.util.TimerTask;

import org.slf4j.Logger;

import static com.alipay.oceanbase.rpc.property.Property.*;

public class ObTableDirectLoad extends AbstractPropertyAware {

    private static final Logger     logger               = TableClientLoggerFactory
                                                             .getLogger(ObTableDirectLoad.class);

    private ObTable                 table                = null;
    private String                  tableName;
    private ObDirectLoadParameter   parameter            = null;
    private boolean                 forceCreate          = false;

    private long                    tableId              = 0;
    private long                    taskId               = 0;
    private String[]                columnNames          = new String[0];
    private ObTableLoadClientStatus status               = ObTableLoadClientStatus.MAX_STATUS;
    private ResultCodes             errorCode            = ResultCodes.OB_SUCCESS;
    private ObAddr                  srvAddr              = null;

    private Timer                   timer;

    /* Properities */
    protected int                   runtimeRetryTimes    = RUNTIME_RETRY_TIMES.getDefaultInt();
    protected int                   runtimeRetryInterval = RUNTIME_RETRY_INTERVAL.getDefaultInt();

    public ObTableDirectLoad(ObTable table, String tableName, ObDirectLoadParameter parameter,
                             boolean forceCreate) {
        if (ObGlobal.OB_VERSION < ObGlobal.OB_VERSION_4_2_1_0) {
            logger.warn("not supported ob version {}", ObGlobal.obVsnString());
            throw new ObTableException("not supported ob version " + ObGlobal.obVsnString(),
                ResultCodes.OB_NOT_SUPPORTED.errorCode);
        }
        this.table = table;
        this.tableName = tableName;
        this.parameter = parameter;
        this.forceCreate = forceCreate;
        initProperties();
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
        return String.format("{tableName:%s, tableId:%d, taskId:%d}", tableName, tableId, taskId);
    }

    private void initProperties() {
        runtimeRetryTimes = parseToInt(RUNTIME_RETRY_TIMES.getKey(), runtimeRetryTimes);
        runtimeRetryInterval = parseToInt(RUNTIME_RETRY_INTERVAL.getKey(), runtimeRetryInterval);
    }

    public void begin() throws Exception {
        if (status != ObTableLoadClientStatus.MAX_STATUS) {
            logger.warn("unexpected status to begin, table:{}, status:{}", toString(),
                status.toString());
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
        logger.info("begin suceess, table:{}", toString());
        startHeartBeat();
    }

    public void commit() throws Exception {
        if (!isRunning()) {
            logger.warn("unexpected status to commit, table:{}, status:{}", toString(),
                status.toString());
            throw new ObTableException("unexpected status to commit, status:" + status.toString(),
                ResultCodes.OB_STATE_NOT_MATCH.errorCode);
        }
        ObTableDirectLoadCommitArg arg = new ObTableDirectLoadCommitArg();
        arg.setTableId(tableId);
        arg.setTaskId(taskId);
        execute(ObTableDirectLoadOperationType.COMMIT, arg);
        logger.info("commit suceess, table:{}", toString());
    }

    public void abort() throws Exception {
        if (status == ObTableLoadClientStatus.MAX_STATUS) {
            logger.warn("unexpected status to abort, table:{}, status:{}", toString(),
                status.toString());
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
        logger.info("abort suceess, table:{}", toString());
        stopHeartBeat();
    }

    public ObTableLoadClientStatus getStatus() throws Exception {
        if (status == ObTableLoadClientStatus.MAX_STATUS) {
            logger.warn("unexpected status to get status, table:{}, status:{}", toString(),
                status.toString());
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
            logger.warn("unexpected status to insert, table:{}, status:{}", toString(),
                status.toString());
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
                    logger.info(String.format("heart beat failed, table:%s", toString()), e);
                }
            }
        };
        timer.schedule(task, 0, 10000);
        logger.info("start heart beat, table:{}", toString());
    }

    private void stopHeartBeat() {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
        logger.info("stop heart beat, table:{}", toString());
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
        ObTableDirectLoadResult result = (ObTableDirectLoadResult) rpcCall(request);
        if (result.getHeader().getOperationType() != operationType) {
            logger.warn("unexpected result operation type, table:{}, reqOpType:{}, resOpType:{}",
                toString(), operationType.toString(), result.getHeader().getOperationType()
                    .toString());
            throw new ObTableException("unexpected result operation type:"
                                       + result.getHeader().getOperationType().toString()
                                       + ", request operation type:" + operationType.toString(),
                ResultCodes.OB_ERR_UNEXPECTED.errorCode);
        }
        if (result.getResContent().length() == 0) {
            logger.warn("unexpected empty res content, table:{}, OpType:{}", toString(),
                operationType.toString());
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
        ObTableDirectLoadResult result = (ObTableDirectLoadResult) rpcCall(request);
        if (result.getHeader().getOperationType() != operationType) {
            logger.warn("unexpected result operation type, table:{}, reqOpType:{}, resOpType:{}",
                toString(), operationType.toString(), result.getHeader().getOperationType()
                    .toString());
            throw new ObTableException("unexpected result operation type:"
                                       + result.getHeader().getOperationType().toString()
                                       + ", request operation type:" + operationType.toString(),
                ResultCodes.OB_ERR_UNEXPECTED.errorCode);
        }
        if (result.getResContent().length() != 0) {
            logger.warn("unexpected res content not empty, table:{}, OpType:{}", toString(),
                operationType.toString());
            throw new ObTableException("unexpected res content not empty",
                ResultCodes.OB_ERR_UNEXPECTED.errorCode);
        }
    }

    private ObPayload rpcCall(ObPayload request) throws Exception {
        int tries = 0;
        while (true) {
            try {
                return table.execute(request);
            } catch (Exception e) {
                logger
                    .warn(String.format("table execute failed, table:%s, tries:%d", toString(),
                        tries), e);
                if (tries < runtimeRetryTimes) {
                    Thread.sleep(runtimeRetryInterval);
                } else {
                    throw e;
                }
                ++tries;
            }
        }
    }

}

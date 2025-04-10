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

import java.util.concurrent.atomic.AtomicInteger;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadLogger;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadTraceId;
import com.alipay.oceanbase.rpc.direct_load.exception.*;
import com.alipay.oceanbase.rpc.direct_load.future.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObAddr;

public class ObDirectLoadStatementExecutor {

    // begin : NONE -> PREPARE_BEGIN -> BEGINNING -> LOADING | FAIL
    // commit : LOADING -> PREPARE_COMMIT -> COMMITTING -> COMMIT | FAIL
    // heartbeat : LOADING -> LOADING | FAIL
    // abort : LOADING | FAIL -> ABORT
    private static final int                   NONE          = 0;
    private static final int                   BEGINNING     = 1;
    private static final int                   LOADING       = 2;                      // 可以导入数据
    private static final int                   LOADING_ONLY  = 3;
    private static final int                   COMMITTING    = 4;
    private static final int                   COMMIT        = 5;
    private static final int                   FAIL          = 6;
    private static final int                   ABORT         = 7;
    private AtomicInteger                      stateFlag     = new AtomicInteger(NONE);
    private boolean                            isDetached    = false;

    private final ObDirectLoadStatement        statement;
    private final ObDirectLoadTraceId          traceId;
    private final ObDirectLoadLogger           logger;

    private ObDirectLoadStatementFuture        beginFuture   = null;
    private ObDirectLoadStatementFuture        commitFuture  = null;
    private ObDirectLoadStatementHeartBeatTask heartBeatTask = null;
    private ObDirectLoadStatementFuture        abortFuture   = null;

    private long                               tableId       = 0;
    private long                               taskId        = 0;
    private ObAddr                             svrAddr       = null;
    private ObDirectLoadException              cause         = null;                   // 失败原因

    public ObDirectLoadStatementExecutor(ObDirectLoadStatement statement) {
        this.statement = statement;
        this.traceId = statement.getTraceId();
        this.logger = statement.getLogger();
    }

    public ObDirectLoadStatement getStatement() {
        return statement;
    }

    public ObDirectLoadTraceId getTraceId() {
        return traceId;
    }

    public ObDirectLoadLogger getLogger() {
        return logger;
    }

    public long getTableId() {
        return tableId;
    }

    public long getTaskId() {
        return taskId;
    }

    public ObAddr getSvrAddr() {
        return svrAddr;
    }

    public boolean isDetached() {
        return isDetached;
    }

    public String toString() {
        return String.format("{svrAddr:%s, tableId:%d, taskId:%d}", svrAddr, tableId, taskId);
    }

    public synchronized ObDirectLoadStatementFuture begin() {
        logger.info("statement call begin");
        ObDirectLoadStatementAsyncPromiseTask task = null;
        try {
            compareAndSetState(NONE, BEGINNING, "begin");
        } catch (ObDirectLoadException e) {
            logger.warn("statement begin failed", e);
            return new ObDirectLoadStatementFailedFuture(statement, e);
        }
        try {
            task = new ObDirectLoadStatementBeginTask(statement, this);
            task.submit();
            beginFuture = task;
        } catch (ObDirectLoadException e) {
            logger.warn("statement start begin failed", e);
            cause = e;
            tryCompareAndSetState(BEGINNING, FAIL, "set begin failure");
        }
        return task;
    }

    public synchronized ObDirectLoadStatementFuture commit() {
        logger.info("statement call commit");
        ObDirectLoadStatementAsyncPromiseTask task = null;
        try {
            compareAndSetState(LOADING, COMMITTING, "commit");
        } catch (ObDirectLoadException e) {
            logger.warn("statement commit failed", e);
            return new ObDirectLoadStatementFailedFuture(statement, e);
        }
        try {
            task = new ObDirectLoadStatementCommitTask(statement, this);
            task.submit();
            commitFuture = task;
        } catch (ObDirectLoadException e) {
            logger.warn("statement start commit failed", e);
            cause = e;
            tryCompareAndSetState(COMMITTING, FAIL, "set commit failure");
        }
        return task;
    }

    public synchronized void detach() throws ObDirectLoadException {
        logger.info("statement call detach");
        checkState(LOADING, COMMITTING, "detach");
        if (isDetached) {
            logger.debug("statement already is detached");
        } else {
            ObDirectLoadStatementPromiseTask task = new ObDirectLoadStatementDetachTask(statement,
                this);
            task.run();
            if (!task.isDone()) {
                logger.warn("statement detach task unexpected not done");
                throw new ObDirectLoadUnexpectedException(
                    "statement detach task unexpected not done");
            }
            if (!task.isSuccess()) {
                throw task.cause();
            }
            isDetached = true;
            logger.debug("statement detach successful");
        }
    }

    public ObDirectLoadStatementExecutionId getExecutionId() throws ObDirectLoadException {
        checkState(LOADING, "getExecutionId");
        ObDirectLoadStatementExecutionId executionId = new ObDirectLoadStatementExecutionId(
            tableId, taskId, svrAddr);
        return executionId;
    }

    public synchronized void resume(ObDirectLoadStatementExecutionId executionId)
                                                                                 throws ObDirectLoadException {
        logger.info("statement call resume");
        try {
            compareAndSetState(NONE, LOADING_ONLY, "resume");
        } catch (ObDirectLoadException e) {
            logger.warn("statement resume failed", e);
            throw e;
        }
        tableId = executionId.getTableId();
        taskId = executionId.getTaskId();
        svrAddr = executionId.getSvrAddr();
    }

    public void close() {
        // 如果begin还在执行, 等待begin结束
        ObDirectLoadStatementFuture beginFuture = null;
        synchronized (this) {
            if (this.beginFuture != null && !this.beginFuture.isDone()) {
                beginFuture = this.beginFuture;
            }
        }
        if (beginFuture != null) {
            logger.info("statement close wait begin");
            try {
                beginFuture.await();
            } catch (ObDirectLoadInterruptedException e) {
                logger.warn("statement wait begin failed");
            }
        }
        // 如果commit还在执行, 等待commit结束
        ObDirectLoadStatementFuture commitFuture = null;
        synchronized (this) {
            if (this.commitFuture != null && !this.commitFuture.isDone()) {
                commitFuture = this.commitFuture;
            }
        }
        if (commitFuture != null) {
            logger.info("statement close wait commit");
            try {
                commitFuture.await();
            } catch (ObDirectLoadInterruptedException e) {
                logger.warn("statement wait commit failed");
            }
        }
        // 如果heart beat还在执行, 取消heart beat
        ObDirectLoadStatementHeartBeatTask heartBeatTask = null;
        synchronized (this) {
            if (this.heartBeatTask != null && !this.heartBeatTask.isDone()) {
                final boolean canceled = this.heartBeatTask.cancel();
                if (!canceled) {
                    heartBeatTask = this.heartBeatTask;
                }
            }
        }
        if (heartBeatTask != null) {
            logger.info("statement close wait heart beat");
            try {
                heartBeatTask.await();
            } catch (ObDirectLoadInterruptedException e) {
                logger.warn("statement wait heart beat failed");
            }
        }
        // 退出任务
        abortIfNeed();
        ObDirectLoadStatementFuture abortFuture = null;
        synchronized (this) {
            if (this.abortFuture != null && !this.abortFuture.isDone()) {
                abortFuture = this.abortFuture;
            }
        }
        if (abortFuture != null) {
            logger.info("statement close wait abort");
            try {
                abortFuture.await();
            } catch (ObDirectLoadException e) {
                logger.warn("statement abort failed", e);
            }
        }
    }

    private synchronized void abortIfNeed() {
        logger.debug("statement abort if need");
        if (abortFuture != null) {
            logger.debug("statement in abort");
            return;
        }
        final int state = stateFlag.get();
        boolean needAbort = false;
        boolean unexpectedState = false;
        String reason = "";
        if (state == NONE) {
            reason = "not begin";
        } else if (state == BEGINNING) {
            unexpectedState = true;
            reason = "begin not finish";
        } else if (state == LOADING) {
            needAbort = true;
        } else if (state == LOADING_ONLY) {
            needAbort = false;
        } else if (state == COMMITTING) {
            unexpectedState = true;
            reason = "commit not finish";
        } else if (state == COMMIT) {
            reason = "already commit";
        } else if (state == FAIL) {
            if (svrAddr != null) {
                needAbort = true;
            } else {
                reason = "begin fail";
            }
        } else if (state == ABORT) {
            reason = "already abort";
        }
        if (!needAbort) {
            if (unexpectedState) {
                logger.warn("statement cannot abort because " + reason);
            } else {
                logger.debug("statement no need abort because " + reason);
                setState(ABORT);
            }
        } else if (isDetached) {
            logger.debug("statement no need abort because is detached");
        } else {
            abort();
        }
    }

    private ObDirectLoadStatementFuture abort() {
        logger.info("statement call abort");
        setState(ABORT);
        ObDirectLoadStatementAsyncPromiseTask task = null;
        try {
            task = new ObDirectLoadStatementAbortTask(statement, this);
            task.submit();
            abortFuture = task;
        } catch (ObDirectLoadException e) {
            logger.warn("statement start abort failed", e);
        }
        return task;
    }

    void startHeartBeat() throws ObDirectLoadException {
        logger.info("statement start heart beat");
        try {
            ObDirectLoadStatementHeartBeatTask task = new ObDirectLoadStatementHeartBeatTask(
                statement, this);
            task.submit();
            heartBeatTask = task;
        } catch (ObDirectLoadException e) {
            logger.warn("statement start heart beat failed", e);
            throw e;
        }
    }

    void stopHeartBeat() {
        logger.info("statement stop heart beat");
        try {
            ObDirectLoadStatementHeartBeatTask task = heartBeatTask;
            if (task == null) {
                logger.warn("statement heart beat not start");
                throw new ObDirectLoadUnexpectedException("statement heart beat not start");
            }
            final boolean canceled = task.cancel();
            if (!canceled) {
                return;
            }
            heartBeatTask = null;
        } catch (ObDirectLoadException e) {
            logger.warn("statement stop heart beat failed", e);
        }
    }

    public void write(ObDirectLoadBucket bucket) throws ObDirectLoadException {
        checkState(LOADING, LOADING_ONLY, "write");
        ObDirectLoadStatementPromiseTask task = new ObDirectLoadStatementWriteTask(statement, this,
            bucket);
        task.run();
        if (!task.isDone()) {
            logger.warn("statement write task unexpected not done");
            throw new ObDirectLoadUnexpectedException("statement write task unexpected not done");
        }
        if (!task.isSuccess()) {
            throw task.cause();
        }
    }

    private String getUnexpectedStateReason(int state) {
        String reason = "";
        if (state == NONE) {
            reason = "not begin";
        } else if (state == BEGINNING) {
            reason = "is beginning";
        } else if (state == LOADING) {
            reason = "is loading";
        } else if (state == COMMITTING) {
            reason = "is committing";
        } else if (state == COMMIT) {
            reason = "is commit";
        } else if (state == FAIL || state == ABORT) {
            reason = "is fail";
        } else {
            reason = "unknow state";
        }
        return reason;
    }

    void compareAndSetState(int expect, int update, String action)
                                                                  throws ObDirectLoadIllegalStateException {
        if (!stateFlag.compareAndSet(expect, update)) {
            final int state = stateFlag.get();
            String reason = getUnexpectedStateReason(state);
            String message = "statement cannot " + action + " because " + reason + ", state:"
                             + state + ", expect:" + expect + ", update:" + update;
            logger.warn(message);
            if (cause == null) {
                throw new ObDirectLoadIllegalStateException(message);
            } else {
                throw new ObDirectLoadIllegalStateException(message, cause);
            }
        }
    }

    boolean tryCompareAndSetState(int expect, int update, String action) {
        boolean bResult = stateFlag.compareAndSet(expect, update);
        if (!bResult) {
            final int state = stateFlag.get();
            String reason = getUnexpectedStateReason(state);
            String message = "statement cannot " + action + " because " + reason + ", state:"
                             + state + ", expect:" + expect + ", update:" + update;
            logger.warn(message);
        }
        return bResult;
    }

    void setState(int update) {
        stateFlag.set(update);
    }

    void checkState(int expect, String action) throws ObDirectLoadException {
        final int state = stateFlag.get();
        if (state != expect) {
            String reason = getUnexpectedStateReason(state);
            String message = "statement cannot " + action + " because " + reason + ", state:"
                             + state + ", expect:" + expect;
            logger.warn(message);
            if (cause == null) {
                throw new ObDirectLoadIllegalStateException(message);
            } else {
                throw new ObDirectLoadIllegalStateException(message, cause);
            }
        }
    }

    /**
     * check state in range [start, end]
     */
    void checkState(int start, int end, String action) throws ObDirectLoadIllegalStateException {
        final int state = stateFlag.get();
        if (state < start || state > end) {
            String reason = getUnexpectedStateReason(state);
            String message = "statement cannot " + action + " because " + reason + ", state:"
                             + state + ", expect:[" + start + ", " + end + "]";
            logger.warn(message);
            if (cause == null) {
                throw new ObDirectLoadIllegalStateException(message);
            } else {
                throw new ObDirectLoadIllegalStateException(message, cause);
            }
        }
    }

    BeginProxy getBeginProxy() {
        return new BeginProxy(this);
    }

    CommitProxy getCommitProxy() {
        return new CommitProxy(this);
    }

    WriteProxy getWriteProxy() {
        return new WriteProxy(this);
    }

    HeartBeatProxy getHeartBeatProxy() {
        return new HeartBeatProxy(this);
    }

    private static abstract class BaseProxy {

        protected final ObDirectLoadConnection        connection;
        protected final ObDirectLoadStatement         statement;
        protected final ObDirectLoadStatementExecutor executor;
        protected final ObDirectLoadLogger            logger;

        BaseProxy(ObDirectLoadStatementExecutor executor) {
            this.executor = executor;
            this.statement = executor.getStatement();
            this.connection = statement.getConnection();
            this.logger = statement.getLogger();
        }

        void checkState() throws ObDirectLoadException {
        }

        void checkStatus() throws ObDirectLoadException {
            connection.checkStatus();
            statement.checkStatus();
            checkState();
            statement.checkTimeout();
        }

    };

    static final class BeginProxy extends BaseProxy {

        BeginProxy(ObDirectLoadStatementExecutor executor) {
            super(executor);
        }

        @Override
        void checkState() throws ObDirectLoadException {
            executor.checkState(BEGINNING, "begin");
        }

        void setSuccess0(ObAddr addr, long tableId, long taskId) throws ObDirectLoadException {
            executor.checkState(BEGINNING, "begin");
            executor.svrAddr = addr;
            executor.tableId = tableId;
            executor.taskId = taskId;
            executor.startHeartBeat();
        }

        void setSuccess() throws ObDirectLoadException {
            executor.compareAndSetState(BEGINNING, LOADING, "set begin success");
            executor.beginFuture = null;
            logger.info("statement begin successful");
        }

        void setFailure(ObDirectLoadException cause) {
            try {
                executor.compareAndSetState(BEGINNING, FAIL, "set begin failure");
            } catch (ObDirectLoadException e) {
                return;
            }
            executor.cause = cause;
            logger.warn("statement begin failed", cause);
            executor.abortIfNeed();
        }

        void clear() {
            executor.stopHeartBeat();
        }

    };

    static final class CommitProxy extends BaseProxy {

        CommitProxy(ObDirectLoadStatementExecutor executor) {
            super(executor);
        }

        @Override
        void checkState() throws ObDirectLoadException {
            executor.checkState(COMMITTING, "commit");
        }

        void setSuccess() throws ObDirectLoadException {
            executor.compareAndSetState(COMMITTING, COMMIT, "set commit success");
            executor.commitFuture = null;
            logger.info("statement commit successful");
            executor.stopHeartBeat();
        }

        void setFailure(ObDirectLoadException cause) {
            try {
                executor.compareAndSetState(COMMITTING, FAIL, "set commit failure");
            } catch (ObDirectLoadException e) {
                return;
            }
            executor.cause = cause;
            logger.warn("statement commit failed", cause);
            executor.abortIfNeed();
        }

    };

    static final class WriteProxy extends BaseProxy {

        WriteProxy(ObDirectLoadStatementExecutor executor) {
            super(executor);
        }

        @Override
        void checkState() throws ObDirectLoadException {
            executor.checkState(LOADING, LOADING_ONLY, "write");
        }

        void setSuccess() throws ObDirectLoadException {
            // do nothing
        }

        void setFailure(ObDirectLoadException cause) {
            // do nothing
        }

    };

    static final class HeartBeatProxy extends BaseProxy {

        HeartBeatProxy(ObDirectLoadStatementExecutor executor) {
            super(executor);
        }

        @Override
        void checkState() throws ObDirectLoadException {
            executor.checkState(BEGINNING, COMMIT, "heart beat");
        }

        void setSuccess() throws ObDirectLoadException {
            // do nothing
        }

        void setFailure(ObDirectLoadException cause) {
            try {
                executor.compareAndSetState(LOADING, FAIL, "set heart beat failure");
            } catch (ObDirectLoadException e) {
                return;
            }
            executor.cause = cause;
            logger.warn("statement heart beat failed", cause);
            executor.abortIfNeed();
        }

    };

}

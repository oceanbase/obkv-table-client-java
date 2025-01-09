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

package com.alipay.oceanbase.rpc.direct_load;

import java.util.Arrays;

import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadIllegalArgumentException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadIllegalStateException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadTimeoutException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadUnexpectedException;
import com.alipay.oceanbase.rpc.direct_load.execution.ObDirectLoadStatementExecutionId;
import com.alipay.oceanbase.rpc.direct_load.execution.ObDirectLoadStatementExecutor;
import com.alipay.oceanbase.rpc.direct_load.future.ObDirectLoadStatementFailedFuture;
import com.alipay.oceanbase.rpc.direct_load.future.ObDirectLoadStatementFuture;
import com.alipay.oceanbase.rpc.direct_load.util.ObDirectLoadUtil;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;

public class ObDirectLoadStatement {

    private final ObDirectLoadConnection       connection;
    private final ObDirectLoadTraceId          traceId;
    private final ObDirectLoadLogger           logger;

    private String                             tableName            = null;
    private String[]                           columnNames          = null;
    private String[]                           partitionNames       = null;
    private ObLoadDupActionType                dupAction            = ObLoadDupActionType.INVALID_MODE;

    private int                                parallel             = 0;
    private long                               queryTimeout         = 0;

    private long                               maxErrorRowCount     = 0;
    private String                             loadMethod           = "";

    private boolean                            isInited             = false;
    private boolean                            isClosed             = false;

    private ObDirectLoadConnection.ObTablePool obTablePool          = null;
    private ObDirectLoadStatementExecutor      executor             = null;
    private long                               startQueryTimeMillis = 0;

    ObDirectLoadStatement(ObDirectLoadConnection connection) {
        this.connection = connection;
        this.traceId = ObDirectLoadTraceId.generateTraceId();
        this.logger = ObDirectLoadLogger.getLogger(this.traceId);
    }

    public ObDirectLoadConnection getConnection() {
        return connection;
    }

    public ObDirectLoadTraceId getTraceId() {
        return traceId;
    }

    public ObDirectLoadLogger getLogger() {
        return logger;
    }

    public synchronized void init(Builder builder) throws ObDirectLoadException {
        if (isInited) {
            logger.warn("statement init twice");
            throw new ObDirectLoadIllegalStateException("statement init twice");
        }
        if (isClosed) {
            logger.warn("statement is closed");
            throw new ObDirectLoadIllegalStateException("statement is closed");
        }
        fillParams(builder);
        initCheck();
        connection.getProtocol().checkIsSupported(this);
        obTablePool = new ObDirectLoadConnection.ObTablePool(connection, logger, queryTimeout);
        obTablePool.init();
        executor = new ObDirectLoadStatementExecutor(this);
        startQueryTimeMillis = System.currentTimeMillis();
        isInited = true;
        logger.info("statement init successful, args:" + builder);
    }

    public synchronized void close() {
        logger.info("statement close");
        if (isClosed) {
            logger.info("statement is closed");
            return;
        }
        isClosed = true;
        if (executor != null) {
            executor.close();
            executor = null;
        }
        if (obTablePool != null) {
            obTablePool.close();
            obTablePool = null;
        }
        logger.info("statement close successful");
        connection.closeStatement(this);
    }

    private void fillParams(Builder builder) throws ObDirectLoadException {
        if (builder == null) {
            logger.warn("builder cannot be null");
            throw new ObDirectLoadIllegalArgumentException("builder cannot be null");
        }
        tableName = builder.tableName;
        columnNames = builder.columnNames;
        partitionNames = builder.partitionNames;
        dupAction = builder.dupAction;
        parallel = builder.parallel;
        queryTimeout = builder.queryTimeout;
        maxErrorRowCount = builder.maxErrorRowCount;
        loadMethod = builder.loadMethod;
        if (loadMethod.compareToIgnoreCase("inc_replace") == 0) {
            // inc_replace模式强制设置dupAction为STOP_ON_DUP
            dupAction = ObLoadDupActionType.STOP_ON_DUP;
        }
    }

    private void initCheck() throws ObDirectLoadException {
        ObDirectLoadUtil.checkNonEmpty(tableName, "tableName", logger);
        if (columnNames == null) {
            columnNames = new String[0];
        } else {
            ObDirectLoadUtil.checkNonEmptyAndUnique(columnNames, "columnNames", logger);
        }
        if (partitionNames == null) {
            partitionNames = new String[0];
        } else {
            ObDirectLoadUtil.checkNonEmptyAndUnique(partitionNames, "partitionNames", logger);
        }
        ObDirectLoadUtil.checkNonValid(dupAction, ObLoadDupActionType.INVALID_MODE, "dupAction",
            logger);
        ObDirectLoadUtil.checkPositive(parallel, "parallel", logger);
        ObDirectLoadUtil.checkPositive(queryTimeout, "queryTimeout", logger);
        ObDirectLoadUtil.checkPositiveOrZero(maxErrorRowCount, "maxErrorRowCount", logger);
        ObDirectLoadUtil.checkNonEmpty(loadMethod, "loadMethod", logger);
    }

    public void checkStatus() throws ObDirectLoadException {
        if (!isInited) {
            logger.warn("statement not init");
            throw new ObDirectLoadIllegalStateException("statement not init");
        }
        if (isClosed) {
            logger.warn("statement is closed");
            throw new ObDirectLoadIllegalStateException("statement is closed");
        }
    }

    public String getTableName() {
        return tableName;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public String[] getPartitionNames() {
        return partitionNames;
    }

    public ObLoadDupActionType getDupAction() {
        return dupAction;
    }

    public int getParallel() {
        return parallel;
    }

    public long getQueryTimeout() {
        return queryTimeout;
    }

    public long getMaxErrorRowCount() {
        return maxErrorRowCount;
    }

    public String getLoadMethod() {
        return loadMethod;
    }

    public void checkTimeout() throws ObDirectLoadException {
        if (startQueryTimeMillis == 0) {
            logger.warn("statement not start");
            throw new ObDirectLoadUnexpectedException("statement not start");
        }
        if (startQueryTimeMillis + queryTimeout < System.currentTimeMillis()) {
            logger.warn("statement timeout");
            throw new ObDirectLoadTimeoutException("statement timeout");
        }
    }

    public long getTimeoutRemain() {
        long remainTimeout = queryTimeout;
        if (startQueryTimeMillis > 0) {
            remainTimeout -= (System.currentTimeMillis() - startQueryTimeMillis);
        }
        return remainTimeout;
    }

    public ObDirectLoadConnection.ObTablePool getObTablePool() {
        return obTablePool;
    }

    @Deprecated
    public void setBeginRpcTimeout(long timeoutMillis) {
    }

    @Deprecated
    public void setWriteRpcTimeout(long timeoutMillis) {
    }

    public String toString() {
        return String
            .format(
                "{tableName:%s, columnNames:%s, partitionNames:%s, dupAction:%s, parallel:%d, queryTimeout:%d, maxErrorRowCount:%d, loadMethod:%s, executor:%s}",
                tableName, Arrays.toString(columnNames), Arrays.toString(partitionNames),
                dupAction, parallel, queryTimeout, maxErrorRowCount, loadMethod, executor);
    }

    public ObDirectLoadStatementFuture beginAsync() {
        try {
            checkStatus();
            return executor.begin();
        } catch (ObDirectLoadException e) {
            logger.warn("statement begin failed", e);
            return new ObDirectLoadStatementFailedFuture(this, e);
        }
    }

    public void begin() throws ObDirectLoadException {
        ObDirectLoadStatementFuture future = beginAsync();
        future.await();
        if (!future.isSuccess()) {
            throw future.cause();
        }
    }

    public ObDirectLoadStatementFuture commitAsync() {
        try {
            checkStatus();
            return executor.commit();
        } catch (ObDirectLoadException e) {
            logger.warn("statement commit failed", e);
            return new ObDirectLoadStatementFailedFuture(this, e);
        }
    }

    public void commit() throws ObDirectLoadException {
        ObDirectLoadStatementFuture future = commitAsync();
        future.await();
        if (!future.isSuccess()) {
            throw future.cause();
        }
    }

    public void write(ObDirectLoadBucket bucket) throws ObDirectLoadException {
        if (bucket == null || bucket.isEmpty()) {
            logger.warn("Param 'bucket' must not be null or empty, value:" + bucket);
            throw new ObDirectLoadIllegalArgumentException(
                "Param 'bucket' must not be null or empty, value:" + bucket);
        }
        checkStatus();
        executor.write(bucket);
    }

    public ObDirectLoadStatementExecutionId getExecutionId() throws ObDirectLoadException {
        checkStatus();
        return executor.getExecutionId();
    }

    public void resume(ObDirectLoadStatementExecutionId executionId) throws ObDirectLoadException {
        if (executionId == null || !executionId.isValid()) {
            logger.warn("Param 'executionId' must not be null or invalid, value:" + executionId);
            throw new ObDirectLoadIllegalArgumentException(
                "Param 'executionId' must not be null or invalid, value:" + executionId);
        }
        checkStatus();
        executor.resume(executionId);
    }

    public static final class Builder {

        private final ObDirectLoadConnection connection;

        private String                       tableName         = null;
        private String[]                     columnNames       = null;
        private String[]                     partitionNames    = null;
        private ObLoadDupActionType          dupAction         = ObLoadDupActionType.INVALID_MODE;

        private int                          parallel          = 0;
        private long                         queryTimeout      = 0;

        private long                         maxErrorRowCount  = 0;
        private String                       loadMethod        = "full";

        private static final long            MAX_QUERY_TIMEOUT = 2147483647;     // INT_MAX

        Builder(ObDirectLoadConnection connection) {
            this.connection = connection;
        }

        public Builder setTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder setColumnNames(String[] columnNames) {
            this.columnNames = columnNames;
            return this;
        }

        public Builder setPartitionNames(String[] partitionNames) {
            this.partitionNames = partitionNames;
            return this;
        }

        public Builder setDupAction(ObLoadDupActionType dupAction) {
            this.dupAction = dupAction;
            return this;
        }

        public Builder setParallel(int parallel) {
            this.parallel = parallel;
            return this;
        }

        public Builder setQueryTimeout(long queryTimeout) {
            this.queryTimeout = Math.min(queryTimeout, MAX_QUERY_TIMEOUT);
            return this;
        }

        public Builder setMaxErrorRowCount(long maxErrorRowCount) {
            this.maxErrorRowCount = maxErrorRowCount;
            return this;
        }

        public Builder setLoadMethod(String loadMethod) {
            this.loadMethod = loadMethod;
            return this;
        }

        public String toString() {
            return String
                .format(
                    "{tableName:%s, columnNames:%s, partitionNames:%s, dupAction:%s, parallel:%d, queryTimeout:%d, maxErrorRowCount:%d, loadMethod:%s}",
                    tableName, Arrays.toString(columnNames), Arrays.toString(partitionNames),
                    dupAction, parallel, queryTimeout, maxErrorRowCount, loadMethod);
        }

        public ObDirectLoadStatement build() throws ObDirectLoadException {
            return connection.buildStatement(this);
        }

    }

}

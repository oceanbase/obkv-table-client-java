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

import java.util.LinkedList;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.direct_load.exception.*;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocol;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocolFactory;
import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadRpc;
import com.alipay.oceanbase.rpc.direct_load.util.ObDirectLoadUtil;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.table.ObTable;

public class ObDirectLoadConnection {

    private final ObDirectLoadConnectionFactory connectionFactory;
    private final ObDirectLoadTraceId           traceId;
    private final ObDirectLoadLogger            logger;

    private String                              ip                 = null;
    private int                                 port               = 0;

    private String                              tenantName         = null;
    private String                              userName           = null;
    private String                              password           = null;
    private String                              databaseName       = null;

    private int                                 writeConnectionNum = 0;

    private long                                heartBeatTimeout   = 0;
    private long                                heartBeatInterval  = 0;

    private boolean                             isInited           = false;
    private boolean                             isClosed           = false;

    private ObDirectLoadProtocol                protocol           = null;

    private LinkedList<ObDirectLoadStatement>   statementList      = null; // statement列表

    ObDirectLoadConnection(ObDirectLoadConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        this.traceId = ObDirectLoadTraceId.generateTraceId();
        this.logger = ObDirectLoadLogger.getLogger(this.traceId);
    }

    public ObDirectLoadTraceId getTraceId() {
        return traceId;
    }

    public ObDirectLoadLogger getLogger() {
        return logger;
    }

    public synchronized void init(Builder builder) throws ObDirectLoadException {
        if (isInited) {
            logger.warn("init twice, connection:" + this);
            throw new ObDirectLoadIllegalStateException("init twice, connection:" + this);
        }
        if (isClosed) {
            logger.warn("already closed, connection:" + this);
            throw new ObDirectLoadIllegalStateException("already closed, connection:" + this);
        }
        fillParams(builder);
        initCheck();
        initProtocol();
        statementList = new LinkedList<ObDirectLoadStatement>();
        isInited = true;
        logger.info("connection init successful, args:" + builder);
    }

    public void close() {
        logger.info("connection close");
        ObDirectLoadStatement[] statements = null;
        synchronized (this) {
            if (isClosed) {
                logger.info("connection is closed");
                return;
            }
            isClosed = true;
            statements = statementList.toArray(new ObDirectLoadStatement[0]);
        }
        // close all statements
        if (statements.length > 0) {
            logger.info("connection close wait statements close, size:" + statements.length);
            for (ObDirectLoadStatement statement : statements) {
                statement.close();
            }
        }
        logger.info("connection close successful");
        connectionFactory.closeConnection(this);
    }

    public void checkStatus() throws ObDirectLoadException {
        if (!isInited) {
            throw new ObDirectLoadIllegalStateException("connection not init");
        }
        if (isClosed) {
            throw new ObDirectLoadIllegalStateException("connection is closed");
        }
    }

    private void fillParams(Builder builder) throws ObDirectLoadException {
        if (builder == null) {
            logger.warn("builder cannot be null");
            throw new ObDirectLoadIllegalArgumentException("builder cannot be null");
        }
        ip = builder.ip;
        port = builder.port;
        tenantName = builder.tenantName;
        userName = builder.userName;
        password = builder.password;
        databaseName = builder.databaseName;

        heartBeatTimeout = builder.heartBeatTimeout;
        heartBeatInterval = builder.heartBeatInterval;

        writeConnectionNum = builder.writeConnectionNum;
    }

    private void initCheck() throws ObDirectLoadException {
        ObDirectLoadUtil.checkNonEmpty(ip, "ip", logger);
        ObDirectLoadUtil.checkInRange(port, 1, 65535, "port", logger);
        ObDirectLoadUtil.checkNonEmpty(tenantName, "tenantName", logger);
        ObDirectLoadUtil.checkNonEmpty(userName, "userName", logger);
        ObDirectLoadUtil.checkNotNull(password, "password", logger);
        ObDirectLoadUtil.checkNonEmpty(databaseName, "databaseName", logger);
        ObDirectLoadUtil.checkPositive(writeConnectionNum, "writeConnectionNum", logger);
        if (heartBeatTimeout < 3000) {
            logger.warn("Param 'heartBeatTimeout' must not be less than 3000 ms, value:"
                        + heartBeatTimeout);
            throw new ObDirectLoadIllegalArgumentException(
                "Param 'heartBeatTimeout' must not be less than 3000 ms, value:" + heartBeatTimeout);
        }
        if (heartBeatInterval < 100) {
            logger.warn("Param 'heartBeatInterval' must not be less than 1 ms, value:"
                        + heartBeatInterval);
            throw new ObDirectLoadIllegalArgumentException(
                "Param 'heartBeatInterval' must not be less than 1 ms, value:" + heartBeatInterval);
        }
        if (heartBeatTimeout <= heartBeatInterval) {
            logger
                .warn("Param 'heartBeatInterval' must not be greater than or equal to Param 'heartBeatTimeout', heartBeatTimeout:"
                      + heartBeatTimeout + ", heartBeatInterval:" + heartBeatInterval);
            throw new ObDirectLoadIllegalArgumentException(
                "Param 'heartBeatInterval' must not be greater than or equal to Param 'heartBeatTimeout', heartBeatTimeout:"
                        + heartBeatTimeout + ", heartBeatInterval:" + heartBeatInterval);
        }
    }

    private void initProtocol() throws ObDirectLoadException {
        // 构造一个连接, 获取版本号
        ObTable table = null;
        synchronized (connectionFactory) { // 防止并发访问ObGlobal.OB_VERSION
            ObGlobal.OB_VERSION = 0;
            try {
                Properties properties = new Properties();
                properties.setProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(),
                    String.valueOf(1));
                table = new ObTable.Builder(ip, port)
                    .setLoginInfo(tenantName, userName, password, databaseName)
                    .setProperties(properties).build();
            } catch (Exception e) {
                throw new ObDirectLoadException(e);
            }
        }
        this.protocol = ObDirectLoadProtocolFactory.getProtocol(ObGlobal.OB_VERSION);
        this.protocol.init();
        table.close();
    }

    public ObDirectLoadProtocol getProtocol() {
        return protocol;
    }

    public long getHeartBeatTimeout() {
        return heartBeatTimeout;
    }

    public long getHeartBeatInterval() {
        return heartBeatInterval;
    }

    public String toString() {
        return String
            .format(
                "{ip:\"%s\", port:%d, tenantName:\"%s\", userName:\"%s\", databaseName:\"%s\", writeConnectionNum:%d}",
                ip, port, tenantName, userName, databaseName, writeConnectionNum);
    }

    public void executeWithConnection(final ObDirectLoadRpc rpc, ObTable table, long timeoutMillis)
                                                                                                   throws ObDirectLoadException {
        try {
            rpc.setRpcTimeout(timeoutMillis);
            ObPayload request = rpc.getRequest();
            ObPayload result = table.execute(request);
            rpc.setResult(result);
        } catch (Exception e) {
            throw ObDirectLoadExceptionUtil.convertException(e);
        }
    }

    public synchronized ObDirectLoadStatement createStatement() throws ObDirectLoadException {
        if (!isInited) {
            logger.warn("connection not init");
            throw new ObDirectLoadIllegalStateException("connection not init");
        }
        if (isClosed) {
            logger.warn("connection is closed");
            throw new ObDirectLoadIllegalStateException("connection is closed");
        }
        ObDirectLoadStatement stmt = new ObDirectLoadStatement(this);
        this.statementList.addLast(stmt);
        return stmt;
    }

    public synchronized void closeStatement(ObDirectLoadStatement stmt) {
        this.statementList.remove(stmt);
    }

    public ObDirectLoadStatement.Builder getStatementBuilder() {
        return new ObDirectLoadStatement.Builder(this);
    }

    ObDirectLoadStatement buildStatement(ObDirectLoadStatement.Builder builder)
                                                                               throws ObDirectLoadException {
        ObDirectLoadStatement stmt = null;
        try {
            stmt = createStatement();
            stmt.init(builder);
        } catch (Exception e) {
            logger.warn("build statement failed, args:" + builder, e);
            closeStatement(stmt);
            throw e;
        }
        return stmt;
    }

    public static final class Builder {

        private ObDirectLoadConnectionFactory connectionFactory      = null;

        private String                        ip                     = null;
        private int                           port                   = 0;

        private String                        tenantName             = null;
        private String                        userName               = null;
        private String                        password               = null;
        private String                        databaseName           = null;

        private int                           writeConnectionNum     = 1;

        private long                          heartBeatTimeout       = 60 * 1000;                  // 60s
        private long                          heartBeatInterval      = 10 * 1000;                  // 10s

        private static final long             MAX_HEART_BEAT_TIMEOUT = 1L * 365 * 24 * 3600 * 1000; // 1year

        Builder(ObDirectLoadConnectionFactory connectionFactory) {
            this.connectionFactory = connectionFactory;
        }

        public Builder setServerInfo(String ip, int port) {
            this.ip = ip;
            this.port = port;
            return this;
        }

        public Builder setLoginInfo(String tenantName, String userName, String password,
                                    String databaseName) {
            this.tenantName = tenantName;
            this.userName = userName;
            this.password = password;
            this.databaseName = databaseName;
            return this;
        }

        public Builder enableParallelWrite(int writeParallel) {
            this.writeConnectionNum = writeParallel;
            return this;
        }

        public Builder setHeartBeatInfo(long heartBeatTimeout, long heartBeatInterval) {
            this.heartBeatTimeout = Math.min(heartBeatTimeout, MAX_HEART_BEAT_TIMEOUT);
            this.heartBeatInterval = heartBeatInterval;
            return this;
        }

        public String toString() {
            return String
                .format(
                    "{ip:\"%s\", port:%d, tenantName:\"%s\", userName:\"%s\", databaseName:\"%s\", writeConnectionNum:%d, heartBeatTimeout:%d, heartBeatInterval:%d}",
                    ip, port, tenantName, userName, databaseName, writeConnectionNum,
                    heartBeatTimeout, heartBeatInterval);
        }

        public ObDirectLoadConnection build() throws ObDirectLoadException {
            return connectionFactory.buildConnection(this);
        }

    }

    public static final class ObTablePool {

        private static final int             highPrioConnectionIdx      = 0;    // for heart beat
        private static final int             controlConnectionIdx       = 1;    // for begin and commit
        private static final int             writeConnectionStartIdx    = 2;    // for write

        private final ObDirectLoadConnection connection;
        private final ObDirectLoadLogger     logger;
        private final long                   timeoutMillis;

        private ObTable[]                    tables;

        private BlockingQueue<ObTable>       availableWriteObTableQueue = null;
        private int                          availableWriteObTableNum   = 0;

        private boolean                      isInited                   = false;
        private boolean                      isClosed                   = false;

        ObTablePool(ObDirectLoadConnection connection, ObDirectLoadLogger logger, long timeoutMillis) {
            this.connection = connection;
            this.logger = logger;
            this.timeoutMillis = timeoutMillis;
        }

        public void init() throws ObDirectLoadException {
            synchronized (connection.connectionFactory) { // 防止并发访问ObGlobal.OB_VERSION
                initTables();
            }
            initAvailableWriteObTableQueue();
            isInited = true;
        }

        public void close() {
            if (tables != null) {
                for (int i = 0; i < tables.length; ++i) {
                    ObTable table = tables[i];
                    if (table != null) {
                        table.close();
                    }
                }
            }
            tables = null;
            isClosed = true;
        }

        private void initTables() throws ObDirectLoadException {
            final int poolSize = writeConnectionStartIdx + connection.writeConnectionNum;
            Properties properties = new Properties();
            properties
                .setProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), String.valueOf(1));
            properties.setProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(),
                String.valueOf(timeoutMillis));
            properties.setProperty(Property.RPC_OPERATION_TIMEOUT.getKey(),
                String.valueOf(timeoutMillis));
            tables = new ObTable[poolSize];
            try {
                for (int i = 0; i < tables.length; ++i) {
                    tables[i] = new ObTable.Builder(connection.ip, connection.port)
                        .setLoginInfo(connection.tenantName, connection.userName,
                            connection.password, connection.databaseName).setProperties(properties)
                        .build();
                }
            } catch (Exception e) {
                throw new ObDirectLoadException(e);
            }
        }

        private void initAvailableWriteObTableQueue() throws ObDirectLoadException {
            this.availableWriteObTableQueue = new ArrayBlockingQueue<ObTable>(
                connection.writeConnectionNum);
            try {
                for (int i = 0; i < connection.writeConnectionNum; ++i) {
                    ObTable table = tables[writeConnectionStartIdx + i];
                    this.availableWriteObTableQueue.put(table);
                }
                this.availableWriteObTableNum = connection.writeConnectionNum;
            } catch (Exception e) {
                throw new ObDirectLoadException(e);
            }
        }

        public ObTable getHighPrioObTable() throws ObDirectLoadException {
            if (!isInited) {
                logger.warn("ob table pool not init");
                throw new ObDirectLoadIllegalStateException("ob table pool not init");
            }
            if (isClosed) {
                logger.warn("ob table pool is closed");
                throw new ObDirectLoadIllegalStateException("ob table pool is closed");
            }
            return tables[highPrioConnectionIdx];
        }

        public ObTable getControlObTable() throws ObDirectLoadException {
            if (!isInited) {
                logger.warn("ob table pool not init");
                throw new ObDirectLoadIllegalStateException("ob table pool not init");
            }
            if (isClosed) {
                logger.warn("ob table pool is closed");
                throw new ObDirectLoadIllegalStateException("ob table pool is closed");
            }
            return tables[controlConnectionIdx];
        }

        public ObTable takeWriteObTable(long timeoutMillis) throws ObDirectLoadException {
            if (!isInited) {
                logger.warn("ob table pool not init");
                throw new ObDirectLoadIllegalStateException("ob table pool not init");
            }
            try {
                final long startTime = System.currentTimeMillis();
                ObTable table = null;
                while (table == null) {
                    if (isClosed) {
                        logger.warn("ob table pool is closed");
                        throw new ObDirectLoadIllegalStateException("ob table pool is closed");
                    }
                    if (availableWriteObTableNum == 0) {
                        logger.warn("ob table pool no avaiable write ob table");
                        throw new ObDirectLoadUnexpectedException(
                            "ob table pool no avaiable write ob table");
                    }
                    if (startTime + timeoutMillis < System.currentTimeMillis()) {
                        logger.warn("ob table pool task write ob table timeout");
                        throw new ObDirectLoadTimeoutException(
                            "ob table pool task write ob table timeout");
                    }
                    table = availableWriteObTableQueue.poll(1000, TimeUnit.MILLISECONDS);
                }
                return table;
            } catch (Exception e) {
                logger.warn("ob table pool task write ob table failed", e);
                throw ObDirectLoadExceptionUtil.convertException(e);
            }
        }

        public void putWriteObTable(ObTable table) {
            try {
                availableWriteObTableQueue.put(table);
            } catch (Exception e) {
                --availableWriteObTableNum;
                logger.warn("ob table pool put write ob table failed, availableWriteObTableNum:"
                            + availableWriteObTableNum, e);
            }
        }

    };

}

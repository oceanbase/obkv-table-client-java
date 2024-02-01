/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
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

import com.alipay.oceanbase.rpc.Lifecycle;
import com.alipay.oceanbase.rpc.bolt.transport.ObConnectionFactory;
import com.alipay.oceanbase.rpc.bolt.transport.ObPacketFactory;
import com.alipay.oceanbase.rpc.bolt.transport.ObTableConnection;
import com.alipay.oceanbase.rpc.bolt.transport.ObTableRemoting;
import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.exception.ExceptionUtil;
import com.alipay.oceanbase.rpc.exception.ObTableConnectionStatusException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableServerConnectException;
import com.alipay.oceanbase.rpc.batch.QueryByBatch;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.remoting.ConnectionEventHandler;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.connection.ConnectionFactory;
import com.alipay.remoting.exception.RemotingException;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import static com.alipay.oceanbase.rpc.property.Property.*;
import static com.alipay.oceanbase.rpc.protocol.payload.Constants.*;

public class ObTable extends AbstractObTable implements Lifecycle {

    private String                ip;
    private int                   port;

    private String                tenantName;
    private String                userName;
    private String                password;
    private String                database;
    private ConnectionFactory     connectionFactory;
    private ObTableRemoting       realClient;
    private ObTableConnectionPool connectionPool;

    private volatile boolean      initialized = false;
    private volatile boolean      closed      = false;
    private boolean               reRouting   = false;              // only used for init packet factory

    private ReentrantLock         statusLock  = new ReentrantLock();

    /*
     * Init.
     */
    public void init() throws Exception {
        if (initialized) {
            return;
        }
        statusLock.lock();
        try {
            if (initialized) {
                return;
            }
            initProperties();
            init_check();
            connectionFactory = ObConnectionFactory
                .newBuilder()
                .configWriteBufferWaterMark(getNettyBufferLowWatermark(),
                    getNettyBufferHighWatermark()).build();
            connectionFactory.init(new ConnectionEventHandler(new GlobalSwitch())); // Only for monitoring connection status
            realClient = new ObTableRemoting(new ObPacketFactory(reRouting));
            connectionPool = new ObTableConnectionPool(this, obTableConnectionPoolSize);
            connectionPool.init();
            initialized = true;
        } finally {
            statusLock.unlock();
        }
    }

    /*
     * Close.
     */
    public void close() {
        if (closed) {
            return;
        }
        statusLock.lock();
        try {
            if (closed) {
                return;
            }
            if (connectionPool != null) {
                connectionPool.close();
            }
            closed = true;
        } finally {
            statusLock.unlock();
        }
    }

    /*
     * Init check
     */
    private void init_check() throws IllegalArgumentException {
        if (obTableConnectionPoolSize <= 0) {
            throw new IllegalArgumentException("invalid obTableConnectionPoolSize: "
                                               + obTableConnectionPoolSize);
        } else if (ip.isEmpty() || port <= 0) {
            throw new IllegalArgumentException("invalid ip or port: " + ip + ":" + port);
        } else if (userName.isEmpty() || database.isEmpty()) {
            throw new IllegalArgumentException("invalid userName or database: " + userName + ":"
                                               + database);
        }
    }

    private void checkStatus() throws IllegalStateException {
        if (!initialized) {
            throw new IllegalStateException(" database [" + database + "] in ip [" + ip
                                            + "] port [" + port + "]  username [" + userName
                                            + "] is not initialized");
        }

        if (closed) {
            throw new IllegalStateException(" database [" + database + "] in ip [" + ip
                                            + "] port [" + port + "]  username [" + userName
                                            + "] is closed");
        }
    }

    private void initProperties() {
        obTableConnectTimeout = parseToInt(RPC_CONNECT_TIMEOUT.getKey(), obTableConnectTimeout);
        obTableConnectTryTimes = parseToInt(RPC_CONNECT_TRY_TIMES.getKey(), obTableConnectTryTimes);
        obTableExecuteTimeout = parseToInt(RPC_EXECUTE_TIMEOUT.getKey(), obTableExecuteTimeout);
        obTableLoginTimeout = parseToInt(RPC_LOGIN_TIMEOUT.getKey(), obTableLoginTimeout);
        obTableLoginTryTimes = parseToInt(RPC_LOGIN_TRY_TIMES.getKey(), obTableLoginTryTimes);
        obTableOperationTimeout = parseToLong(RPC_OPERATION_TIMEOUT.getKey(),
            obTableOperationTimeout);
        obTableConnectionPoolSize = parseToInt(SERVER_CONNECTION_POOL_SIZE.getKey(),
            obTableConnectionPoolSize);
        nettyBufferLowWatermark = parseToInt(NETTY_BUFFER_LOW_WATERMARK.getKey(),
            nettyBufferLowWatermark);
        nettyBufferHighWatermark = parseToInt(NETTY_BUFFER_HIGH_WATERMARK.getKey(),
            nettyBufferHighWatermark);
        nettyBlockingWaitInterval = parseToInt(NETTY_BLOCKING_WAIT_INTERVAL.getKey(),
            nettyBlockingWaitInterval);
        reRouting = parseToBoolean(SERVER_ENABLE_REROUTING.getKey(), reRouting);
    }

    /*
     * Query.
     */
    @Override
    public TableQuery query(String tableName) throws Exception {
        return new ObTableQueryImpl(tableName, this);
    }

    @Override
    public TableQuery queryByBatchV2(String tableName) throws Exception {
        return new ObTableQueryAsyncImpl(tableName, this);
    }

    public TableQuery queryByBatch(String tableName) throws Exception {
        return new QueryByBatch(query(tableName));
    }

    /*
     * Batch.
     */
    @Override
    public TableBatchOps batch(String tableName) {
        return new ObTableBatchOpsImpl(tableName, this);
    }

    public Map<String, Object> get(String tableName, Object rowkey, String[] columns)
                                                                                     throws RemotingException,
                                                                                     InterruptedException {
        return get(tableName, new Object[] { rowkey }, columns);
    }

    public Map<String, Object> get(String tableName, Object[] rowkeys, String[] columns)
                                                                                        throws RemotingException,
                                                                                        InterruptedException {
        ObTableOperationResult result = execute(tableName, ObTableOperationType.GET, rowkeys,
            columns, null, ObTableOptionFlag.DEFAULT, false, true);
        ObITableEntity entity = result.getEntity();
        return entity.getSimpleProperties();
    }

    /**
     * delete.
     */
    public Update update(String tableName) {
        return new Update(this, tableName);
    }

    /*
     * Update.
     */
    public long update(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                             throws RemotingException,
                                                                                             InterruptedException {
        ObTableOperationResult result = execute(tableName, ObTableOperationType.UPDATE, rowkeys,
            columns, values, ObTableOptionFlag.DEFAULT, false, true);
        return result.getAffectedRows();
    }

    /**
     * delete.
     */
    public Delete delete(String tableName) {
        return new Delete(this, tableName);
    }

    /*
     * Delete.
     */
    public long delete(String tableName, Object[] rowkeys) throws RemotingException,
                                                          InterruptedException {
        ObTableOperationResult result = execute(tableName, ObTableOperationType.DEL, rowkeys, null,
            null, ObTableOptionFlag.DEFAULT, false, true);
        return result.getAffectedRows();
    }

    /**
     * Insert.
     */
    public Insert insert(String tableName) {
        return new Insert(this, tableName);
    }

    /*
     * Insert.
     */
    public long insert(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                             throws RemotingException,
                                                                                             InterruptedException {
        ObTableOperationResult result = execute(tableName, ObTableOperationType.INSERT, rowkeys,
            columns, values, ObTableOptionFlag.DEFAULT, false, true);

        return result.getAffectedRows();
    }

    /**
     * Replace.
     */
    public Replace replace(String tableName) {
        return new Replace(this, tableName);
    }

    /*
     * Replace.
     */
    public long replace(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                              throws RemotingException,
                                                                                              InterruptedException {
        ObTableOperationResult result = execute(tableName, ObTableOperationType.REPLACE, rowkeys,
            columns, values, ObTableOptionFlag.DEFAULT, false, true);
        return result.getAffectedRows();
    }

    /**
     * Insert Or Update.
     */
    public InsertOrUpdate insertOrUpdate(String tableName) {
        return new InsertOrUpdate(this, tableName);
    }

    /*
     * Insert or update.
     */
    public long insertOrUpdate(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                                     throws RemotingException,
                                                                                                     InterruptedException {
        ObTableOperationResult result = execute(tableName, ObTableOperationType.INSERT_OR_UPDATE,
            rowkeys, columns, values, ObTableOptionFlag.DEFAULT, false, true);
        return result.getAffectedRows();
    }

    /**
     * Put.
     */
    public Put put(String tableName) {
        return new Put(this, tableName);
    }

    /**
     * increment.
     */
    public Increment increment(String tableName) {
        return new Increment(this, tableName);
    }

    @Override
    public Map<String, Object> increment(String tableName, Object[] rowkeys, String[] columns,
                                         Object[] values, boolean withResult) throws Exception {
        ObTableOperationResult result = execute(tableName, ObTableOperationType.INCREMENT, rowkeys,
            columns, values, ObTableOptionFlag.DEFAULT, withResult, true);
        ObITableEntity entity = result.getEntity();
        return entity.getSimpleProperties();
    }

    /**
     * append.
     */
    public Append append(String tableName) {
        return new Append(this, tableName);
    }

    @Override
    public Map<String, Object> append(String tableName, Object[] rowkeys, String[] columns,
                                      Object[] values, boolean withResult) throws Exception {
        ObTableOperationResult result = execute(tableName, ObTableOperationType.APPEND, rowkeys,
            columns, values, ObTableOptionFlag.DEFAULT, withResult, true);
        ObITableEntity entity = result.getEntity();
        return entity.getSimpleProperties();
    }

    /**
     * batch mutation.
     */
    public BatchOperation batchOperation(String tableName) {
        return new BatchOperation(this, tableName);
    }

    /**
     * Insert.
     */
    public CheckAndInsUp checkAndInsUp(String tableName, ObTableFilter filter,
                                       InsertOrUpdate insUp, boolean checkExists) {
        return new CheckAndInsUp(this, tableName, filter, insUp, checkExists);
    }

    /*
     * Execute.
     */
    public ObTableOperationResult execute(String tableName, ObTableOperationType type,
                                          Object[] rowkeys, String[] columns, Object[] values,
                                          ObTableOptionFlag optionFlag,
                                          boolean returningAffectedEntity,
                                          boolean returningAffectedRows) throws RemotingException,
                                                                        InterruptedException {
        checkStatus();
        ObTableOperationRequest request = ObTableOperationRequest.getInstance(tableName, type,
            rowkeys, columns, values, obTableOperationTimeout);
        request.setOptionFlag(optionFlag);
        request.setReturningAffectedEntity(returningAffectedEntity);
        request.setReturningAffectedRows(returningAffectedRows);
        ObPayload result = execute(request);
        checkObTableOperationResult(ip, port, result);
        return (ObTableOperationResult) result;
    }

    /*
     * Execute.
     */
    public ObPayload execute(final ObPayload request) throws RemotingException,
                                                     InterruptedException {

        ObTableConnection connection = null;
        try {
            connection = getConnection();
            // check connection is available, if not available, reconnect it
            connection.checkStatus();
        } catch (ConnectException ex) {
            // cannot connect to ob server, need refresh table location
            throw new ObTableServerConnectException(ex);
        } catch (ObTableServerConnectException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ObTableConnectionStatusException("check status failed", ex);
        }

        return realClient.invokeSync(connection, request, obTableExecuteTimeout);
    }

    private void checkObTableOperationResult(String ip, int port, Object result) {
        if (result == null) {
            throw new ObTableException("client get unexpected NULL result");
        }

        if (!(result instanceof ObTableOperationResult)) {
            throw new ObTableException("client get unexpected result: "
                                       + result.getClass().getName());
        }

        ObTableOperationResult obTableOperationResult = (ObTableOperationResult) result;
        ((ObTableOperationResult) result).setExecuteHost(ip);
        ((ObTableOperationResult) result).setExecutePort(port);
        ExceptionUtil.throwObTableException(ip, port, obTableOperationResult.getSequence(),
            obTableOperationResult.getUniqueId(), obTableOperationResult.getHeader().getErrno(),
            obTableOperationResult.getHeader().getErrMsg());
    }

    /*
     * Get ip.
     */
    public String getIp() {
        return ip;
    }

    /*
     * Set ip.
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    /*
     * Get port.
     */
    public int getPort() {
        return port;
    }

    /*
     * Set port.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /*
     * Get tenant name.
     */
    public String getTenantName() {
        return tenantName;
    }

    /*
     * Set tenant name.
     */
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    /*
     * Get user name.
     */
    public String getUserName() {
        return userName;
    }

    /*
     * Set user name.
     */
    public void setUserName(String userName) {
        this.userName = userName;
    }

    /*
     * Get password.
     */
    public String getPassword() {
        return password;
    }

    /*
     * Set password.
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /*
     * Get database.
     */
    public String getDatabase() {
        return database;
    }

    /*
     * Set database.
     */
    public void setDatabase(String database) {
        this.database = database;
    }

    /*
     * Get connection factory.
     */
    public ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /*
     * Set connection factory.
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /*
     * Get real client.
     */
    public ObTableRemoting getRealClient() {
        return realClient;
    }

    /*
     * Set real client.
     */
    public void setRealClient(ObTableRemoting realClient) {
        this.realClient = realClient;
    }

    /*
     * Get connection.
     */
    public ObTableConnection getConnection() throws Exception {
        ObTableConnection conn = connectionPool.getConnection();
        int count = 0;
        while (conn.getConnection() != null
               && (conn.getCredential() == null || conn.getCredential().length() == 0)
               && count < obTableConnectionPoolSize) {
            conn = connectionPool.getConnection();
            count++;
        }
        if (count == obTableConnectionPoolSize) {
            throw new ObTableException("all connection's credential is null");
        }
        return conn;
    }

    public static class Builder {

        private String     ip;
        private int        port;

        private String     tenantName;
        private String     userName;
        private String     password;
        private String     database;

        private Properties properties = new Properties();

        /*
         * Builder.
         */
        public Builder(String ip, int port) {
            this.ip = ip;
            this.port = port;
        }

        /*
         * Set login info.
         */
        public Builder setLoginInfo(String tenantName, String userName, String password,
                                    String database) {
            this.tenantName = tenantName;
            this.userName = userName;
            this.password = password;
            this.database = database;
            return this;
        }

        /*
         * Add propery.
         */
        public Builder addPropery(String key, String value) {
            this.properties.put(key, value);
            return this;
        }

        /*
         * Set properties.
         */
        public Builder setProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        /*
         * Build.
         */
        public ObTable build() throws Exception {
            ObTable obTable = new ObTable();
            obTable.setIp(ip);
            obTable.setPort(port);
            obTable.setTenantName(tenantName);
            obTable.setUserName(userName);
            obTable.setPassword(password);
            obTable.setDatabase(database);
            obTable.setProperties(properties);

            obTable.init();

            return obTable;
        }

    }

    /*
     * A simple pool for ObTableConnection with fix size.  Redesign it when we needs more.
     * The scheduling policy is round-robin. It's also simple but enough currently. Now, we promise sequential
     * consistency, while each thread call invokeSync for data access, ensuring the sequential consistency.
     * <p>
     * Thread safety:
     * (1) init and close require external synchronization or locking, which should be called only once.
     * (2) getConnection from the pool is synchronized, thus thread-safe, .
     * (3) ObTableConnection is shared and thread-safe, granted by underlying library.
     */
    private static class ObTableConnectionPool {
        private final int                    obTableConnectionPoolSize;
        private ObTable                      obTable;
        private volatile ObTableConnection[] connectionPool;
        // round-robin scheduling
        private AtomicLong                   turn = new AtomicLong(0);

        /*
         * Ob table connection pool.
         */
        public ObTableConnectionPool(ObTable obTable, int connectionPoolSize) {
            this.obTable = obTable;
            this.obTableConnectionPoolSize = connectionPoolSize;
            connectionPool = new ObTableConnection[connectionPoolSize];
        }

        /*
         * Init.
         */
        public void init() throws Exception {
            for (int i = 0; i < obTableConnectionPoolSize; i++) {
                connectionPool[i] = new ObTableConnection(obTable);
                connectionPool[i].init();
            }
        }

        /*
         * Get connection.
         */
        public ObTableConnection getConnection() {
            int round = (int) (turn.getAndIncrement() % obTableConnectionPoolSize);
            return connectionPool[round];
        }

        /*
         * Close.
         */
        public void close() {
            if (connectionPool == null) {
                return;
            }
            for (int i = 0; i < connectionPool.length; i++) {
                if (connectionPool[i] != null) {
                    connectionPool[i].close();
                    connectionPool[i] = null;
                }
            }
        }
    }

}

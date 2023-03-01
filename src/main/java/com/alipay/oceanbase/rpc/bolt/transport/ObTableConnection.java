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

package com.alipay.oceanbase.rpc.bolt.transport;

import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.login.ObTableLoginRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.login.ObTableLoginResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Security;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.oceanbase.rpc.util.TraceUtil;
import com.alipay.remoting.Connection;
import org.slf4j.Logger;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class ObTableConnection {

    private static final Logger LOGGER   = TableClientLoggerFactory
                                             .getLogger(ObTableConnection.class);
    private ObBytesString       credential;
    private long                tenantId = 1;                                    //默认值切勿不要随意改动
    private Connection          connection;
    private final ObTable       obTable;
    private long                uniqueId;                                        // as trace0 in rpc header
    private AtomicLong          sequence;                                        // as trace1 in rpc header

    public static long ipToLong(String strIp) {
        String[] ip = strIp.split("\\.");
        return (Long.parseLong(ip[0]) << 24) + (Long.parseLong(ip[1]) << 16)
               + (Long.parseLong(ip[2]) << 8) + (Long.parseLong(ip[3]));
    }

    /*
     * Ob table connection.
     */
    public ObTableConnection(ObTable obTable) {
        this.obTable = obTable;
    }

    /*
     * Init.
     */

    public void init() throws Exception {
        // sequence is a monotone increasing long value inside each connection
        sequence = new AtomicLong();
        connect();
        /* layout of uniqueId(64 bytes)
         * ip_: 32
         * port_: 16;
         * is_user_request_: 1;
         * is_ipv6_:1;
         * reserved_: 14;
         */
        long ip = ipToLong(connection.getLocalIP());
        long port = (long) connection.getLocalPort() << 32;
        long isUserRequest = (1l << (32 + 16));
        long reserved = 0;
        uniqueId = ip | port | isUserRequest | reserved;
    }

    private synchronized boolean connect() throws Exception {
        if (checkAvailable()) { // double check status available
            return false;
        }
        Exception cause = null;
        int tries = 0;
        int maxTryTimes = obTable.getObTableConnectTryTimes();
        for (; tries < maxTryTimes; tries++) {
            try {
                connection = obTable.getConnectionFactory().createConnection(obTable.getIp(),
                    obTable.getPort(), obTable.getObTableConnectTimeout());
                break;
            } catch (Exception e) {
                cause = e;
                LOGGER.warn(
                    "connect failed at " + tries + " try " + TraceUtil.formatIpPort(obTable), e);
            }
        }

        if (tries >= maxTryTimes) {
            throw new ObTableServerConnectException("connect failed after max " + maxTryTimes
                                                    + " tries " + TraceUtil.formatIpPort(obTable),
                cause);
        }

        // login the server. If login failed, close the raw connection to make the connection creation atomic.
        try {
            login();
        } catch (Exception e) {
            close();
            throw e;
        }
        return true;
    }

    private synchronized void login() throws Exception {
        ObTableLoginRequest request = new ObTableLoginRequest();
        request.setTenantName(obTable.getTenantName());
        request.setUserName(obTable.getUserName());
        request.setDatabaseName(obTable.getDatabase());
        generatePassSecret(request);
        ObTableLoginResult result;

        Exception cause = null;
        int tries = 0;
        int maxTryTimes = obTable.getObTableLoginTryTimes();
        for (; tries < maxTryTimes; tries++) {
            try {
                result = (ObTableLoginResult) obTable.getRealClient().invokeSync(this, request,
                    obTable.getObTableLoginTimeout());

                if (result != null && result.getCredential() != null
                    && result.getCredential().length() > 0) {
                    credential = result.getCredential();
                    tenantId = result.getTenantId();
                    break;
                }
            } catch (Exception e) {
                cause = e;
                String errMessage = "login failed at " + tries + " try "
                                    + TraceUtil.formatIpPort(obTable);
                LOGGER.warn(errMessage, e);
                // no need to retry when the username or password is wrong.
                if (e instanceof ObTableAuthException) {
                    throw new ObTableLoginException(errMessage, e);
                }
            }
        }

        if (tries >= maxTryTimes) {
            throw new ObTableServerConnectException("login failed after max " + maxTryTimes
                                                    + " tries " + TraceUtil.formatIpPort(obTable),
                cause);
        }
    }

    /*
     * Close.
     */
    public void close() {
        if (connection != null) {
            connection.close();
            connection = null;
            credential = null;
        }
    }

    /*
     * Check status.
     */
    public void checkStatus() throws Exception {
        if (connection == null) {
            reconnect("Check connection is null");
        }
        if (connection.getChannel() == null || !connection.getChannel().isActive()) {
            reconnect("Check connection failed for address: " + connection.getUrl());
        }
        if (!connection.getChannel().isWritable()) {
            LOGGER.warn("The connection might be write overflow : " + connection.getUrl());
            // Wait some interval for the case when a big package is blocking the buffer but server is ok.
            // Don't bother to call flush() here as we invoke writeAndFlush() when send request.
            Thread.sleep(obTable.getNettyBlockingWaitInterval());
            if (!connection.getChannel().isWritable()) {
                throw new ObTableConnectionUnWritableException(
                    "Check connection failed for address: " + connection.getUrl()
                            + ", maybe write overflow!");
            }
        }
    }

    private synchronized void reconnect(String msg) throws Exception {
        if (connect()) {
            LOGGER.warn("reconnect success. reconnect reason: [{}]", msg);
        } else {
            LOGGER.warn("connection maybe reconnect by other thread. reconnect reason: [{}]", msg);
        }
    }

    /*
     * Get credential.
     */
    public ObBytesString getCredential() {
        return credential;
    }

    /*
     * Set credential.
     */
    public void setCredential(ObBytesString credential) {
        this.credential = credential;
    }

    /*
     * Get tenant id.
     */
    public long getTenantId() {
        return tenantId;
    }

    /*
     * Set tenant id.
     */
    public void setTenantId(long tenantId) {
        this.tenantId = tenantId;
    }

    /*
     * Get connection.
     */
    public Connection getConnection() {
        return connection;
    }

    /*
     * Get ob table.
     */
    public ObTable getObTable() {
        return obTable;
    }

    private boolean checkAvailable() {
        if (connection == null) {
            System.out.println("my debug: connection is null, need do create connection");
            return false;
        }
        if (connection.getChannel() == null) {
            System.out.println("my debug: connection's channel is null, need do create connection");
            return false;
        }

        if (!connection.getChannel().isActive()) {
            System.out
                .println("my debug: connection's channel is inactive, need do create connection");
            return false;
        }

        if (credential == null) {
            System.out
                .println("my debug: connection's credential is null, need do create connection");
            return false;
        }
        return true;
    }

    private void generatePassSecret(ObTableLoginRequest request) {
        ObBytesString scramble = Security.getPasswordScramble(20);
        request.setPassSecret(Security.scramblePassword(obTable.getPassword().getBytes(),
            scramble.bytes));
        request.setPassScramble(scramble);
    }

    /*
     * Get unique id.
     */
    public long getUniqueId() {
        return uniqueId;
    }

    /*
     * Get next sequence.
     */
    public long getNextSequence() {
        return sequence.incrementAndGet();
    }

}

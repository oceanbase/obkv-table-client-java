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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.LocationUtil;
import com.alipay.oceanbase.rpc.protocol.payload.impl.login.ObTableLoginRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.login.ObTableLoginResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.util.*;
import com.alipay.remoting.Connection;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;

import java.net.ConnectException;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.MONITOR;
import static com.alipay.oceanbase.rpc.util.TraceUtil.formatTraceMessage;

public class ObTableConnection {

    private static final Logger LOGGER           = TableClientLoggerFactory
                                                     .getLogger(ObTableConnection.class);
    private static ObjectMapper objectMapper     = new ObjectMapper();
    private ObBytesString       credential;
    private long                tenantId         = 1;                                    //默认值切勿不要随意改动
    private Connection          connection;
    private final ObTable       obTable;
    private long                uniqueId;                                                // as trace0 in rpc header
    private AtomicLong          sequence;                                                // as trace1 in rpc header
    private AtomicBoolean       isReConnecting   = new AtomicBoolean(false);             // indicate is re-connecting or not
    private AtomicBoolean       isExpired        = new AtomicBoolean(false);
    private LocalDateTime       lastConnectionTime;
    private boolean             loginWithConfigs = false;

    public static long ipToLong(String strIp) {
        String[] ip = strIp.split("\\.");
        return (Long.parseLong(ip[0]) << 24) + (Long.parseLong(ip[1]) << 16)
               + (Long.parseLong(ip[2]) << 8) + (Long.parseLong(ip[3]));
    }

    public boolean checkExpired() {
        long maxConnectionTimes = obTable.getConnMaxExpiredTime();
        return lastConnectionTime.isBefore(LocalDateTime.now().minusMinutes(maxConnectionTimes));
    }

    public boolean isExpired() {
        return isExpired.get();
    }

    public void setExpired(boolean expired) {
        isExpired.set(expired);
    }

    public void enableLoginWithConfigs() {
        loginWithConfigs = true;
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
    }

    private boolean connect() throws Exception {
        if (checkAvailable()) { // double check status available
            return false;
        }
        final long start = System.currentTimeMillis();
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
        String endpoint = obTable.getIp() + ":" + obTable.getPort();
        MONITOR.info(logMessage(null, "CONNECT", endpoint, System.currentTimeMillis() - start));

        if (tries >= maxTryTimes) {
            LOGGER.warn("connect failed after max " + maxTryTimes + " tries "
                        + TraceUtil.formatIpPort(obTable));
            throw new ObTableServerConnectException("connect failed after max " + maxTryTimes
                                                    + " tries " + TraceUtil.formatIpPort(obTable),
                cause);
        }

        // login the server. If login failed, close the raw connection to make the connection creation atomic.
        try {
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
            login();
            lastConnectionTime = LocalDateTime.now();
        } catch (Exception e) {
            close();
            throw e;
        }
        return true;
    }

    private void login() throws Exception {
        final long start = System.currentTimeMillis();
        ObTableLoginRequest request = new ObTableLoginRequest();
        request.setTenantName(obTable.getTenantName());
        request.setUserName(obTable.getUserName());
        request.setDatabaseName(obTable.getDatabase());
        // When the caller doesn't provide any parameters, configsMap is empty.  
        // In this case, we don't generate any JSON to avoid creating an empty object.
        if (loginWithConfigs && !obTable.getConfigs().isEmpty()) {
            String configStr = objectMapper.writeValueAsString(obTable.getConfigs());
            request.setConfigsStr(configStr);
            loginWithConfigs = false;
        }
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
                    // Set version if missing
                    if (ObGlobal.obVsnMajor() == 0) {
                        // version should be set before login when direct mode
                        if (result.getServerVersion().isEmpty()) {
                            throw new RuntimeException(
                                "Failed to get server version from login result");
                        }
                        LocationUtil.parseObVerionFromLogin(result.getServerVersion());
                        LOGGER.info("The OB_VERSION parsed from login result is: {}",
                            ObGlobal.OB_VERSION);
                    }
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

        String endpoint = obTable.getIp() + ":" + obTable.getPort();
        MONITOR.info(logMessage(formatTraceMessage(request), "LOGIN", endpoint,
            System.currentTimeMillis() - start));
        if (tries >= maxTryTimes) {
            LOGGER.warn("login failed after max " + maxTryTimes + " tries "
                        + TraceUtil.formatIpPort(obTable));
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

    public void reConnectAndLogin(String msg) throws ObTableException {
        try {
            // 1. check the connection is available, force to close it
            if (checkAvailable()) {
                LOGGER.warn("The connection would be closed and reconnected if: "
                            + connection.getUrl());
                close();
            }
            // 2. reconnect
            reconnect(msg);
        } catch (ConnectException ex) {
            // cannot connect to ob server, need refresh table location
            throw new ObTableServerConnectException(ex);
        } catch (ObTableServerConnectException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new ObTableConnectionStatusException("check status failed, cause: " + ex.getMessage(), ex);
        }
    }

    /**
     * Reconnect current connection and login
     *
     * @param msg the reconnect reason
     * @exception Exception if connect successfully or connection already reconnected by others
     *                      throw exception if connect failed
     *
     */
    private void reconnect(String msg) throws Exception {
        if (isReConnecting.compareAndSet(false, true)) {
            try {
                if (connect()) {
                    LOGGER.warn("reconnect success. reconnect reason: [{}]", msg);
                } else {
                    LOGGER.info(
                        "connection maybe reconnect by other thread. reconnect reason: [{}]", msg);
                }
            } catch (Exception e) {
                throw e;
            } finally {
                if (!isReConnecting.compareAndSet(true, false)) {
                    LOGGER
                        .error(
                            "failed to set connecting to false after connect finished, reconnect reason: [{}]",
                            msg);
                }
            }
        } else {
            LOGGER.warn("There is someone connecting, no need reconnect");
            throw new ObTableException("This connection is already Connecting");
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
            return false;
        }
        if (connection.getChannel() == null) {
            return false;
        }

        if (!connection.getChannel().isActive()) {
            return false;
        }

        if (credential == null) {
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

    private String logMessage(String traceId, String methodName, String endpoint, long executeTime) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(endpoint)) {
            endpoint = endpoint.replaceAll(",", "#");
        }

        StringBuilder stringBuilder = new StringBuilder();
        if (traceId != null) {
            stringBuilder.append(traceId).append(" - ");
        }
        stringBuilder.append(methodName).append(",").append(endpoint).append(",")
            .append(executeTime);
        return stringBuilder.toString();
    }

}

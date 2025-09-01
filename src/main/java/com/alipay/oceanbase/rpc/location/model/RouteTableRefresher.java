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
package com.alipay.oceanbase.rpc.location.model;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableEntryRefreshException;
import com.alipay.oceanbase.rpc.exception.ObTableTryLockTimeoutException;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.location.LocationUtil;
import com.alipay.oceanbase.rpc.table.ObTable;
import org.slf4j.Logger;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.getLogger;

public class RouteTableRefresher {

    private static final Logger                                           logger    = getLogger(RouteTableRefresher.class);

    private static final int                                              failureLimit = 3;

    private static final String                                           sql = "select 'detect server alive' from dual";

    private final ObTableClient                                           tableClient;

    private final ObUserAuth                                              sysUA;

    private final ScheduledExecutorService                                scheduler = Executors.newScheduledThreadPool(2);

    private final static ConcurrentHashMap<ObServerAddr, Lock>            suspectLocks = new ConcurrentHashMap<>(); // ObServer -> access lock

    private final static ConcurrentHashMap<ObServerAddr, SuspectObServer> suspectServers = new ConcurrentHashMap<>(); // ObServer -> information structure

    private final static HashMap<ObServerAddr, Long>                      serverLastAccessTimestamps = new HashMap<>(); // ObServer -> last access timestamp

    public RouteTableRefresher(ObTableClient tableClient, ObUserAuth sysUA) {
        this.tableClient = tableClient;
        this.sysUA = sysUA;
    }

    /**
     * check whether observers have changed every 30 seconds
     * if changed, refresh in the background
     * */
    public void start() {
        scheduler.scheduleAtFixedRate(this::doRsListCheck, 30, 30, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::doCheckAliveTask, 1, 1, TimeUnit.SECONDS);
    }

    public void close() {
        try {
            scheduler.shutdown();
            // wait at most 1 seconds to close the scheduler
            if (!scheduler.awaitTermination(1, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("scheduler await for terminate interrupted: {}.", e.getMessage());
            scheduler.shutdownNow();
        }
    }

    /**
     * check whether root servers have been changed
     * if changed, update local connection cache
     * */
    private void doRsListCheck() {
        try {
            TableRoute tableRoute = tableClient.getTableRoute();
            ConfigServerInfo configServer = tableRoute.getConfigServerInfo();
            List<ObServerAddr> oldRsList = configServer.getRsList();
            ConfigServerInfo newConfigServer = LocationUtil.loadRsListForConfigServerInfo(
                configServer, tableClient.getParamURL(), tableClient.getDataSourceName(),
                tableClient.getRsListAcquireConnectTimeout(),
                tableClient.getRsListAcquireReadTimeout(), tableClient.getRsListAcquireTryTimes(),
                tableClient.getRsListAcquireRetryInterval());
            List<ObServerAddr> newRsList = newConfigServer.getRsList();
            boolean needRefresh = false;
            if (oldRsList.size() != newRsList.size()) {
                needRefresh = true;
            } else {
                for (ObServerAddr oldAddr : oldRsList) {
                    boolean found = false;
                    for (ObServerAddr newAddr : newRsList) {
                        if (oldAddr.equals(newAddr)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        needRefresh = true;
                        break;
                    }
                }
            }
            if (needRefresh) {
                newConfigServer = LocationUtil.refreshIDC2RegionMapFroConfigServerInfo(
                    newConfigServer, tableClient.getParamURL(),
                    tableClient.getRsListAcquireConnectTimeout(),
                    tableClient.getRsListAcquireReadTimeout(),
                    tableClient.getRsListAcquireTryTimes(),
                    tableClient.getRsListAcquireRetryInterval());
                tableRoute.setConfigServerInfo(newConfigServer);
                tableRoute.refreshRosterByRsList(newRsList);
            }
        } catch (Exception e) {
            logger.warn("RouteTableRefresher::doRsListCheck fail", e);
        }
    }

    private void doCheckAliveTask() {
        for (Map.Entry<ObServerAddr, SuspectObServer> entry : suspectServers.entrySet()) {
            try {
                checkAlive(entry.getKey());
            } catch (Exception e) {
                // silence resolving
                logger.warn("RouteTableRefresher::doCheckAliveTask fail, failed server: {}", entry.getKey().toString(), e);
            }
        }
    }

    private void checkAlive(ObServerAddr addr) {
        long connectTimeout = 1000L; // 1s
        long socketTimeout = 5000L; // 5s
        String url = LocationUtil.formatObServerUrl(addr, connectTimeout, socketTimeout);
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        try {
            connection = LocationUtil.getMetaRefreshConnection(url, sysUA);
            statement = connection.createStatement();
            rs = statement.executeQuery(sql);
            boolean alive = false;
            while (rs.next()) {
                String res = rs.getString("detect server alive");
                alive = res.equalsIgnoreCase("detect server alive");
            }
            if (alive) {
                removeFromSuspectIPs(addr);
            } else {
                calcFailureOrClearCache(addr);
            }
        } catch (Throwable t) {
            logger.debug("check alive failed, server: {}", addr.toString(), t);
            if (t instanceof SQLException) {
                // occurred during query
                calcFailureOrClearCache(addr);
            } if (t instanceof ObTableEntryRefreshException) {
                // occurred during connection construction
                ObTableEntryRefreshException e = (ObTableEntryRefreshException) t;
                if (e.isConnectInactive()) {
                    calcFailureOrClearCache(addr);
                } else {
                    logger.warn("background check-alive mechanic meet ObTableEntryRefreshException, server: {}", addr.toString(), t);
                    removeFromSuspectIPs(addr);
                }
            } else {
                // silence resolving
                logger.warn("background check-alive mechanic meet exception, server: {}", addr.toString(), t);
                removeFromSuspectIPs(addr);
            }
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public static void addIntoSuspectIPs(SuspectObServer server) throws InterruptedException {
        ObServerAddr addr = server.getAddr();
        if (suspectServers.get(addr) != null) {
            // already in the list, directly return
            return;
        }
        long addInterval = 20000L; // 20s
        Lock tempLock = new ReentrantLock();
        Lock lock = suspectLocks.putIfAbsent(addr, tempLock);
        lock = (lock == null) ? tempLock : lock;
        boolean acquired = false;
        try {
            int retryTimes = 0;
            while (true) {
                try {
                    acquired = lock.tryLock(1, TimeUnit.SECONDS);
                    if (!acquired) {
                        throw new ObTableTryLockTimeoutException("try to get suspect server lock timeout, timeout: 1s");
                    }
                    if (suspectServers.get(addr) != null) {
                        // already in the list, directly break
                        break;
                    }
                    Long lastServerAccessTs = serverLastAccessTimestamps.get(addr);
                    if (lastServerAccessTs != null) {
                        long interval = System.currentTimeMillis() - lastServerAccessTs;
                        if (interval < addInterval) {
                            // do not repeatedly add within 20 seconds since last adding
                            break;
                        }
                    }
                    logger.debug("add into suspect list, server: {}", addr);
                    suspectServers.put(addr, server);
                    serverLastAccessTimestamps.put(addr, server.getAccessTimestamp());
                    break;
                } catch (ObTableTryLockTimeoutException e) {
                    // if try lock timeout, need to retry
                    ++retryTimes;
                    logger.warn("wait to try lock to timeout 1s when add observer into suspect ips, server: {}, tryTimes: {}",
                            addr.toString(), retryTimes, e);
                }
            } // end while
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    private void removeFromSuspectIPs(ObServerAddr addr) {
        Lock lock = suspectLocks.get(addr);
        if (lock == null) {
            // lock must have been added before remove
            throw new ObTableUnexpectedException(String.format("ObServer [%s:%d] need to be add into suspect ips before remove",
                    addr.getIp(), addr.getSvrPort()));
        }
        boolean acquired = false;
        try {
            int retryTimes = 0;
            while (true) {
                try {
                    acquired = lock.tryLock(1, TimeUnit.SECONDS);
                    if (!acquired) {
                        throw new ObTableTryLockTimeoutException("try to get suspect server lock timeout, timeout: 1s");
                    }
                    // no need to remove lock
                    SuspectObServer server = suspectServers.remove(addr);
                    if (server != null) {
                        int failure = server.getFailure();
                        if (failure < failureLimit) {
                            ObTable obTable = tableClient.getTableRoute().getTableRoster().getTable(addr);
                            if (obTable != null && !obTable.isValid()) {
                                obTable.setValid();
                            }
                        }
                    }
                    logger.debug("removed server from suspect list: {}", addr);
                    break;
                } catch (ObTableTryLockTimeoutException e) {
                    // if try lock timeout, need to retry
                    ++retryTimes;
                    logger.warn("wait to try lock to timeout when add observer into suspect ips, server: {}, tryTimes: {}",
                            addr.toString(), retryTimes, e);
                } catch (InterruptedException e) {
                    // do not throw exception to user layer
                    // next background task will continue to remove it
                    logger.warn("waiting to get lock while interrupted by other threads", e);
                }
            }
        } finally {
            if (acquired) {
                lock.unlock();
            }
        }
    }

    private void calcFailureOrClearCache(ObServerAddr addr) {
        TableRoute tableRoute = tableClient.getTableRoute();
        SuspectObServer server = suspectServers.get(addr);
        server.incrementFailure();
        int failure = server.getFailure();
        if (failure >= failureLimit) {
            tableRoute.removeObServer(addr);
            removeFromSuspectIPs(addr);
        }
        logger.debug("background keep-alive mechanic failed to receive response, server: {}, failure: {}",
                addr, failure);
    }

    public static class SuspectObServer {
        private final  ObServerAddr addr;
        private final long accessTimestamp;
        private int failure;
        public SuspectObServer(ObServerAddr addr) {
            this.addr = addr;
            accessTimestamp = System.currentTimeMillis();
            failure = 0;
        }
        public ObServerAddr getAddr() {
            return this.addr;
        }
        public long getAccessTimestamp() {
            return this.accessTimestamp;
        }
        public int getFailure() {
            return this.failure;
        }
        public void incrementFailure() {
            ++failure;
        }
    }
}

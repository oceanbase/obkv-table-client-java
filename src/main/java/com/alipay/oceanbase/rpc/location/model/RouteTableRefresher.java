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

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.location.LocationUtil;
import org.slf4j.Logger;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.getLogger;

public class RouteTableRefresher {

    private static final Logger            logger    = getLogger(RouteTableRefresher.class);

    private final ObTableClient            tableClient;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public RouteTableRefresher(ObTableClient tableClient) {
        this.tableClient = tableClient;
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

    /**
     * check whether observers have changed every 30 seconds
     * if changed, refresh in the background
     * */
    public void start() {
        scheduler.scheduleAtFixedRate(this::doRsListCheck, 30, 30, TimeUnit.SECONDS);
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

}

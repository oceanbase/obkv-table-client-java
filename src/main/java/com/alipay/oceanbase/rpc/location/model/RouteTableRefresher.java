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

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.rpc.ObTableClient;
import org.slf4j.Logger;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.getLogger;

public class RouteTableRefresher {

    private static final Logger            logger    = getLogger(RouteTableRefresher.class);

    private final TableRoute               tableRoute;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public RouteTableRefresher(List<ObServerAddr> rsList, TableRoute tableRoute,
                               ObTableClient tableClient) {
        this.tableRoute = tableRoute;
    }

    /**
     * check whether observers have been changed
     * if changed, update local connection cache
     * */
    private void doRsListCheck() {
        try {
            tableRoute.refreshTableRosterIfChanged(false);
        } catch (Exception e) {
            logger.warn("RouteTableRefresher::doRsListCheck fail, error message: {}",
                e.getMessage());
        }
    }

    /**
     * check whether observers have changed every 3 minutes
     * if changed, refresh in the background
     * */
    public void start() {
        scheduler.scheduleAtFixedRate(this::doRsListCheck, 3, 3, TimeUnit.MINUTES);
    }

    public void close() {
        try {
            scheduler.shutdown();
            // wait at most 5 seconds to close the scheduler
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            logger.warn("scheduler await for terminate interrupted: {}.", e.getMessage());
            scheduler.shutdownNow();
        }
    }

}

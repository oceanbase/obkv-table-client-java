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

import com.alipay.oceanbase.rpc.ObTableClient;


import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import org.slf4j.Logger;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.getLogger;

public class RouteTableRefresher extends Thread{

    private static final Logger logger                                  = getLogger(RouteTableRefresher.class);

    private volatile AtomicBoolean isFinished = new AtomicBoolean(false);                                       // Thread end flag

    private final Semaphore semaphore = new Semaphore(0);

    private volatile ConcurrentLinkedQueue<ObPair<String, Boolean>> refreshTableTasks; // Task refresh queue

    ObTableClient client;

    private final Lock lock = new ReentrantLock();                  // Ensure the atomicity of the AddIfAbsent operation.

    public RouteTableRefresher(ObTableClient client){
        this.client = client;
    }

    public void finish() {
        isFinished.set(true);
    }

    @Override
    public void run() {
        refreshTableTasks = new ConcurrentLinkedQueue<>();
        while (!isFinished.get()) {
            try {
                semaphore.acquire();    // A semaphore is associated with a task; it ensures that only one task is processed at a time.
                logger.info("Thread name {}, id{} acquire semaphore, begin execute route refresher", currentThread().getName(), currentThread().getId());
            } catch (InterruptedException e) {
                logger.info("Thread name {}, id {} is interrupted", currentThread().getName(), currentThread().getId());
            }
            ObPair<String, Boolean> refreshTableTask = refreshTableTasks.peek();
            if (refreshTableTask != null && refreshTableTask.getRight()) {
                String tableName = refreshTableTask.getLeft();
                try {
                    logger.info("backgroundRefreshTableTask run refresh, table name {}", tableName);
                    TableEntry tableEntry = client.getOrRefreshTableEntry(tableName, true, false, false);
                    client.getTableLocations().put(refreshTableTask.getLeft(), tableEntry);
                } catch (Exception e) {
                    String message = "RefreshTableBackground run meet exception" + e.getMessage();
                    logger.warn(message);
                }
            }
            refreshTableTasks.poll();
        }
    }

    public void addTableIfAbsent(String tableName, Boolean isRefreshing){
        lock.lock();
        if (!refreshTableTasks.contains(new ObPair<>(tableName, isRefreshing))) {
            logger.info("add table {}, is refreshing {} to refresh task.", tableName, isRefreshing);
            refreshTableTasks.add(new ObPair<>(tableName,isRefreshing));
        }
        lock.unlock();
    }

    public void triggerRefreshTable() {
        semaphore.release();
    }
}

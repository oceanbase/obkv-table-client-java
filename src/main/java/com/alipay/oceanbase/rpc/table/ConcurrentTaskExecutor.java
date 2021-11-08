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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConcurrentTaskExecutor {

    private final CountDownLatch  taskCountDownLatch;
    private final AtomicBoolean   stopped;
    private final ExecutorService executor;

    private final List<Throwable> throwableList = Collections
                                                    .synchronizedList(new LinkedList<Throwable>());

    /**
     * Concurrent task executor.
     */
    public ConcurrentTaskExecutor(ExecutorService executor, int taskNum) {
        this.taskCountDownLatch = new CountDownLatch(taskNum);
        this.stopped = new AtomicBoolean(false);
        this.executor = executor;
    }

    /**
     * Execute.
     */
    public void execute(final ConcurrentTask task) {
        task.init(stopped, taskCountDownLatch);
        executor.execute(task);
    }

    /**
     * Is complete.
     */
    public boolean isComplete() {
        return taskCountDownLatch.getCount() == 0;
    }

    /**
     * Wait complete.
     */
    public boolean waitComplete(long timeout, TimeUnit unit) throws InterruptedException {
        return taskCountDownLatch.await(timeout, unit);
    }

    /**
     * Stop.
     */
    public void stop() {
        stopped.compareAndSet(false, true);
    }

    /**
     * Collect exceptions.
     */
    public void collectExceptions(Throwable e) {
        throwableList.add(e);
    }

    /**
     * Get throwable list.
     */
    public List<Throwable> getThrowableList() {
        return throwableList;
    }

}

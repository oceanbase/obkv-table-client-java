/*-
 * #%L
 * orm-bridge
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

package com.alipay.oceanbase.rpc.util;

import com.alipay.sofa.common.thread.SofaThreadPoolExecutor;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.OCEANBASE_TABLE_CLIENT_LOGGER_SPACE;

public class AsyncExecutor {

    private SofaThreadPoolExecutor executor;
    private AtomicLong             completeCount = new AtomicLong(0);
    private AtomicLong             taskCount     = new AtomicLong(0);
    private boolean                allTaskAdded;

    private CountDownLatch         finished      = new CountDownLatch(1);

    private Exception              exception     = null;

    /**
     * Async executor.
     */
    public AsyncExecutor() {
        this(4, 4, 1024);
    }

    /**
     * Async executor.
     */
    public AsyncExecutor(int corePoolSize, int maxPoolSize, int queueSize) {
        executor = new SofaThreadPoolExecutor(corePoolSize, maxPoolSize, 1000, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(queueSize), "asyncPool",
            OCEANBASE_TABLE_CLIENT_LOGGER_SPACE);
    }

    /**
     * Wait complete.
     */
    public void waitComplete() throws Exception {
        startMonitor();
        this.allTaskAdded = true;

        finished.await();
        if (exception != null) {
            throw exception;
        }
    }

    /**
     * Add task.
     */
    public void addTask(final Runnable runnable) {
        executor.execute(new Runnable() {
            /**
             * Run.
             */
            @Override
            public void run() {
                try {
                    runnable.run();
                } catch (Exception ex) {
                    exception = ex;
                    finishIt(); // meet exception, stop executor
                } finally {
                    completeCount.incrementAndGet();
                }
            }
        });

        taskCount.incrementAndGet();
    }

    /**
     * monitor executor tasks, if finished, shutdown executor
     */
    private void startMonitor() {
        new Thread(new Runnable() {
            /**
             * Run.
             */
            @Override
            public void run() {
                while (true) {
                    try {
                        if (allTaskAdded) {
                            if (taskCount.get() == completeCount.get()) {
                                finishIt();
                                break;
                            }
                        }
                        Thread.sleep(300);
                    } catch (InterruptedException e) {
                    }
                }
            }
        }).start();
    }

    private void finishIt() {
        completeCount.set(taskCount.get());
        finished.countDown();
        executor.shutdown();
    }

}

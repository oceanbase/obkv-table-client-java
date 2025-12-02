/*-
 * #%L
 * zdal-common
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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class ConcurrentTask implements Runnable {

    private CountDownLatch         countDownLatch;
    private AtomicBoolean          stopped;
    private ConcurrentTaskExecutor executor;

    /*
     * Run.
     */
    @Override
    public void run() {
        try {
            if (stopped.get()) {
                return;
            }
            doTask();
        } catch (Exception e) {
            if (executor != null) {
                executor.collectExceptions(e);
            }
            throw new RuntimeException(e);
        } finally {
            countDownLatch.countDown();
        }
    }

    void init(AtomicBoolean status, CountDownLatch countDownLatch) {
        this.stopped = status;
        this.countDownLatch = countDownLatch;
    }

    void init(ConcurrentTaskExecutor executor) {
        this.executor = executor;
    }

    public abstract void doTask();

}

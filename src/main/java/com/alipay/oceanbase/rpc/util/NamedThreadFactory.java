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

package com.alipay.oceanbase.rpc.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread factory to name the thread purposely
 */
public class NamedThreadFactory implements ThreadFactory {

    private static final AtomicInteger poolNumber   = new AtomicInteger(1);
    private final AtomicInteger        threadNumber = new AtomicInteger(1);
    private final ThreadGroup          group;
    private final String               namePrefix;
    private final boolean              isDaemon;

    /**
     * Named thread factory.
     */
    public NamedThreadFactory() {
        this("ThreadPool");
    }

    /**
     * Named thread factory.
     * @param name thread factory name
     */
    public NamedThreadFactory(String name) {
        this(name, false);
    }

    /**
     * Named thread factory.
     * @param preffix thread name prefix
     * @param daemon is daemon
     */
    public NamedThreadFactory(String preffix, boolean daemon) {
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        namePrefix = preffix + "-" + poolNumber.getAndIncrement() + "-thread-";
        isDaemon = daemon;
    }

    /**
     * Create a thread.
     * @see java.util.concurrent.ThreadFactory#newThread(java.lang.Runnable)
     * @param r runnable task
     * @return thread handle
     */
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
        t.setDaemon(isDaemon);
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }
        return t;
    }

}

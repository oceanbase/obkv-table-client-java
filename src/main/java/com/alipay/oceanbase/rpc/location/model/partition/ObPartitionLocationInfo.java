/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.location.model.partition;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alipay.oceanbase.rpc.protocol.payload.Constants.INVALID_LS_ID;

public class ObPartitionLocationInfo {
    private ObPartitionLocation   partitionLocation   = null;
    private Long                  tabletLsId          = INVALID_LS_ID;
    private Long                  lastUpdateTime      = 0L;
    public ReentrantReadWriteLock rwLock              = new ReentrantReadWriteLock();
    public AtomicBoolean          initialized         = new AtomicBoolean(false);
    public final CountDownLatch   initializationLatch = new CountDownLatch(1);

    public ReentrantLock          refreshLock         = new ReentrantLock();

    public ObPartitionLocation getPartitionLocation() {
        rwLock.readLock().lock();
        try {
            return partitionLocation;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void updateLocation(ObPartitionLocation newLocation, Long tabletLsId) {
        rwLock.writeLock().lock();
        try {
            this.partitionLocation = newLocation;
            this.tabletLsId = tabletLsId;
            this.lastUpdateTime = System.currentTimeMillis();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Long getTabletLsId() {
        rwLock.readLock().lock();
        try {
            return tabletLsId;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public Long getLastUpdateTime() {
        rwLock.readLock().lock();
        try {
            return lastUpdateTime;
        } finally {
            rwLock.readLock().unlock();
        }
    }
}

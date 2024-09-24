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

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.alipay.oceanbase.rpc.protocol.payload.Constants.OB_INVALID_ID;

// 这个类不做线程安全之类的处理
public class ObPartitionLocationInfo {
    private ObPartitionLocation   partitionLocation = null;
    private Long                  tabletLsId        = OB_INVALID_ID;
    private Long                  lastUpdateTime;                                  // 最后更新时间  
    public ReentrantReadWriteLock rwLock            = new ReentrantReadWriteLock(); // 读写锁  
    public AtomicBoolean          initialized       = new AtomicBoolean(false);

    public ObPartitionLocationInfo() {
        this.lastUpdateTime = System.currentTimeMillis(); // 初始化为当前时间  
    }

    public ObPartitionLocation getPartitionLocation() {
        rwLock.readLock().lock();
        try {
            return partitionLocation;
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void setPartitionLocation(ObPartitionLocation partitionLocation) {
        this.partitionLocation = partitionLocation;
    }

    public void updateLocation(ObPartitionLocation newLocation) {
        rwLock.writeLock().lock();
        try {
            this.partitionLocation = newLocation;
            this.lastUpdateTime = System.currentTimeMillis();
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public Long getTabletLsId() {
        return tabletLsId;
    }

    public void setTabletLsId(Long tabletLsId) {
        this.tabletLsId = tabletLsId;
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

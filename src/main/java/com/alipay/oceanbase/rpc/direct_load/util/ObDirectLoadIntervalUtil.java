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

package com.alipay.oceanbase.rpc.direct_load.util;

import java.util.concurrent.atomic.AtomicLong;

public class ObDirectLoadIntervalUtil {

    private AtomicLong lastTime = new AtomicLong(0);
    private AtomicLong count    = new AtomicLong(0);

    public boolean reachTimeInterval(long timeMillis) {
        long curTime = System.currentTimeMillis();
        long oldTime = lastTime.get();
        if (timeMillis + oldTime <= curTime && lastTime.compareAndSet(oldTime, curTime)) {
            return true;
        }
        return false;
    }

    public boolean reachCountInterval(long c) {
        return (count.incrementAndGet() % c == 0);
    }

}

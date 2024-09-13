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

package com.alipay.oceanbase.rpc.direct_load;

import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;

public class ObDirectLoadRuntimeInfo {

    protected long                  lastScheduledTime  = 0;
    protected ObDirectLoadException lastScheduledCause = null;
    protected int                   scheduledCount     = 0;

    public long getLastScheduledTime() {
        return lastScheduledTime;
    }

    public void setLastScheduledTime(long scheduledTime) {
        this.lastScheduledTime = scheduledTime;
    }

    public ObDirectLoadException getLastScheduledCause() {
        return lastScheduledCause;
    }

    public void setLastScheduledCause(ObDirectLoadException cause) {
        this.lastScheduledCause = cause;
    }

    public int getScheduledCount() {
        return scheduledCount;
    }

    public void incScheduledCount() {
        this.scheduledCount++;
    }

}

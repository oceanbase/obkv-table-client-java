/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObLoadDupActionType;

public class ObDirectLoadParameter {

    private long                parallel         = 0;
    private long                maxErrorRowCount = 0;
    private ObLoadDupActionType dupAction        = ObLoadDupActionType.INVALID_MODE;
    private long                timeout          = 0;
    private long                heartBeatTimeout = 0;

    public ObDirectLoadParameter() {
    }

    public ObDirectLoadParameter(long parallel, long maxErrorRowCount,
                                 ObLoadDupActionType dupAction, long timeout, long heartBeatTimeout) {
        this.parallel = parallel;
        this.maxErrorRowCount = maxErrorRowCount;
        this.dupAction = dupAction;
        this.timeout = timeout;
        this.heartBeatTimeout = heartBeatTimeout;
    }

    public long getParallel() {
        return parallel;
    }

    public ObDirectLoadParameter setParallel(int parallel) {
        this.parallel = parallel;
        return this;
    }

    public long getMaxErrorRowCount() {
        return maxErrorRowCount;
    }

    public ObDirectLoadParameter setMaxErrorRowCount(long maxErrorRowCount) {
        this.maxErrorRowCount = maxErrorRowCount;
        return this;
    }

    public ObLoadDupActionType getDupAction() {
        return dupAction;
    }

    public ObDirectLoadParameter setDupAction(ObLoadDupActionType dupAction) {
        this.dupAction = dupAction;
        return this;
    }

    public long getTimeout() {
        return timeout;
    }

    public ObDirectLoadParameter setTimeout(long timeout) {
        this.timeout = timeout;
        return this;
    }

    public long getHeartBeatTimeout() {
        return heartBeatTimeout;
    }

    public ObDirectLoadParameter setHeartBeatTimeout(long heartBeatTimeout) {
        this.heartBeatTimeout = heartBeatTimeout;
        return this;
    }

}

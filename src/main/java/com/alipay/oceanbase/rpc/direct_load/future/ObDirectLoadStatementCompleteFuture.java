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

package com.alipay.oceanbase.rpc.direct_load.future;

import java.util.concurrent.TimeUnit;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadRuntimeInfo;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadInterruptedException;

abstract class ObDirectLoadStatementCompleteFuture implements ObDirectLoadStatementFuture {

    protected final ObDirectLoadStatement statement;

    ObDirectLoadStatementCompleteFuture(ObDirectLoadStatement statement) {
        this.statement = statement;
    }

    @Override
    public ObDirectLoadStatement getStatement() {
        return this.statement;
    }

    @Override
    public boolean isDone() {
        return true;
    }

    @Override
    public void await() throws ObDirectLoadInterruptedException {
        return;
    }

    @Override
    public boolean await(long timeoutMillis) throws ObDirectLoadInterruptedException {
        return true;
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws ObDirectLoadInterruptedException {
        return true;
    }

    @Override
    public ObDirectLoadRuntimeInfo getRuntimeInfo() {
        return new ObDirectLoadRuntimeInfo();
    }

}

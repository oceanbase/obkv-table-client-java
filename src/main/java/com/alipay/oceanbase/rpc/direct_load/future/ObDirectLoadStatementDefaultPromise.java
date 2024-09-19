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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadLogger;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadRuntimeInfo;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.*;

public class ObDirectLoadStatementDefaultPromise implements ObDirectLoadStatementPromise {

    protected final ObDirectLoadStatement statement;
    protected final ObDirectLoadLogger    logger;

    private CountDownLatch                waiter     = new CountDownLatch(1);

    private static int                    NONE       = 0;
    private static int                    SET_RESULT = 1;                      // 设置结果中
    private static int                    SUCC       = 2;                      // 成功
    private static int                    FAIL       = 3;                      // 失败
    private AtomicInteger                 resultFlag = new AtomicInteger(NONE);
    private ObDirectLoadException         cause      = null;

    public ObDirectLoadStatementDefaultPromise(ObDirectLoadStatement statement) {
        this.statement = statement;
        this.logger = statement.getLogger();
    }

    @Override
    public ObDirectLoadStatement getStatement() {
        return this.statement;
    }

    @Override
    public boolean isDone() {
        return (resultFlag.get() > SET_RESULT);
    }

    @Override
    public boolean isSuccess() {
        return (resultFlag.get() == SUCC);
    }

    @Override
    public ObDirectLoadException cause() {
        return this.cause;
    }

    @Override
    public void await() throws ObDirectLoadInterruptedException {
        try {
            waiter.await();
        } catch (InterruptedException e) {
            throw ObDirectLoadExceptionUtil.convertException(e);
        }
    }

    @Override
    public boolean await(long timeoutMillis) throws ObDirectLoadInterruptedException {
        try {
            return waiter.await(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw ObDirectLoadExceptionUtil.convertException(e);
        }
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws ObDirectLoadInterruptedException {
        try {
            return waiter.await(timeout, unit);
        } catch (InterruptedException e) {
            throw ObDirectLoadExceptionUtil.convertException(e);
        }
    }

    @Override
    public ObDirectLoadStatementPromise setSuccess() {
        if (resultFlag.compareAndSet(NONE, SET_RESULT)) {
            resultFlag.set(SUCC);
            waiter.countDown();
        }
        return this;
    }

    @Override
    public ObDirectLoadStatementPromise setFailure(ObDirectLoadException cause) {
        if (resultFlag.compareAndSet(NONE, SET_RESULT)) {
            this.cause = cause;
            resultFlag.set(FAIL);
            waiter.countDown();
        }
        return this;
    }

    @Override
    public ObDirectLoadRuntimeInfo getRuntimeInfo() {
        return new ObDirectLoadRuntimeInfo();
    }

}

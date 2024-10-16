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

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadExceptionUtil;
import com.alipay.oceanbase.rpc.util.NamedThreadFactory;
import com.alipay.remoting.util.NettyEventLoopUtil;

import io.netty.channel.EventLoopGroup;

public abstract class ObDirectLoadStatementAsyncPromiseTask extends
                                                           ObDirectLoadStatementPromiseTask {

    private static final int            backgroundThreadCount = 2;
    private static final EventLoopGroup eventLoopGroup        = NettyEventLoopUtil
                                                                  .newEventLoopGroup(
                                                                      backgroundThreadCount,
                                                                      new NamedThreadFactory(
                                                                          "direct-load", true));

    public ObDirectLoadStatementAsyncPromiseTask(ObDirectLoadStatement statement) {
        super(statement);
    }

    public void submit() throws ObDirectLoadException {
        try {
            eventLoopGroup.submit(this);
        } catch (Exception e) {
            ObDirectLoadException cause = ObDirectLoadExceptionUtil.convertException(e);
            setFailure(cause);
            throw cause;
        }
    }

    protected void schedule(long delay, TimeUnit unit) throws ObDirectLoadException {
        try {
            eventLoopGroup.schedule(this, delay, unit);
        } catch (Exception e) {
            ObDirectLoadException cause = ObDirectLoadExceptionUtil.convertException(e);
            // setFailure(cause);
            throw cause;
        }
    }

    protected void schedule(long delayMillis) throws ObDirectLoadException {
        schedule(delayMillis, TimeUnit.MILLISECONDS);
    }

}

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
import com.alipay.oceanbase.rpc.direct_load.exception.*;

public interface ObDirectLoadStatementFuture {

    /**
     * 获取关联的stmt对象
     */
    ObDirectLoadStatement getStatement();

    /**
     * 异步任务是否完成, 结果可能是成功、失败、取消
     * 
     * @return {@code true} 异步任务完成 {@code false} 异步任务未完成
     */
    boolean isDone();

    /**
     * 异步任务是否被取消
     * 
     * @return {@code true} 异步任务被取消
     */
    // boolean isCancelled();

    /**
     * 异步任务是否成功
     * 
     * @return {@code true} 异步任务成功
     */
    boolean isSuccess();

    /**
     * 异步任务失败的原因
     */
    ObDirectLoadException cause();

    /**
     * 
     * 添加监听器.异步任务完成时会通知监听器.
     */
    // ObDirectLoadStatementFuture addListener(ObDirectLoadStatementFutureListener listener);

    /**
     * 等待异步任务完成
     */
    void await() throws ObDirectLoadInterruptedException;

    /**
     * 等待异步任务完成
     * 
     * @param timeoutMillis 超时时间, 单位:ms
     * 
     * @return {@code true} 异步任务完成 {@code false} 等待超时
     */
    boolean await(long timeoutMillis) throws ObDirectLoadInterruptedException;

    /**
     * 等待异步任务完成
     * 
     * @param timeout 超时时间
     * @param unit 超时时间单位
     * 
     * @return {@code true} 异步任务完成 {@code false} 等待超时
     */
    boolean await(long timeout, TimeUnit unit) throws ObDirectLoadInterruptedException;

    /**
     * 取消异步任务
     * 
     * @param mayInterruptIfRunning 是否中断执行中的异步任务
     * 
     * @return {@code true} 取消成功 {@code false} 取消失败
     */
    // boolean cancel(boolean mayInterruptIfRunning);

    /**
     * 获取运行时信息
     */
    ObDirectLoadRuntimeInfo getRuntimeInfo();

}

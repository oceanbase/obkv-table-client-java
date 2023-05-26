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

package com.alipay.oceanbase.rpc.bolt.transport;

import com.alipay.oceanbase.rpc.bolt.protocol.ObTablePacket;
import com.alipay.remoting.InvokeCallback;
import com.alipay.remoting.InvokeContext;
import com.alipay.remoting.InvokeFuture;
import com.alipay.remoting.RemotingCommand;
import com.alipay.oceanbase.rpc.exception.ObTableTimeoutExcetion;
import io.netty.util.Timeout;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ObClientFuture implements InvokeFuture {

    private CountDownLatch  waiter = new CountDownLatch(1);
    private RemotingCommand response;
    private int             channelId;

    // BY_WORKER indicate response must be release by worker itself.
    // BY_BACKGROUND indicate response must be release by background decoder thread
    private static int      INIT = 0;
    private static int      BY_WORKER = 1;
    private static int      BY_BACKGROUND = 2;

    private AtomicInteger   releaseFlag = new AtomicInteger(INIT);

    /*
     * Ob client future.
     */
    public ObClientFuture(int channelId) {
        this.channelId = channelId;
    }

    /*
     * Wait response.
     */
    @Override
    public RemotingCommand waitResponse(long timeoutMillis) throws InterruptedException {
        try {
            if (waiter.await(timeoutMillis, TimeUnit.MILLISECONDS) || !releaseFlag.compareAndSet(INIT, BY_BACKGROUND)) {
                return response;
            } else {
                return null;
            }
        } catch (InterruptedException e) {
            releaseFlag.set(BY_BACKGROUND);
            if (response instanceof ObTablePacket) {
               ((ObTablePacket) response).releaseByteBuf();
            }
            throw e;
        } finally {}
    }

    /*
     * Wait response.
     */
    @Override
    public RemotingCommand waitResponse() throws InterruptedException {
        waiter.await();
        return response;
    }

    /*
     * Put response.
     */
    @Override
    public void putResponse(RemotingCommand response) {
        this.response = response;
        waiter.countDown();
        if (!releaseFlag.compareAndSet(INIT, BY_WORKER)) {
            if (response instanceof ObTablePacket) {
                ((ObTablePacket) response).releaseByteBuf();
            }
        }
    }

    /*
     * Invoke id.
     */
    @Override
    public int invokeId() {
        return channelId;
    }

    /*
     * Is done.
     */
    @Override
    public boolean isDone() {
        return this.waiter.getCount() == 0;
    }

    /*
     * Create connection closed response.
     */
    @Override
    public RemotingCommand createConnectionClosedResponse(InetSocketAddress responseHost) {
        return null;
    }

    /*
     * Execute invoke callback.
     */
    @Override
    public void executeInvokeCallback() {

    }

    /*
     * Try async execute invoke callback abnormally.
     */
    @Override
    public void tryAsyncExecuteInvokeCallbackAbnormally() {

    }

    /*
     * Set cause.
     */
    @Override
    public void setCause(Throwable cause) {

    }

    /*
     * Get cause.
     */
    @Override
    public Throwable getCause() {
        return null;
    }

    /*
     * Get invoke callback.
     */
    @Override
    public InvokeCallback getInvokeCallback() {
        return null;
    }

    /*
     * Add timeout.
     */
    @Override
    public void addTimeout(Timeout timeout) {

    }

    /*
     * Cancel timeout.
     */
    @Override
    public void cancelTimeout() {

    }

    /*
     * Get app class loader.
     */
    @Override
    public ClassLoader getAppClassLoader() {
        return null;
    }

    /*
     * Get protocol code.
     */
    @Override
    public byte getProtocolCode() {
        return 0;
    }

    /*
     * Set invoke context.
     */
    @Override
    public void setInvokeContext(InvokeContext invokeContext) {

    }

    /*
     * Get invoke context.
     */
    @Override
    public InvokeContext getInvokeContext() {
        return null;
    }
}

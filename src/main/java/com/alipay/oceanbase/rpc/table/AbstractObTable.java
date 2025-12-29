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

package com.alipay.oceanbase.rpc.table;

import static com.alipay.oceanbase.rpc.property.Property.*;

public abstract class AbstractObTable extends AbstractTable {

    protected int  obTableConnectTimeout     = RPC_CONNECT_TIMEOUT.getDefaultInt();

    protected int  obTableConnectTryTimes    = RPC_CONNECT_TRY_TIMES.getDefaultInt();

    protected int  obTableExecuteTimeout     = RPC_EXECUTE_TIMEOUT.getDefaultInt();

    protected int  obTableLoginTimeout       = RPC_LOGIN_TIMEOUT.getDefaultInt();

    protected int  obTableLoginTryTimes      = RPC_LOGIN_TRY_TIMES.getDefaultInt();

    protected long obTableOperationTimeout   = RPC_OPERATION_TIMEOUT.getDefaultLong();

    protected int  obTableConnectionPoolSize = SERVER_CONNECTION_POOL_SIZE.getDefaultInt();

    protected int  nettyBufferLowWatermark   = NETTY_BUFFER_LOW_WATERMARK.getDefaultInt();

    protected int  nettyBufferHighWatermark  = NETTY_BUFFER_HIGH_WATERMARK.getDefaultInt();

    protected int  nettyBlockingWaitInterval = NETTY_BLOCKING_WAIT_INTERVAL.getDefaultInt();

    protected long maxConnExpiredTime        = MAX_CONN_EXPIRED_TIME.getDefaultLong();

    protected boolean timeTraceEnabled       = RPC_TIME_TRACE_ENABLED.getDefaultBoolean();

    /*
     * Get ob table connect try times.
     */
    public int getObTableConnectTryTimes() {
        return obTableConnectTryTimes;
    }

    /*
     * Set ob table connect try times.
     */
    public void setObTableConnectTryTimes(int obTableConnectTryTimes) {
        this.obTableConnectTryTimes = obTableConnectTryTimes;
    }

    /*
     * Get ob table login try times.
     */
    public int getObTableLoginTryTimes() {
        return obTableLoginTryTimes;
    }

    /*
     * Set ob table login try times.
     */
    public void setObTableLoginTryTimes(int obTableLoginTryTimes) {
        this.obTableLoginTryTimes = obTableLoginTryTimes;
    }

    /*
     * Get ob table connection pool size.
     */
    public int getObTableConnectionPoolSize() {
        return obTableConnectionPoolSize;
    }

    /*
     * Set ob table connection pool size.
     */
    public void setObTableConnectionPoolSize(int obTableConnectionPoolSize) {
        this.obTableConnectionPoolSize = obTableConnectionPoolSize;
    }

    /*
     * Get ob table connect timeout.
     */
    public int getObTableConnectTimeout() {
        return obTableConnectTimeout;
    }

    /*
     * Set ob table connect timeout.
     */
    public void setObTableConnectTimeout(int obTableConnectTimeout) {
        this.obTableConnectTimeout = obTableConnectTimeout;
    }

    /*
     * Get ob table execute timeout.
     */
    public int getObTableExecuteTimeout() {
        return obTableExecuteTimeout;
    }

    /*
     * Set ob table execute timeout.
     */
    public void setObTableExecuteTimeout(int obTableExecuteTimeout) {
        this.obTableExecuteTimeout = obTableExecuteTimeout;
    }

    /*
     * Get ob table login timeout.
     */
    public int getObTableLoginTimeout() {
        return obTableLoginTimeout;
    }

    /*
     * Set ob table login timeout.
     */
    public void setObTableLoginTimeout(int obTableLoginTimeout) {
        this.obTableLoginTimeout = obTableLoginTimeout;
    }

    /*
     * Get ob table operation timeout.
     */
    public long getObTableOperationTimeout() {
        return obTableOperationTimeout;
    }

    /*
     * Set ob table operation timeout.
     */
    public void setObTableOperationTimeout(long obTableOperationTimeout) {
        this.obTableOperationTimeout = obTableOperationTimeout;
    }

    /*
     * Get netty buffer low watermark.
     */
    public int getNettyBufferLowWatermark() {
        return nettyBufferLowWatermark;
    }

    /*
     * Get netty buffer high watermark.
     */
    public int getNettyBufferHighWatermark() {
        return nettyBufferHighWatermark;
    }

    /*
     * Get netty blocking wait interval.
     */
    public int getNettyBlockingWaitInterval() {
        return nettyBlockingWaitInterval;
    }

    /*
     * Get connection max expired time
     */
    public long getConnMaxExpiredTime() {
        return maxConnExpiredTime;
    }

    /*
     * Check if time trace is enabled.
     */
    public boolean isTimeTraceEnabled() {
        return timeTraceEnabled;
    }

    /*
     * Set time trace enabled.
     */
    public void setTimeTraceEnabled(boolean timeTraceEnabled) {
        this.timeTraceEnabled = timeTraceEnabled;
    }
}

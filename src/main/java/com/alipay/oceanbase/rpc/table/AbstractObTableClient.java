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

import java.util.concurrent.ExecutorService;

import static com.alipay.oceanbase.rpc.property.Property.*;

public abstract class AbstractObTableClient extends AbstractTable {

    protected long            metadataRefreshInterval                   = METADATA_REFRESH_INTERVAL
                                                                            .getDefaultLong();

    protected long            metadataRefreshLockTimeout                = METADATA_REFRESH_LOCK_TIMEOUT
                                                                            .getDefaultLong();

    protected int             rsListAcquireConnectTimeout               = RS_LIST_ACQUIRE_CONNECT_TIMEOUT
                                                                            .getDefaultInt();

    protected int             rsListAcquireReadTimeout                  = RS_LIST_ACQUIRE_READ_TIMEOUT
                                                                            .getDefaultInt();

    protected int             rsListAcquireTryTimes                     = RS_LIST_ACQUIRE_TRY_TIMES
                                                                            .getDefaultInt();

    protected long            rsListAcquireRetryInterval                = RS_LIST_ACQUIRE_RETRY_INTERVAL
                                                                            .getDefaultLong();

    protected long            tableEntryAcquireConnectTimeout           = TABLE_ENTRY_ACQUIRE_CONNECT_TIMEOUT
                                                                            .getDefaultLong();

    protected long            tableEntryAcquireSocketTimeout            = TABLE_ENTRY_ACQUIRE_SOCKET_TIMEOUT
                                                                            .getDefaultLong();

    protected long            tableEntryRefreshIntervalBase             = TABLE_ENTRY_REFRESH_INTERVAL_BASE
                                                                            .getDefaultLong();

    protected long            tableEntryRefreshIntervalCeiling          = TABLE_ENTRY_REFRESH_INTERVAL_CEILING
                                                                            .getDefaultLong();

    protected boolean         tableEntryRefreshIntervalWait             = TABLE_ENTRY_REFRESH_INTERVAL_WAIT
                                                                            .getDefaultBoolean();

    protected int             tableEntryRefreshTryTimes                 = TABLE_ENTRY_REFRESH_TRY_TIMES
                                                                            .getDefaultInt();

    protected long            tableEntryRefreshLockTimeout              = TABLE_ENTRY_REFRESH_LOCK_TIMEOUT
                                                                            .getDefaultLong();

    protected long            ODPTableEntryRefreshLockTimeout           = ODP_TABLE_ENTRY_REFRESH_LOCK_TIMEOUT
                                                                            .getDefaultLong();

    protected int             tableEntryRefreshContinuousFailureCeiling = TABLE_ENTRY_REFRESH_CONTINUOUS_FAILURE_CEILING
                                                                            .getDefaultInt();

    protected long            serverAddressPriorityTimeout              = SERVER_ADDRESS_PRIORITY_TIMEOUT
                                                                            .getDefaultLong();
    protected long            serverAddressCachingTimeout               = SERVER_ADDRESS_CACHING_TIMEOUT
                                                                            .getDefaultLong();
    protected int             runtimeContinuousFailureCeiling           = RUNTIME_CONTINUOUS_FAILURE_CEILING
                                                                            .getDefaultInt();

    protected long            runtimeMaxWait                            = RUNTIME_MAX_WAIT
                                                                            .getDefaultLong();
    protected int             runtimeRetryTimes                         = RUNTIME_RETRY_TIMES
                                                                            .getDefaultInt();

    protected int             runtimeRetryInterval                      = RUNTIME_RETRY_INTERVAL
                                                                            .getDefaultInt();

    protected long            runtimeBatchMaxWait                       = RUNTIME_BATCH_MAX_WAIT
                                                                            .getDefaultLong();

    protected ExecutorService runtimeBatchExecutor                      = (ExecutorService) RUNTIME_BATCH_EXECUTOR
                                                                            .getDefaultObject();

    protected int             rpcConnectTimeout                         = RPC_CONNECT_TIMEOUT
                                                                            .getDefaultInt();
    protected int             rpcExecuteTimeout                         = RPC_EXECUTE_TIMEOUT
                                                                            .getDefaultInt();
    protected int             rpcLoginTimeout                           = RPC_LOGIN_TIMEOUT
                                                                            .getDefaultInt();
    protected long            slowQueryMonitorThreshold                 = SLOW_QUERY_MONITOR_THRESHOLD
                                                                            .getDefaultLong();
    protected Long            maxConnExpiredTime                        = MAX_CONN_EXPIRED_TIME
                                                                            .getDefaultLong();

    @Deprecated
    /*
     * Get metadata refresh internal.
     */
    public long getMetadataRefreshInternal() {
        return metadataRefreshInterval;
    }

    @Deprecated
    /*
     * Set metadata refresh internal.
     */
    public void setMetadataRefreshInternal(long metadataRefreshInterval) {
        this.metadataRefreshInterval = metadataRefreshInterval;
    }

    /*
     * Get metadata refresh interval.
     */
    public long getMetadataRefreshInterval() {
        return metadataRefreshInterval;
    }

    /*
     * Set metadata refresh interval.
     */
    public void setMetadataRefreshInterval(long metadataRefreshInterval) {
        this.metadataRefreshInterval = metadataRefreshInterval;
    }

    /*
     * Get metadata refresh lock timeout.
     */
    public long getMetadataRefreshLockTimeout() {
        return metadataRefreshLockTimeout;
    }

    /*
     * Set metadata refresh lock timeout.
     */
    public void setMetadataRefreshLockTimeout(long metadataRefreshLockTimeout) {
        this.metadataRefreshLockTimeout = metadataRefreshLockTimeout;
    }

    /*
     * Get rs list acquire connect timeout.
     */
    public int getRsListAcquireConnectTimeout() {
        return rsListAcquireConnectTimeout;
    }

    /*
     * Set rs list acquire connect timeout.
     */
    public void setRsListAcquireConnectTimeout(int rsListAcquireConnectTimeout) {
        this.rsListAcquireConnectTimeout = rsListAcquireConnectTimeout;
    }

    /*
     * Get rs list acquire read timeout.
     */
    public int getRsListAcquireReadTimeout() {
        return rsListAcquireReadTimeout;
    }

    /*
     * Set rs list acquire read timeout.
     */
    public void setRsListAcquireReadTimeout(int rsListAcquireReadTimeout) {
        this.rsListAcquireReadTimeout = rsListAcquireReadTimeout;
    }

    /*
     * Get rs list acquire try times.
     */
    public int getRsListAcquireTryTimes() {
        return rsListAcquireTryTimes;
    }

    /*
     * Set rs list acquire try times.
     */
    public void setRsListAcquireTryTimes(int rsListAcquireTryTimes) {
        this.rsListAcquireTryTimes = rsListAcquireTryTimes;
    }

    @Deprecated
    /*
     * Get rs list acquire retry internal.
     */
    public long getRsListAcquireRetryInternal() {
        return rsListAcquireRetryInterval;
    }

    @Deprecated
    /*
     * Set rs list acquire retry internal.
     */
    public void setRsListAcquireRetryInternal(long rsListAcquireRetryInterval) {
        this.rsListAcquireRetryInterval = rsListAcquireRetryInterval;
    }

    /*
     * Get rs list acquire retry interval.
     */
    public long getRsListAcquireRetryInterval() {
        return rsListAcquireRetryInterval;
    }

    /*
     * Set rs list acquire retry interval.
     */
    public void setRsListAcquireRetryInterval(long rsListAcquireRetryInterval) {
        this.rsListAcquireRetryInterval = rsListAcquireRetryInterval;
    }

    /*
     * Get table entry acquire connect timeout.
     */
    public long getTableEntryAcquireConnectTimeout() {
        return tableEntryAcquireConnectTimeout;
    }

    /*
     * Set table entry acquire connect timeout.
     */
    public void setTableEntryAcquireConnectTimeout(long tableEntryAcquireConnectTimeout) {
        this.tableEntryAcquireConnectTimeout = tableEntryAcquireConnectTimeout;
    }

    /*
     * Get table entry acquire socket timeout.
     */
    public long getTableEntryAcquireSocketTimeout() {
        return tableEntryAcquireSocketTimeout;
    }

    /*
     * Set table entry acquire socket timeout.
     */
    public void setTableEntryAcquireSocketTimeout(long tableEntryAcquireSocketTimeout) {
        this.tableEntryAcquireSocketTimeout = tableEntryAcquireSocketTimeout;
    }

    /*
     * Get table entry refresh interval base.
     */
    public long getTableEntryRefreshIntervalBase() {
        return tableEntryRefreshIntervalBase;
    }

    /*
     * Set table entry refresh interval base.
     */
    public void setTableEntryRefreshIntervalBase(long tableEntryRefreshIntervalBase) {
        this.tableEntryRefreshIntervalBase = tableEntryRefreshIntervalBase;
    }

    /*
     * Get table entry refresh interval ceiling.
     */
    public long getTableEntryRefreshIntervalCeiling() {
        return tableEntryRefreshIntervalCeiling;
    }

    /*
     * Set table entry refresh interval ceiling.
     */
    public void setTableEntryRefreshIntervalCeiling(long tableEntryRefreshIntervalCeiling) {
        this.tableEntryRefreshIntervalCeiling = tableEntryRefreshIntervalCeiling;
    }

    /*
     * Get table entry refresh try times.
     */
    public int getTableEntryRefreshTryTimes() {
        return tableEntryRefreshTryTimes;
    }

    /*
     * Set table entry refresh try times.
     */
    public void setTableEntryRefreshTryTimes(int tableEntryRefreshTryTimes) {
        this.tableEntryRefreshTryTimes = tableEntryRefreshTryTimes;
    }

    /*
     * Get table entry refresh lock timeout.
     */
    public long getTableEntryRefreshLockTimeout() {
        return tableEntryRefreshLockTimeout;
    }

    /*
     * Set table entry refresh lock timeout.
     */
    public void setTableEntryRefreshLockTimeout(long tableEntryRefreshLockTimeout) {
        this.tableEntryRefreshLockTimeout = tableEntryRefreshLockTimeout;
    }

    /*
     * Get odp table entry refresh lock timeout.
     */
    public long getODPTableEntryRefreshLockTimeout() {
        return ODPTableEntryRefreshLockTimeout;
    }

    /*
     * Set odp table entry refresh lock timeout.
     */
    public void setODPTableEntryRefreshLockTimeout(long ODPTableEntryRefreshLockTimeout) {
        this.ODPTableEntryRefreshLockTimeout = ODPTableEntryRefreshLockTimeout;
    }

    /*
     * Get table entry refresh continuous failure ceiling.
     */
    public int getTableEntryRefreshContinuousFailureCeiling() {
        return tableEntryRefreshContinuousFailureCeiling;
    }

    /*
     * Set table entry refresh continuous failure ceiling.
     */
    public void setTableEntryRefreshContinuousFailureCeiling(int tableEntryRefreshContinuousFailureCeiling) {
        this.tableEntryRefreshContinuousFailureCeiling = tableEntryRefreshContinuousFailureCeiling;
    }

    /*
     * Get server address priority timeout.
     */
    public long getServerAddressPriorityTimeout() {
        return serverAddressPriorityTimeout;
    }

    /*
     * Set server address priority timeout.
     */
    public void setServerAddressPriorityTimeout(long serverAddressPriorityTimeout) {
        this.serverAddressPriorityTimeout = serverAddressPriorityTimeout;
    }

    /*
     * Get server address caching timeout.
     */
    public long getServerAddressCachingTimeout() {
        return serverAddressCachingTimeout;
    }

    /*
     * Set server address caching timeout.
     */
    public void setServerAddressCachingTimeout(long serverAddressCachingTimeout) {
        this.serverAddressCachingTimeout = serverAddressCachingTimeout;
    }

    /*
     * Get runtime continuous failure ceiling.
     */
    public int getRuntimeContinuousFailureCeiling() {
        return runtimeContinuousFailureCeiling;
    }

    /*
     * Set runtime continuous failure ceiling.
     */
    public void setRuntimeContinuousFailureCeiling(int runtimeContinuousFailureCeiling) {
        this.runtimeContinuousFailureCeiling = runtimeContinuousFailureCeiling;
    }

    /*
     * Get runtime max wait.
     */
    public long getRuntimeMaxWait() {
        return runtimeMaxWait;
    }

    /*
     * Set runtime max wait.
     */
    public void setRuntimeMaxWait(long runtimeMaxWait) {
        this.runtimeMaxWait = runtimeMaxWait;
        this.properties.put(RUNTIME_MAX_WAIT.getKey(), String.valueOf(runtimeMaxWait));

    }

    /*
     * Get runtime retry times.
     */
    public int getRuntimeRetryTimes() {
        return runtimeRetryTimes;
    }

    /*
     * Get runtime retry interval.
     */
    public int getRuntimeRetryInterval() {
        return runtimeRetryInterval;
    }

    /*
     * Set runtime retry interval.
     */
    public void setRuntimeRetryInterval(int runtimeRetryInterval) {
        this.runtimeRetryInterval = runtimeRetryInterval;
    }

    /*
     * Set runtime retry times.
     */
    public void setRuntimeRetryTimes(int runtimeRetryTimes) {
        this.properties.put(RUNTIME_RETRY_TIMES.getKey(), String.valueOf(runtimeRetryTimes));
        this.runtimeRetryTimes = runtimeRetryTimes;
    }

    /*
     * Is table entry refresh interval wait.
     */
    public boolean isTableEntryRefreshIntervalWait() {
        return tableEntryRefreshIntervalWait;
    }

    /*
     * Set table entry refresh interval wait.
     */
    public void setTableEntryRefreshIntervalWait(boolean tableEntryRefreshIntervalWait) {
        this.tableEntryRefreshIntervalWait = tableEntryRefreshIntervalWait;
    }

    /*
     * Get rpc connect timeout.
     */
    public int getRpcConnectTimeout() {
        return rpcConnectTimeout;
    }

    /*
     * Set rpc connect timeout.
     */
    public void setRpcConnectTimeout(int rpcConnectTimeout) {
        this.properties.put(RPC_CONNECT_TIMEOUT.getKey(), String.valueOf(rpcConnectTimeout));
        this.rpcConnectTimeout = rpcConnectTimeout;
    }

    /*
     * Get rpc execute timeout.
     */
    public int getRpcExecuteTimeout() {
        return rpcExecuteTimeout;
    }

    /*
     * Set rpc execute timeout.
     */
    public void setRpcExecuteTimeout(int rpcExecuteTimeout) {
        this.properties.put(RPC_EXECUTE_TIMEOUT.getKey(), String.valueOf(rpcExecuteTimeout));
        this.rpcExecuteTimeout = rpcExecuteTimeout;
    }

    /*
     * Get rpc login timeout.
     */
    public int getRpcLoginTimeout() {
        return rpcLoginTimeout;
    }

    /*
     * Set rpc login timeout.
     */
    public void setRpcLoginTimeout(int rpcLoginTimeout) {
        this.properties.put(RPC_LOGIN_TIMEOUT.getKey(), String.valueOf(rpcLoginTimeout));
        this.rpcLoginTimeout = rpcLoginTimeout;
    }

    /*
     * Get runtime batch max wait.
     */
    public long getRuntimeBatchMaxWait() {
        return runtimeBatchMaxWait;
    }

    /*
     * Set runtime batch max wait.
     */
    public void setRuntimeBatchMaxWait(long runtimeBatchMaxWait) {
        this.properties.put(RUNTIME_BATCH_MAX_WAIT.getKey(), String.valueOf(runtimeBatchMaxWait));
        this.runtimeBatchMaxWait = runtimeBatchMaxWait;
    }

    /*
     * Get runtime batch executor.
     */
    public ExecutorService getRuntimeBatchExecutor() {
        return runtimeBatchExecutor;
    }

    /*
     * Set runtime batch executor.
     */
    public void setRuntimeBatchExecutor(ExecutorService runtimeBatchExecutor) {
        this.runtimeBatchExecutor = runtimeBatchExecutor;
    }

    /*
     * Get slow query threshold.
     */
    public long getslowQueryMonitorThreshold() {
        return slowQueryMonitorThreshold;
    }

    /*
     * Set slow query threshold.
     */
    public void setslowQueryMonitorThreshold(long slowQueryMonitorThreshold) {
        this.properties.put(SLOW_QUERY_MONITOR_THRESHOLD.getKey(),
            String.valueOf(slowQueryMonitorThreshold));
        this.slowQueryMonitorThreshold = slowQueryMonitorThreshold;
    }
}

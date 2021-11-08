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

package com.alipay.oceanbase.rpc.property;

import com.alipay.remoting.config.Configs;

/**
 * all default time is millisecond
 *
 */
public enum Property {

    //remote call related
    RPC_CONNECT_TIMEOUT("rpc.connect.timeout", 500, "建立RPC连接的超时时间"),

    RPC_CONNECT_TRY_TIMES("rpc.connect.try.times", 3, "建立RPC连接的尝试次数"),

    RPC_EXECUTE_TIMEOUT("rpc.execute.timeout", 3000, "执行RPC请求的socket超时时间"),

    RPC_LOGIN_TIMEOUT("rpc.login.timeout", 1000, "请求RPC登录的超时时间"),

    RPC_LOGIN_TRY_TIMES("rpc.login.try.times", 3, "请求RPC登录的尝试次数"),

    RPC_OPERATION_TIMEOUT("rpc.operation.timeout", 10000L, "OB内部执行RPC请求的超时时间"),

    METADATA_REFRESH_INTERVAL("metadata.refresh.interval", 60000L, "刷新METADATA的时间间隔"),

    /**
     * typo of METADATA_REFRESH_INTERVAL
     */
    @Deprecated
    METADATA_REFRESH_INTERNAL("metadata.refresh.internal", 60000L, "刷新METADATA的时间间隔"),

    METADATA_REFRESH_LOCK_TIMEOUT("metadata.refresh.lock.timeout", 8000L, "刷新METADATA的锁超时时间"),

    RS_LIST_ACQUIRE_CONNECT_TIMEOUT("rs.list.acquire.connect.timeout", 200, "获取RS列表的建连的超时时间"),

    RS_LIST_ACQUIRE_READ_TIMEOUT("rs.list.acquire.read.timeout", 1000, "获取RS列表的读取的超时时间"),

    RS_LIST_ACQUIRE_TRY_TIMES("rs.list.acquire.try.times", 3, "获取RS列表的尝试次数"),

    RS_LIST_ACQUIRE_RETRY_INTERVAL("rs.list.acquire.retry.interval", 100L, "每次尝试获取RS列表的时间间隔"),

    /**
     * typo of RS_LIST_ACQUIRE_RETRY_INTERVAL
     */
    @Deprecated
    RS_LIST_ACQUIRE_RETRY_INTERNAL("rs.list.acquire.retry.internal", 100L, "每次尝试获取RS列表的时间间隔"),

    TABLE_ENTRY_ACQUIRE_CONNECT_TIMEOUT("table.entry.acquire.connect.timeout", 500L,
                                        "刷新TABLE地址的建连超时时间"),

    TABLE_ENTRY_ACQUIRE_SOCKET_TIMEOUT("table.entry.acquire.socket.timeout", 3000L,
                                       "刷新TABLE地址的SOCKET超时时间"),

    TABLE_ENTRY_REFRESH_INTERVAL_BASE("table.entry.refresh.interval.base", 100L, "刷新TABLE地址的基础时间间隔"),

    /**
     * typo of TABLE_ENTRY_REFRESH_INTERVAL_BASE
     */
    @Deprecated
    TABLE_ENTRY_REFRESH_INTERNAL_BASE("table.entry.refresh.internal.base", 100L, "刷新TABLE地址的基础时间间隔"),

    TABLE_ENTRY_REFRESH_INTERVAL_CEILING("table.entry.refresh.interval.ceiling", 1600L,
                                         "刷新TABLE地址的最大时间间隔"),

    /**
     * typo of TABLE_ENTRY_REFRESH_INTERVAL_CEILING
     */
    @Deprecated
    TABLE_ENTRY_REFRESH_INTERNAL_CEILING("table.entry.refresh.internal.ceiling", 1600L,
                                         "刷新TABLE地址的最大时间间隔"),

    TABLE_ENTRY_REFRESH_INTERVAL_WAIT("table.entry.refresh.interval.wait", false,
                                      "刷新TABLE地址时是否等待间隔时间"),

    TABLE_ENTRY_REFRESH_LOCK_TIMEOUT("table.entry.refresh.lock.timeout", 4000L, "刷新TABLE地址的锁超时时间"),

    TABLE_ENTRY_REFRESH_TYE_TIMES("table.entry.refresh.try.times", 3, "刷新TABLE地址的尝试次数"),

    TABLE_ENTRY_REFRESH_CONTINUOUS_FAILURE_CEILING(
                                                   "table.entry.refresh.continuous.failure.ceiling",
                                                   10, "连续刷新TABLE地址的失败上限，会刷新METADATA"),

    SERVER_ADDRESS_PRIORITY_TIMEOUT("server.address.priority.timeout", 1800000L, "SERVER地址优先级的失效时间"),

    SERVER_CONNECTION_POOL_SIZE("server.connection.pool.size", 1, "单个SERVER的连接数"),

    SERVER_ADDRESS_CACHING_TIMEOUT("server.address.caching.timeout", 3600000L, "SERVER地址缓存的失效时间"),

    //runtime execute related
    RUNTIME_CONTINUOUS_FAILURE_CEILING("runtime.continuous.failure.ceiling", 100,
                                       "连续运行失败上限，会刷新TABLE的地址"),

    RUNTIME_RETRY_TIMES("runtime.retry.times", 1, "运行过程中遇到可重试错误时的重试次数"),

    RUNTIME_RETRY_INTERVAL("runtime.retry.interval", 1, "运行出错时重试的时间间隔"),

    RUNTIME_MAX_WAIT("runtime.max.wait", 3000L, "单次执行超时时间会在超时时间内重试"),

    RUNTIME_BATCH_MAX_WAIT("runtime.batch.max.wait", 3000L, "批量执行请求的超时时间"),

    RUNTIME_BATCH_EXECUTOR("runtime.batch.executor", null, "批量请求时并发执行的线程池"),

    // overwrite the global default netty watermark for ob table: [512K, 1M]
    NETTY_BUFFER_LOW_WATERMARK(Configs.NETTY_BUFFER_LOW_WATERMARK, 512 * 1024, "netty写缓存的低水位"),

    NETTY_BUFFER_HIGH_WATERMARK(Configs.NETTY_BUFFER_HIGH_WATERMARK, 1024 * 1024, "netty写缓存的高水位"),
    // Theoretically with normal 10Gbps network card, it costs 0.5 ms to flush 512K,
    // that is, flush the buffer from high_watermark(1M) to low_watermark(512k) by default.
    // So before throw Exception when buffer is full, sleep 1ms by default for the scenario
    // when a big package is blocking the buffer but the server is OK.
    NETTY_BLOCKING_WAIT_INTERVAL("bolt.netty.blocking.wait.interval", 1, "netty写缓存满后等待时间");

    private final String key;
    private final Object defaultV;
    private final String desc;

    Property(String key, Object defaultV, String desc) {
        this.key = key;
        this.defaultV = defaultV;
        this.desc = desc;
    }

    /**
     * Get key.
     */
    public String getKey() {
        return key;
    }

    /**
     * Get default int.
     */
    public int getDefaultInt() {
        return (Integer) defaultV;
    }

    /**
     * Get default long.
     */
    public long getDefaultLong() {
        return (Long) defaultV;
    }

    /**
     * Get default object.
     */
    public Object getDefaultObject() {
        return defaultV;
    }

    /**
     * Get default boolean.
     */
    public boolean getDefaultBoolean() {
        return (Boolean) defaultV;
    }

    /**
     * Get desc.
     */
    public String getDesc() {
        return desc;
    }
}

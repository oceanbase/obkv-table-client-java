/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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

package com.alipay.oceanbase.rpc.dds.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupClusterDbkeyConfig;
import com.alipay.sofa.dds.config.AttributesConfig;
import com.alipay.sofa.dds.config.advanced.DoubleWriteRule;
import com.alipay.sofa.dds.config.advanced.TestLoadRule;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DDS配置变更监控工具类
 * 
 * 根据OBKV客户端日志整改文档要求，提供统一的配置变更监控接口，
 * 确保所有配置变更都能被大禹监控平台正确采集和分析。
 * 
 * @since 4.3.0
 */
public class DdsConfigMonitor {

    private static final Logger     logger                        = LoggerFactory
                                                                      .getLogger(DdsConfigMonitor.class);
    private static final Logger     ddsConfigLogger               = TableClientLoggerFactory
                                                                      .getDDSConfigLogger();

    // 监控统计计数器
    private static final AtomicLong totalConfigUpdates            = new AtomicLong(0);
    private static final AtomicLong successfulConfigUpdates       = new AtomicLong(0);
    private static final AtomicLong failedConfigUpdates           = new AtomicLong(0);
    private static final AtomicLong totalConnectionCreations      = new AtomicLong(0);
    private static final AtomicLong successfulConnectionCreations = new AtomicLong(0);
    private static final AtomicLong totalGroupCreations           = new AtomicLong(0);
    private static final AtomicLong successfulGroupCreations      = new AtomicLong(0);

    /**
     * 记录配置更新开始事件
     */
    public static void recordConfigUpdateStart(String updateType, String trigger) {
        long timestamp = System.currentTimeMillis();
        totalConfigUpdates.incrementAndGet();

        ddsConfigLogger.info(
            "DDS_CONFIG_UPDATE_START,"
                    + "updateType={},trigger={},timestamp={},totalUpdates={},status=STARTED",
            updateType, trigger, timestamp, totalConfigUpdates.get());

        logger.info("DDS配置更新开始: updateType={}, trigger={}", updateType, trigger);
    }

    /**
     * 记录配置更新完成事件
     */
    public static void recordConfigUpdateSuccess(String updateType, long duration,
                                                 int oldGroupCount, int newGroupCount,
                                                 int oldConnectionCount, int newConnectionCount) {
        long timestamp = System.currentTimeMillis();
        successfulConfigUpdates.incrementAndGet();

        ddsConfigLogger.info("DDS_CONFIG_UPDATE_SUCCESS,"
                             + "updateType={},duration={},oldGroupCount={},newGroupCount={},"
                             + "oldConnectionCount={},newConnectionCount={},timestamp={},"
                             + "totalUpdates={},successUpdates={},status=SUCCESS", updateType,
            duration, oldGroupCount, newGroupCount, oldConnectionCount, newConnectionCount,
            timestamp, totalConfigUpdates.get(), successfulConfigUpdates.get());

        logger.info("DDS配置更新成功: updateType={}, duration={}ms", updateType, duration);
    }

    /**
     * 记录配置更新失败事件
     */
    public static void recordConfigUpdateFailure(String updateType, String errorMessage,
                                                 long duration) {
        long timestamp = System.currentTimeMillis();
        failedConfigUpdates.incrementAndGet();

        ddsConfigLogger.error("DDS_CONFIG_UPDATE_FAILED,"
                              + "updateType={},duration={},errorMessage={},timestamp={},"
                              + "totalUpdates={},successUpdates={},failedUpdates={},status=FAILED",
            updateType, duration, errorMessage, timestamp, totalConfigUpdates.get(),
            successfulConfigUpdates.get(), failedConfigUpdates.get());

        logger.error("DDS配置更新失败: updateType={}, error={}", updateType, errorMessage);
    }

    /**
     * 记录连接创建开始事件
     */
    public static void recordConnectionCreationStart(int configCount) {
        totalConnectionCreations.incrementAndGet();

        ddsConfigLogger.info("DDS_CONNECTION_CREATE_START,"
                             + "configCount={},totalCreations={},status=STARTED", configCount,
            totalConnectionCreations.get());
    }

    /**
     * 记录连接创建完成事件
     */
    public static void recordConnectionCreationComplete(int successCount, int failureCount,
                                                        long duration) {
        successfulConnectionCreations.addAndGet(successCount);

        ddsConfigLogger.info("DDS_CONNECTION_CREATE_COMPLETE,"
                             + "successCount={},failureCount={},duration={},totalCreations={},"
                             + "successfulCreations={},status=COMPLETED", successCount,
            failureCount, duration, totalConnectionCreations.get(),
            successfulConnectionCreations.get());
    }

    /**
     * 记录单个连接创建成功
     */
    public static void recordSingleConnectionSuccess(String dbkey, String appName) {
        ddsConfigLogger.info("DDS_CONNECTION_CREATE_SUCCESS,"
                             + "dbkey={},appName={},status=SUCCESS", dbkey, appName);
    }

    /**
     * 记录单个连接创建失败
     */
    public static void recordSingleConnectionFailure(String dbkey, String appName,
                                                     String errorMessage) {
        ddsConfigLogger.error("DDS_CONNECTION_CREATE_FAILED,"
                              + "dbkey={},appName={},errorMessage={},status=FAILED", dbkey,
            appName, errorMessage);
    }

    /**
     * 记录组创建开始事件
     */
    public static void recordGroupCreationStart(int groupCount, int atomDataSourceCount) {
        totalGroupCreations.incrementAndGet();

        ddsConfigLogger.info(
            "DDS_GROUP_CREATE_START,"
                    + "groupCount={},atomDataSourceCount={},totalGroups={},status=STARTED",
            groupCount, atomDataSourceCount, totalGroupCreations.get());
    }

    /**
     * 记录组创建完成事件
     */
    public static void recordGroupCreationComplete(int successGroups, int failureGroups,
                                                   long duration) {
        successfulGroupCreations.addAndGet(successGroups);

        ddsConfigLogger.info("DDS_GROUP_CREATE_COMPLETE,"
                             + "successGroups={},failureGroups={},duration={},totalGroups={},"
                             + "successfulGroups={},status=COMPLETED", successGroups,
            failureGroups, duration, totalGroupCreations.get(), successfulGroupCreations.get());
    }

    /**
     * 记录单个组创建成功
     */
    public static void recordSingleGroupSuccess(String groupKey, int groupIndex,
                                                int atomDataSourceCount) {
        ddsConfigLogger.info("DDS_GROUP_CREATE_SUCCESS,"
                             + "groupKey={},groupIndex={},atomDataSourceCount={},status=SUCCESS",
            groupKey, groupIndex, atomDataSourceCount);
    }

    /**
     * 记录单个组创建失败
     */
    public static void recordSingleGroupFailure(String groupKey, String errorMessage) {
        ddsConfigLogger
            .error("DDS_GROUP_CREATE_FAILED," + "groupKey={},errorMessage={},status=FAILED",
                groupKey, errorMessage);
    }

    /**
     * 记录配置比较结果
     */
    public static void recordConfigComparison(String updateType, int oldConfigCount,
                                              int newConfigCount, int configsToUpdate,
                                              int configsToRemove, int configsToAdd) {
        ddsConfigLogger.info(
            "DDS_CONFIG_COMPARISON," + "updateType={},oldConfigCount={},newConfigCount={},"
                    + "configsToUpdate={},configsToRemove={},configsToAdd={},status=COMPLETED",
            updateType, oldConfigCount, newConfigCount, configsToUpdate, configsToRemove,
            configsToAdd);
    }

    /**
     * 记录配置替换事件
     */
    public static void recordConfigReplacement(int oldGroupCount, int newGroupCount,
                                               int deprecatedIndexCount) {
        ddsConfigLogger.info(
            "DDS_CONFIG_REPLACEMENT,"
                    + "oldGroupCount={},newGroupCount={},deprecatedIndexCount={},status=SUCCESS",
            oldGroupCount, newGroupCount, deprecatedIndexCount);
    }

    /**
     * 记录异步清理事件
     */
    public static void recordAsyncCleanup(String dbkey, long delay, long actualWaitTime) {
        ddsConfigLogger.info("DDS_ASYNC_CLEANUP,"
                             + "dbkey={},requestedDelay={},actualWaitTime={},status=EXECUTED",
            dbkey, delay, actualWaitTime);
    }

    /**
     * 获取监控统计信息
     */
    public static String getMonitorStatistics() {
        return String.format("DDS配置监控统计 - 配置更新: 总数=%d, 成功=%d, 失败=%d | " + "连接创建: 总数=%d, 成功=%d | "
                             + "组创建: 总数=%d, 成功=%d", totalConfigUpdates.get(),
            successfulConfigUpdates.get(), failedConfigUpdates.get(),
            totalConnectionCreations.get(), successfulConnectionCreations.get(),
            totalGroupCreations.get(), successfulGroupCreations.get());
    }

    /**
     * 重置监控统计（用于测试）
     */
    public static void resetStatistics() {
        totalConfigUpdates.set(0);
        successfulConfigUpdates.set(0);
        failedConfigUpdates.set(0);
        totalConnectionCreations.set(0);
        successfulConnectionCreations.set(0);
        totalGroupCreations.set(0);
        successfulGroupCreations.set(0);

        ddsConfigLogger.info("DDS_MONITOR_STATISTICS_RESET,status=RESET");
    }
}

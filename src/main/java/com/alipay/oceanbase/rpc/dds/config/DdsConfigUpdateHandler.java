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

package com.alipay.oceanbase.rpc.dds.config;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.dds.group.ObTableClientGroup;
import com.alipay.oceanbase.rpc.dds.util.ConfigWrapper;
import com.alipay.oceanbase.rpc.dds.util.ConfigComparisonUtil;
import com.alipay.oceanbase.rpc.dds.util.DataSourceFactory;
import com.alipay.oceanbase.rpc.dds.util.VersionedConfigSnapshot;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.sofa.dds.config.AttributesConfig;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.advanced.DoubleWriteRule;
import com.alipay.sofa.dds.config.advanced.TestLoadRule;
import com.alipay.sofa.dds.config.dynamic.DynamicConfigHandler;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupClusterDbkeyConfig;
import com.alipay.sofa.dds.config.rule.AppRule;
import com.alipay.sofa.dds.config.group.GroupDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceWeight;
import com.alipay.sofa.dds.config.group.AtomDataSourceWeight;

public class DdsConfigUpdateHandler implements DynamicConfigHandler {
    
  private final AtomicReference<VersionedConfigSnapshot> currentConfig;
  ObTableClient.RunningMode runningMode = ObTableClient.RunningMode.NORMAL;
  Properties properties;
  
  private final ReentrantReadWriteLock configLock = new ReentrantReadWriteLock();

  private static final Logger logger = LoggerFactory.getLogger(DdsConfigUpdateHandler.class);
  private static final Logger ddsConfigLogger = TableClientLoggerFactory.getDDSConfigLogger();
  
  // async cleanup executor
  private final ScheduledExecutorService scheduledExecutor;
  private final ConcurrentHashMap<String, AsyncCloseTask> pendingCloseTasks;
  private final AtomicInteger totalAsyncCloses = new AtomicInteger(0);
  private final AtomicInteger completedAsyncCloses = new AtomicInteger(0);
  
  private Supplier<Map<String, ExtendedDataSourceConfig>> extendedDataSourceSupplier = () -> Collections.emptyMap();
  private Supplier<AppRule> appRuleSupplier = () -> null;

  // Callback to notify DdsObTableClient when config is updated (e.g., for syncing row key elements)
  private ConfigUpdateCallback configUpdateCallback;

  public DdsConfigUpdateHandler(ObTableClient.RunningMode runningMode, Properties tableClientProperty,
      AtomicReference<VersionedConfigSnapshot> sharedConfig) {
    this.runningMode = runningMode != null ? runningMode : ObTableClient.RunningMode.NORMAL;
    this.properties = tableClientProperty;
    this.currentConfig = sharedConfig != null ? sharedConfig : new AtomicReference<>();
    
    // async cleanup executor
    this.scheduledExecutor = Executors.newScheduledThreadPool(2, r -> {
      Thread t = new Thread(r, "ObTableClient-AsyncCleaner");
      t.setDaemon(true);
      return t;
    });
    this.pendingCloseTasks = new ConcurrentHashMap<>();
  }

  public void setExtendedDataSourceSupplier(Supplier<Map<String, ExtendedDataSourceConfig>> supplier) {
    this.extendedDataSourceSupplier = supplier != null ? supplier : () -> Collections.emptyMap();
  }

  public void setAppRuleSupplier(Supplier<AppRule> supplier) {
    this.appRuleSupplier = supplier != null ? supplier : () -> null;
  }

  /**
   * Set a callback to be invoked before configuration update is applied.
   * This is used to sync row key elements to new data sources BEFORE they become visible.
   * IMPORTANT: The callback is executed INSIDE the write lock, BEFORE the new config is set.
   * This ensures that all data sources are properly configured before being exposed to requests.
   *
   * @param callback the callback to run before config update
   */
  public void setConfigUpdateCallback(ConfigUpdateCallback callback) {
    this.configUpdateCallback = callback;
  }

  /**
   * Reset group cluster configuration.
   * NOTE: This method is called by DDS SDK in its background thread.
   */
  @Override
  public void resetGroupCluster(GroupClusterConfig groupCluster) {
      ConfigUpdateTask task = new ConfigUpdateTask(
          ConfigUpdateType.GROUP_CLUSTER, 
          groupCluster, 
          null, 
          null
      );
      processConfigUpdate(task);
  }
  
  /**
   * Reset group cluster dbkey configuration.
   * NOTE: This method is called by DDS SDK in its background thread.
   */
  @Override
  public boolean resetGroupClusterDbkey(GroupClusterDbkeyConfig groupClusterDbkey) {
      ConfigUpdateTask task = new ConfigUpdateTask(
          ConfigUpdateType.GROUP_CLUSTER_DBKEY, 
          null, 
          groupClusterDbkey, 
          null
      );
      processConfigUpdate(task);
      return true;
  }
  
  /**
   * Reset attributes configuration.
   * NOTE: This method is called by DDS SDK in its background thread.
   */
  @Override
  public void resetAttributes(AttributesConfig attributes) {
      ConfigUpdateTask task = new ConfigUpdateTask(
          ConfigUpdateType.ATTRIBUTES, 
          null, 
          null, 
          attributes
      );
      processConfigUpdate(task);
  }
  
  /**
   * Reset double write rules.
   * NOTE: This method is called by DDS SDK in its background thread.
   */
  @Override
  public void resetDoubleWriteRules(Map<String, DoubleWriteRule> doubleWriteRules) {
      ConfigUpdateTask task = new ConfigUpdateTask(
          ConfigUpdateType.DOUBLE_WRITE_RULES, 
          null, 
          null, 
          doubleWriteRules
      );
      processConfigUpdate(task);
  }
  
  /**
   * Reset white list rules.
   * NOTE: This method is called by DDS SDK in its background thread.
   */
  @Override
  public void resetWhiteListRules(Map<String, String> whiteListRules) {
      ConfigUpdateTask task = new ConfigUpdateTask(
          ConfigUpdateType.WHITE_LIST_RULES, 
          null, 
          null, 
          whiteListRules
      );
      processConfigUpdate(task);
  }
  
  /**
   * Reset self adjust rules.
   * NOTE: This method is called by DDS SDK in its background thread.
   */
  @Override
  public void resetSelfAdjustRules(Map<String, String> selfAdjustRules) {
      ConfigUpdateTask task = new ConfigUpdateTask(
          ConfigUpdateType.SELF_ADJUST_RULES, 
          null, 
          null, 
          selfAdjustRules
      );
      processConfigUpdate(task);
  }
  
  /**
   * Reset test load rule.
   * NOTE: This method is called by DDS SDK in its background thread.
   */
  @Override
  public void resetTestLoadRule(TestLoadRule testLoadRule) {
      ConfigUpdateTask task = new ConfigUpdateTask(
          ConfigUpdateType.TEST_LOAD_RULE, 
          null, 
          null, 
          testLoadRule
      );
      processConfigUpdate(task);
  }
  
  /**
   * Process configuration update task.
   */
  private void processConfigUpdate(ConfigUpdateTask task) {
      long startTime = System.currentTimeMillis();
      String taskType = task.getType().name();
      
      ddsConfigLogger.info("DDS_CONFIG_UPDATE_START," +
              "taskType={},timestamp={},taskCount={}", 
              taskType, startTime, 1);
      
      try {
          logger.info("Processing config update task: {}", task.getType());
          
          // 1. Build new configuration
          VersionedConfigSnapshot oldConfig = currentConfig.get();
          VersionedConfigSnapshot newConfig = buildNewConfig(task);
          
          // 2. Replace configuration atomically
          replaceConfigAtomically(newConfig);
          
          long endTime = System.currentTimeMillis();
          long duration = endTime - startTime;
          
          // 记录成功的配置变更
          String oldGroupCount = oldConfig != null ? 
              String.valueOf(oldConfig.getGroupDataSources().size()) : "0";
          String newGroupCount = String.valueOf(newConfig.getGroupDataSources().size());
          String oldAtomCount = oldConfig != null ? 
              String.valueOf(oldConfig.getAtomDataSources().size()) : "0";
          String newAtomCount = String.valueOf(newConfig.getAtomDataSources().size());
          
          ddsConfigLogger.info("DDS_CONFIG_UPDATE_SUCCESS," +
                  "taskType={},duration={},oldGroupCount={},newGroupCount={}," +
                  "oldAtomCount={},newAtomCount={},status=success", 
                  taskType, duration, oldGroupCount, newGroupCount, 
                  oldAtomCount, newAtomCount);
          
          logger.info("Config update completed successfully for task: {}", task.getType());
          
      } catch (Exception e) {
          long endTime = System.currentTimeMillis();
          long duration = endTime - startTime;
          
          // 记录失败的配置变更
          ddsConfigLogger.error("DDS_CONFIG_UPDATE_FAILED," +
                  "taskType={},duration={},errorMessage={},status=failed", 
                  taskType, duration, e.getMessage());
          
          logger.error("Failed to process config update task: {}", task.getType(), e);
      }
  }
  
  /**
   * Build new configuration based on task type.
   */
  private VersionedConfigSnapshot buildNewConfig(ConfigUpdateTask task) throws Exception {
      VersionedConfigSnapshot current = currentConfig.get();
      if (current == null) {
          throw new IllegalStateException("No current configuration available");
      }
      
      // Build new configuration based on task type
      switch (task.getType()) {
          case GROUP_CLUSTER:
              return buildGroupClusterConfig(current, (GroupClusterConfig) task.getConfig());
          case GROUP_CLUSTER_DBKEY:
              return buildGroupClusterDbkeyConfig(current, (GroupClusterDbkeyConfig) task.getConfig());
          case ATTRIBUTES:
              return buildAttributesConfig(current, (AttributesConfig) task.getConfig());
          case DOUBLE_WRITE_RULES:
              return buildDoubleWriteRulesConfig(current, (Map<String, DoubleWriteRule>) task.getConfig());
          case WHITE_LIST_RULES:
              return buildWhiteListRulesConfig(current, (Map<String, String>) task.getConfig());
          case SELF_ADJUST_RULES:
              return buildSelfAdjustRulesConfig(current, (Map<String, String>) task.getConfig());
          case TEST_LOAD_RULE:
              return buildTestLoadRuleConfig(current, (TestLoadRule) task.getConfig());
          default:
              throw new IllegalArgumentException("Unknown config update type: " + task.getType());
      }
  }
  
  /**
   * Build group cluster configuration.
   * 改进：区分连接更新和规则更新，只有在真正需要时才重建
   */
  private VersionedConfigSnapshot buildGroupClusterConfig(
          VersionedConfigSnapshot current, 
          GroupClusterConfig newGroupCluster) throws Exception {
      
      // 1. 获取最新和当前的数据源配置
      Map<String, ExtendedDataSourceConfig> availableConfigs = resolveLatestExtendedConfigs();
      Map<String, ExtendedDataSourceConfig> currentDataSourceConfigs = current.getDataSourceConfigs();
      
      // 3. 验证配置完整性：确保Group配置引用的所有dbkey都有对应的数据源配置
      validateConfigConsistency(newGroupCluster, availableConfigs);
      
      // 2. 分析配置更新类型
      ConfigComparisonUtil.ConfigUpdateType updateType = ConfigComparisonUtil.analyzeUpdateType(
          current.getGroupCluster(),
          newGroupCluster,
          currentDataSourceConfigs,
          availableConfigs
      );
      
      ddsConfigLogger.info("DDS_CONFIG_ANALYSIS," +
              "updateType={},groupClusterChanged={},dataSourceConfigsChanged={}," +
              "currentDataSourceCount={},availableDataSourceCount={}", 
              updateType.name(), 
              !ConfigComparisonUtil.isGroupClusterConfigEqual(current.getGroupCluster(), newGroupCluster),
              !ConfigComparisonUtil.findConfigsToUpdate(currentDataSourceConfigs, availableConfigs).isEmpty(),
              currentDataSourceConfigs.size(), availableConfigs.size());
      
      Map<String, ObTableClient> newAtomDataSources = new ConcurrentHashMap<>();
      Map<Integer, ObTableClientGroup> newGroupDataSources = new ConcurrentHashMap<>();
      
      switch (updateType) {
          case NONE:
              ddsConfigLogger.info("No configuration changes detected, reusing all existing resources");
              // 完全没有变化，复用所有现有资源
              newAtomDataSources.putAll(current.getAtomDataSources());
              newGroupDataSources.putAll(current.getGroupDataSources());
              break;
              
          case RULES_ONLY:
              ddsConfigLogger.info("Only rules (weights) changed, reusing connections but rebuilding groups");
              // 只有规则变化，复用连接但重新创建组
              newAtomDataSources.putAll(current.getAtomDataSources());
              // 验证Group配置与现有连接的兼容性
              validateGroupConfigCompatibility(newGroupCluster, newAtomDataSources);
              newGroupDataSources = null; // 标记需要重建组
              break;
              
          case CONNECTIONS_ONLY:
              ddsConfigLogger.info("Only connections changed, reusing rules but rebuilding connections and groups");
              // 只有连接变化，重新创建连接和组
              newGroupDataSources = null; // 标记需要重建组
              break;
              
          case CONNECTIONS_AND_RULES:
          default:
              ddsConfigLogger.info("Both connections and rules changed, rebuilding everything");
              // 连接和规则都变化，全部重建
              newGroupDataSources = null; // 标记需要重建组
              break;
      }
      
      // 3. 根据更新类型决定是否需要创建新的连接
      if (updateType == ConfigComparisonUtil.ConfigUpdateType.CONNECTIONS_ONLY ||
          updateType == ConfigComparisonUtil.ConfigUpdateType.CONNECTIONS_AND_RULES) {
          
          // 需要重建连接
          Map<String, ExtendedDataSourceConfig> configsToUpdate = ConfigComparisonUtil.findConfigsToUpdate(
              currentDataSourceConfigs, availableConfigs);
          
          newAtomDataSources = createOrReuseAtomDataSources(
              configsToUpdate, current.getAtomDataSources(), runningMode, properties);
      }
      
      // 4. 根据更新类型决定是否需要重建组
      if (updateType == ConfigComparisonUtil.ConfigUpdateType.RULES_ONLY ||
          updateType == ConfigComparisonUtil.ConfigUpdateType.CONNECTIONS_ONLY ||
          updateType == ConfigComparisonUtil.ConfigUpdateType.CONNECTIONS_AND_RULES) {
          
          // 需要重建组（规则变化或完全重建）
          newGroupDataSources = DataSourceFactory.createGroupDataSources(
              newGroupCluster, newAtomDataSources);
      }
      
      // 5. Get latest AppRule (rebuild logical tables if updated)
      AppRule latestAppRule;
      try {
        latestAppRule = appRuleSupplier.get();
        if (latestAppRule == null) {
          latestAppRule = current.getAppRule();
        }
      } catch (Exception e) {
        ddsConfigLogger.warn("Failed to fetch latest AppRule from supplier, using current snapshot's AppRule", e);
        latestAppRule = current.getAppRule();
      }
      
      // 6. Wrap configuration into versioned snapshot
      boolean appRuleChanged = latestAppRule != current.getAppRule();
      if (appRuleChanged) {
        ddsConfigLogger.info("AppRule updated, will rebuild LogicalTables with new AppRule");
      }
      
      return ConfigWrapper.wrapConfig(
          newGroupCluster,
          availableConfigs,  // 使用最新配置
          latestAppRule,
          newAtomDataSources,
          newGroupDataSources
      );
  }

  /**
   * Build group cluster dbkey configuration.
   */
  private VersionedConfigSnapshot buildGroupClusterDbkeyConfig(
          VersionedConfigSnapshot current,
          GroupClusterDbkeyConfig groupClusterDbkey) throws Exception {
      
      ddsConfigLogger.info("Building group cluster dbkey configuration");
      
      // GroupClusterDbkeyConfig updates are typically incremental updates to existing group cluster
      // Reuse current configuration structure and only update necessary parts
      GroupClusterConfig currentGroupCluster = current.getGroupCluster();
      if (currentGroupCluster == null) {
          throw new IllegalStateException("No current group cluster configuration available");
      }
      
      // For dbkey updates, we rebuild the entire group cluster with updated dbkey information
      return buildGroupClusterConfig(current, currentGroupCluster);
  }

  /**
   * Build attributes configuration.
   */
  private VersionedConfigSnapshot buildAttributesConfig(
          VersionedConfigSnapshot current,
          AttributesConfig attributes) throws Exception {
      
      ddsConfigLogger.info("Attributes configuration update - reusing current snapshot");
      
      // Attributes configuration typically doesn't require data source rebuild
      // Return current snapshot with new timestamp
      return new VersionedConfigSnapshot(
          System.currentTimeMillis(),
          current.getGroupCluster(),
          current.getDataSourceConfigs(),
          current.getAppRule(),
          current.getDeprecatedElasticIndexes(),
          current.getElasticConfigs(),
          current.getAtomDataSources(),
          current.getGroupDataSources(),
          current.getLogicalTables()
      );
  }

  /**
   * Build double write rules configuration.
   */
  private VersionedConfigSnapshot buildDoubleWriteRulesConfig(
          VersionedConfigSnapshot current,
          Map<String, DoubleWriteRule> doubleWriteRules) throws Exception {
      
      ddsConfigLogger.info("Double write rules configuration update - reusing current snapshot");
      
      // Double write rules typically don't require data source rebuild
      // Return current snapshot with new timestamp
      return new VersionedConfigSnapshot(
          System.currentTimeMillis(),
          current.getGroupCluster(),
          current.getDataSourceConfigs(),
          current.getAppRule(),
          current.getDeprecatedElasticIndexes(),
          current.getElasticConfigs(),
          current.getAtomDataSources(),
          current.getGroupDataSources(),
          current.getLogicalTables()
      );
  }

  /**
   * Build white list rules configuration.
   */
  private VersionedConfigSnapshot buildWhiteListRulesConfig(
          VersionedConfigSnapshot current,
          Map<String, String> whiteListRules) throws Exception {
      
      ddsConfigLogger.info("White list rules configuration update - reusing current snapshot");
      
      // White list rules typically don't require data source rebuild
      // Return current snapshot with new timestamp
      return new VersionedConfigSnapshot(
          System.currentTimeMillis(),
          current.getGroupCluster(),
          current.getDataSourceConfigs(),
          current.getAppRule(),
          current.getDeprecatedElasticIndexes(),
          current.getElasticConfigs(),
          current.getAtomDataSources(),
          current.getGroupDataSources(),
          current.getLogicalTables()
      );
  }

  /**
   * Build self adjust rules configuration.
   */
  private VersionedConfigSnapshot buildSelfAdjustRulesConfig(
          VersionedConfigSnapshot current,
          Map<String, String> selfAdjustRules) throws Exception {
      
      ddsConfigLogger.info("Self adjust rules configuration update - reusing current snapshot");
      
      // Self adjust rules typically don't require data source rebuild
      // Return current snapshot with new timestamp
      return new VersionedConfigSnapshot(
          System.currentTimeMillis(),
          current.getGroupCluster(),
          current.getDataSourceConfigs(),
          current.getAppRule(),
          current.getDeprecatedElasticIndexes(),
          current.getElasticConfigs(),
          current.getAtomDataSources(),
          current.getGroupDataSources(),
          current.getLogicalTables()
      );
  }

  /**
   * Build test load rule configuration.
   */
  private VersionedConfigSnapshot buildTestLoadRuleConfig(
          VersionedConfigSnapshot current,
          TestLoadRule testLoadRule) throws Exception {
      
      ddsConfigLogger.info("Test load rule configuration update - reusing current snapshot");
      
      // Test load rule typically doesn't require data source rebuild
      // Return current snapshot with new timestamp
      return new VersionedConfigSnapshot(
          System.currentTimeMillis(),
          current.getGroupCluster(),
          current.getDataSourceConfigs(),
          current.getAppRule(),
          current.getDeprecatedElasticIndexes(),
          current.getElasticConfigs(),
          current.getAtomDataSources(),
          current.getGroupDataSources(),
          current.getLogicalTables()
      );
  }

  /**
   * Replace configuration atomically.
   * IMPORTANT: Sync row key elements to new data sources BEFORE exposing the new config
   * to avoid race conditions where requests might use unconfigured data sources.
   */
  private void replaceConfigAtomically(VersionedConfigSnapshot newConfig) {
      configLock.writeLock().lock();
      try {
          VersionedConfigSnapshot oldConfig = currentConfig.get();

          // Sync row key elements to newly created data sources BEFORE exposing the config
          // This ensures that when the new config is set, all data sources are properly configured
          if (configUpdateCallback != null) {
              try {
                  // Pass the new config to the callback so it can sync to the new data sources
                  // IMPORTANT: Callback is executed BEFORE setting the new config
                  configUpdateCallback.onConfigUpdate(newConfig, oldConfig);
                  ddsConfigLogger.info("DDS_CONFIG_CALLBACK_EXECUTED,status=success");
              } catch (Exception e) {
                  ddsConfigLogger.error("DDS_CONFIG_CALLBACK_FAILED,errorMessage={}", e.getMessage(), e);
                  logger.error("Failed to execute config update callback", e);
                  // Don't proceed with config update if callback fails
                  return;
              }
          }

          // Now atomically set the new config (all data sources are properly configured)
          currentConfig.set(newConfig);

          // Clean up obsolete data sources (async, after config is set)
          if (oldConfig != null) {
              cleanupObsoleteDataSources(oldConfig, newConfig);
          }

          logger.info("Configuration replaced atomically, deprecated indexes: {}",
              newConfig.getDeprecatedElasticIndexes());

          ddsConfigLogger.info("DDS_CONFIG_REPLACED," +
                  "oldGroupCount={},newGroupCount={},deprecatedIndexes={},status=success",
                  oldConfig != null ? oldConfig.getGroupDataSources().size() : 0,
                  newConfig.getGroupDataSources().size(),
                  newConfig.getDeprecatedElasticIndexes().size());

      } finally {
          configLock.writeLock().unlock();
      }
  }

  /**
   * Functional interface for config update callback.
   * Called BEFORE the new config is set, ensuring data sources are properly configured
   * before being exposed to requests.
   */
  public interface ConfigUpdateCallback {
      /**
       * Called during config update, before the new config is atomically set.
       * This allows the callback to sync configurations to new data sources
       * before they become visible to requests.
       *
       * @param newConfig the new configuration snapshot (contains new data sources)
       * @param oldConfig the old configuration snapshot (may contain old data sources to be cleaned up)
       */
      void onConfigUpdate(VersionedConfigSnapshot newConfig, VersionedConfigSnapshot oldConfig);
  }
  
  /**
   * 异步清理过时的数据源连接
   * 改进：使用异步清理，给正在进行的RPC请求时间完成
   */
  private void cleanupObsoleteDataSources(VersionedConfigSnapshot oldConfig, 
                                          VersionedConfigSnapshot newConfig) {
      Map<String, ObTableClient> oldDataSources = oldConfig.getAtomDataSources();
      Map<String, ObTableClient> newDataSources = newConfig.getAtomDataSources();
      
      if (oldDataSources == null || oldDataSources.isEmpty()) {
          return;
      }
      
      // 找出真正需要清理的dbkey（被完全移除的）
      Set<String> dbkeysToCleanup = new HashSet<>();
      if (newDataSources != null) {
          for (String oldDbkey : oldDataSources.keySet()) {
              if (!newDataSources.containsKey(oldDbkey)) {
                  dbkeysToCleanup.add(oldDbkey);
                  ddsConfigLogger.info("Data source config removed for dbkey: {}", oldDbkey);
              }
          }
      } else {
          // 如果新配置为空，清理所有旧数据源
          dbkeysToCleanup.addAll(oldDataSources.keySet());
      }
      
      // 异步清理被移除的数据源
      if (!dbkeysToCleanup.isEmpty()) {
          ddsConfigLogger.info("Scheduling async cleanup for {} obsolete data sources: {}", 
              dbkeysToCleanup.size(), dbkeysToCleanup);
          
          // 获取RPC执行超时时间作为延迟时间
          long delayMillis = getRpcExecuteTimeout();
          if (delayMillis <= 0) {
              delayMillis = 30000; // 默认30秒
          }
          
          ddsConfigLogger.info("Using RPC execute timeout ({}ms) as cleanup delay to ensure all in-flight RPC requests complete safely", 
              delayMillis);
          
          for (String dbkey : dbkeysToCleanup) {
              ObTableClient client = oldDataSources.get(dbkey);
              if (client != null) {
                  scheduleAsyncClose(dbkey, client, delayMillis, TimeUnit.MILLISECONDS);
              }
          }
          
          ddsConfigLogger.info("Async cleanup initiated. All RPC requests will complete within {}ms before connections are closed", 
              delayMillis);
      } else {
          ddsConfigLogger.info("No obsolete data sources to clean up");
      }
  }
  
  /**
   * 获取RPC执行超时时间
   */
  private long getRpcExecuteTimeout() {
      try {
          // 从属性中获取RPC执行超时时间
          String timeoutStr = properties.getProperty("rpc.execute.timeout", "30000");
          return Long.parseLong(timeoutStr);
      } catch (Exception e) {
          ddsConfigLogger.warn("Failed to get RPC execute timeout, using default 30 seconds", e);
          return 30000;
      }
  }
  
  /**
   * 调度异步连接关闭 - 简化版本
   */
  private void scheduleAsyncClose(String dbkey, ObTableClient client, 
                                  long delay, TimeUnit timeUnit) {
      AsyncCloseTask task = new AsyncCloseTask(dbkey, client, TimeUnit.MILLISECONDS.convert(delay, timeUnit));
      pendingCloseTasks.put(dbkey, task);
      totalAsyncCloses.incrementAndGet();
      
      ddsConfigLogger.info("Scheduling async close for ObTableClient dbkey: {} with delay: {} {} (elapsed since creation: {}ms)", 
          dbkey, delay, timeUnit, System.currentTimeMillis() - task.creationTime);
      
      scheduledExecutor.schedule(() -> {
          try {
              task.run();
              pendingCloseTasks.remove(dbkey);
              completedAsyncCloses.incrementAndGet();
              ddsConfigLogger.debug("Async close task completed and removed from pending tasks for dbkey: {}", dbkey);
          } catch (Exception e) {
              ddsConfigLogger.error("Error executing async close task for dbkey: {}", dbkey, e);
              pendingCloseTasks.remove(dbkey);
          }
      }, delay, timeUnit);
  }
  
  /**
   * Get current configuration snapshot.
   */
  public VersionedConfigSnapshot getCurrentConfig() {
      return currentConfig.get();
  }
  
  /**
   * Configuration update task type.
   */
  public enum ConfigUpdateType {
      GROUP_CLUSTER,
      GROUP_CLUSTER_DBKEY,
      ATTRIBUTES,
      DOUBLE_WRITE_RULES,
      WHITE_LIST_RULES,
      SELF_ADJUST_RULES,
      TEST_LOAD_RULE
  }
  
  /**
   * Configuration update task.
   */
  public static class ConfigUpdateTask {
      private final ConfigUpdateType type;
      private final Object config;
      private final long timestamp;
      
      public ConfigUpdateTask(ConfigUpdateType type, Object config, Object config2, Object config3) {
          this.type = type;
          this.config = config;
          this.timestamp = System.currentTimeMillis();
      }
      
      public ConfigUpdateType getType() {
          return type;
      }
      
      public Object getConfig() {
          return config;
      }
      
      public long getTimestamp() {
          return timestamp;
      }
  }


  /**
   * 创建或复用原子数据源
   * 复用配置未变化的连接，避免不必要的重连
   */
  private Map<String, ObTableClient> createOrReuseAtomDataSources(
          Map<String, ExtendedDataSourceConfig> configsToUpdate,
          Map<String, ObTableClient> existingAtomDataSources,
          ObTableClient.RunningMode runningMode,
          Properties tableClientProperty) throws Exception {
      
      Map<String, ObTableClient> newAtomDataSources = new ConcurrentHashMap<>();
      
      // 1. 复用配置未变化的连接
      if (existingAtomDataSources != null) {
          for (Map.Entry<String, ObTableClient> entry : existingAtomDataSources.entrySet()) {
              String dbkey = entry.getKey();
              ObTableClient existingClient = entry.getValue();
              
              // 如果这个dbkey不在更新列表中，说明配置未变化，可以复用
              if (!configsToUpdate.containsKey(dbkey)) {
                  newAtomDataSources.put(dbkey, existingClient);
                  ddsConfigLogger.info("Reusing existing ObTableClient for dbkey: {}", dbkey);
              } else {
                  ddsConfigLogger.info("Config changed for dbkey: {}, will create new ObTableClient", dbkey);
              }
          }
      }
      
      // 2. 为配置发生变化的dbkey创建新的连接
      if (configsToUpdate != null && !configsToUpdate.isEmpty()) {
          Map<String, ExtendedDataSourceConfig> newConfigs = new ConcurrentHashMap<>();
          for (Map.Entry<String, ExtendedDataSourceConfig> entry : configsToUpdate.entrySet()) {
              if (entry.getValue() != null) { // 只创建真正需要更新的配置
                  newConfigs.put(entry.getKey(), entry.getValue());
              }
          }
          
          if (!newConfigs.isEmpty()) {
              ddsConfigLogger.info("Creating {} new ObTableClient instances for updated configs", newConfigs.size());
              Map<String, ObTableClient> newlyCreatedClients = DataSourceFactory.createAtomDataSources(
                  newConfigs, runningMode, tableClientProperty);
              newAtomDataSources.putAll(newlyCreatedClients);
          }
      }
      
      if (configsToUpdate == null) {
          configsToUpdate = Collections.emptyMap();
      }
      
      ddsConfigLogger.info("Atom data sources summary: total={}, reused={}, newly created={}", 
          newAtomDataSources.size(), 
          newAtomDataSources.size() - configsToUpdate.size(), 
          configsToUpdate.size());
      
      return newAtomDataSources;
  }
  
  private Map<String, ExtendedDataSourceConfig> resolveLatestExtendedConfigs() {
    Map<String, ExtendedDataSourceConfig> configs = null;
    try {
      configs = extendedDataSourceSupplier.get();
    } catch (Exception e) {
      ddsConfigLogger.warn("Failed to fetch extended data source configs from supplier", e);
    }
    if (configs == null || configs.isEmpty()) {
      VersionedConfigSnapshot snapshot = currentConfig.get();
      if (snapshot != null) {
        configs = snapshot.getDataSourceConfigs();
      }
    }
    return configs != null ? new ConcurrentHashMap<>(configs) : new ConcurrentHashMap<>();
  }

  /**
   * 异步连接关闭任务 - 简化版本
   * 直接使用延迟策略，无需复杂的安全检查
   */
  private static class AsyncCloseTask implements Runnable {
    private final String dbkey;
    private final ObTableClient client;
    private final long delayMillis;
    private final long creationTime;
    
    public AsyncCloseTask(String dbkey, ObTableClient client, long delayMillis) {
      this.dbkey = dbkey;
      this.client = client;
      this.delayMillis = delayMillis;
      this.creationTime = System.currentTimeMillis();
    }
    
    @Override
    public void run() {
      try {
        long waitTime = System.currentTimeMillis() - creationTime;
        ddsConfigLogger.info("Executing async close for ObTableClient dbkey: {} (waited {}ms, requested delay: {}ms)", 
            dbkey, waitTime, delayMillis);
        
        // close connection directly, delay strategy ensures all RPC requests complete
        client.close();
        ddsConfigLogger.info("Successfully completed async close for ObTableClient dbkey: {}", dbkey);
        
      } catch (Exception e) {
        ddsConfigLogger.error("Error during async close for ObTableClient dbkey: {}", dbkey, e);
        try {
          // ensure to close connection even if error occurs
          client.close();
        } catch (Exception closeException) {
          ddsConfigLogger.error("Failed to close ObTableClient dbkey: {} during error handling", dbkey, closeException);
        }
      }
    }
  }
  
  /**
   * get async cleanup stats
   */
  public String getAsyncCleanupStats() {
      int total = totalAsyncCloses.get();
      int completed = completedAsyncCloses.get();
      int pending = pendingCloseTasks.size();
      
      return String.format("Async cleanup stats - Total: %d, Completed: %d, Pending: %d", 
          total, completed, pending);
  }
  
  /**
   * shutdown async cleanup executor
   * used for graceful shutdown
   */
  public void shutdownAsyncCleanup() {
      ddsConfigLogger.info("Shutting down async cleanup executor, stats: {}", getAsyncCleanupStats());
      
      try {
          scheduledExecutor.shutdown();
          if (!scheduledExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
              scheduledExecutor.shutdownNow();
          }
          ddsConfigLogger.info("Async cleanup executor shutdown completed");
      } catch (InterruptedException e) {
          ddsConfigLogger.error("Interrupted during async cleanup shutdown", e);
          scheduledExecutor.shutdownNow();
          Thread.currentThread().interrupt();
      }
  }

  /**
   * 验证Group配置与数据源配置的一致性
   * 确保Group配置中引用的所有dbkey都有对应的数据源配置
   */
  private void validateConfigConsistency(GroupClusterConfig groupClusterConfig,
                                        Map<String, ExtendedDataSourceConfig> availableConfigs) {
    if (groupClusterConfig == null || groupClusterConfig.getGroupCluster().isEmpty()) {
      ddsConfigLogger.debug("No group cluster config found, skipping consistency validation");
      return;
    }
    
    Set<String> requiredDbkeys = new HashSet<>();
    for (GroupDataSourceConfig groupConfig : groupClusterConfig.getGroupCluster().values()) {
      GroupDataSourceWeight groupWeight = groupConfig.getGroupDataSourceWeight();
      if (groupWeight != null) {
        List<AtomDataSourceWeight> weights = groupWeight.getDataSourceReadWriteWeights();
        if (weights != null) {
          for (AtomDataSourceWeight weight : weights) {
            requiredDbkeys.add(weight.getDbkey());
          }
        }
      }
    }
    
    Set<String> availableDbkeys = availableConfigs.keySet();
    Set<String> missingDbkeys = new HashSet<>(requiredDbkeys);
    missingDbkeys.removeAll(availableDbkeys);
    
    if (!missingDbkeys.isEmpty()) {
      ddsConfigLogger.error("DDS_CONFIG_VALIDATION_FAILED - Group configuration references missing dbkeys: {}. Available: {}. Required: {}", 
                           missingDbkeys, availableDbkeys, requiredDbkeys);
      
      throw new IllegalArgumentException(String.format(
          "CRITICAL: Group configuration references missing dbkeys: %s. Available: %s. " +
          "Please ensure all dbkeys referenced in group weights exist in atom data sources before applying configuration.",
          missingDbkeys, availableDbkeys));
    }
    
    ddsConfigLogger.info("DDS_CONFIG_VALIDATION_SUCCESS - All required dbkeys are available. Required: {}, Available: {}", 
                        requiredDbkeys, availableDbkeys);
  }
  
  /**
   * 验证Group配置与现有连接的兼容性
   * 在RULES_ONLY模式下，确保新的Group配置不会引用不存在的dbkey
   */
  private void validateGroupConfigCompatibility(GroupClusterConfig groupClusterConfig,
                                               Map<String, ObTableClient> existingAtomDataSources) {
      Set<String> availableDbkeys = existingAtomDataSources.keySet();
      Set<String> missingDbkeys = new HashSet<>();
      
      for (GroupDataSourceConfig groupConfig : groupClusterConfig.getGroupCluster().values()) {
          GroupDataSourceWeight groupWeight = groupConfig.getGroupDataSourceWeight();
          if (groupWeight != null) {
              List<AtomDataSourceWeight> weights = groupWeight.getDataSourceReadWriteWeights();
              if (weights != null) {
                  for (AtomDataSourceWeight weight : weights) {
                      String dbkey = weight.getDbkey();
                      if (!availableDbkeys.contains(dbkey)) {
                          missingDbkeys.add(dbkey);
                      }
                  }
              }
          }
      }
      
      if (!missingDbkeys.isEmpty()) {
          throw new IllegalArgumentException(String.format(
              "CRITICAL: Group configuration references missing dbkeys: %s. Available: %s. " +
              "Please ensure all dbkeys referenced in group weights exist in atom data sources.",
              missingDbkeys, availableDbkeys));
      }
  }
}

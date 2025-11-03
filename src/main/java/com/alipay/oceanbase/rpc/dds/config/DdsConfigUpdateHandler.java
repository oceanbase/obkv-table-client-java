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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.dds.group.ObTableClientGroup;
import com.alipay.oceanbase.rpc.dds.util.ConfigWrapper;
import com.alipay.oceanbase.rpc.dds.util.DataSourceFactory;
import com.alipay.oceanbase.rpc.dds.util.VersionedConfigSnapshot;
import com.alipay.sofa.dds.config.AttributesConfig;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.advanced.DoubleWriteRule;
import com.alipay.sofa.dds.config.advanced.TestLoadRule;
import com.alipay.sofa.dds.config.dynamic.DynamicConfigHandler;
import com.alipay.sofa.dds.config.group.AtomDataSourceWeight;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupClusterDbkeyConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceWeight;
import com.alipay.sofa.dds.config.rule.AppRule;

public class DdsConfigUpdateHandler implements DynamicConfigHandler {
    
  private final AtomicReference<VersionedConfigSnapshot> currentConfig;
  ObTableClient.RunningMode runningMode;
  Properties properties;
  
  private final ReentrantReadWriteLock configLock = new ReentrantReadWriteLock();

  private static final Logger logger = LoggerFactory.getLogger(DdsConfigUpdateHandler.class);

  private Supplier<Map<String, ExtendedDataSourceConfig>> extendedDataSourceSupplier = () -> Collections.emptyMap();
  private Supplier<AppRule> appRuleSupplier = () -> null;
  
  public DdsConfigUpdateHandler(ObTableClient.RunningMode runningMode, Properties tableClientProperty,
      AtomicReference<VersionedConfigSnapshot> sharedConfig) {
    this.runningMode = runningMode;
    this.properties = tableClientProperty;
    this.currentConfig = sharedConfig != null ? sharedConfig : new AtomicReference<>();
  }

  public void setExtendedDataSourceSupplier(Supplier<Map<String, ExtendedDataSourceConfig>> supplier) {
    this.extendedDataSourceSupplier = supplier != null ? supplier : () -> Collections.emptyMap();
  }

  public void setAppRuleSupplier(Supplier<AppRule> supplier) {
    this.appRuleSupplier = supplier != null ? supplier : () -> null;
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
      try {
          logger.info("Processing config update task: {}", task.getType());
          
          // 1. Build new configuration
          VersionedConfigSnapshot newConfig = buildNewConfig(task);
          
          // 2. Replace configuration atomically
          replaceConfigAtomically(newConfig);
          
          logger.info("Config update completed successfully for task: {}", task.getType());
          
      } catch (Exception e) {
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
   */
  private VersionedConfigSnapshot buildGroupClusterConfig(
          VersionedConfigSnapshot current, 
          GroupClusterConfig newGroupCluster) throws Exception {
      
      // 1. Create new data source configurations
      Map<String, ExtendedDataSourceConfig> availableConfigs = resolveLatestExtendedConfigs();
      Map<String, ExtendedDataSourceConfig> newDataSourceConfigs =
          createDataSourceConfigsFromGroupCluster(newGroupCluster, availableConfigs);
      
      // 2. Create new atom data sources
      Map<String, ObTableClient> newAtomDataSources = DataSourceFactory.createAtomDataSources(
          newDataSourceConfigs, runningMode, properties);
      
      // 3. Create new client groups
      Map<Integer, ObTableClientGroup> newGroupDataSources = DataSourceFactory.createGroupDataSources(
          newGroupCluster, newAtomDataSources);
      
      // 4. Get latest AppRule (rebuild logical tables if updated)
      AppRule latestAppRule;
      try {
        latestAppRule = appRuleSupplier.get();
        if (latestAppRule == null) {
          latestAppRule = current.getAppRule();
        }
      } catch (Exception e) {
        logger.warn("Failed to fetch latest AppRule from supplier, using current snapshot's AppRule", e);
        latestAppRule = current.getAppRule();
      }
      
      // 5. Wrap configuration into versioned snapshot
      boolean appRuleChanged = latestAppRule != current.getAppRule();
      if (appRuleChanged) {
        logger.info("AppRule updated, will rebuild LogicalTables with new AppRule");
      }
      
      return ConfigWrapper.wrapConfig(
          newGroupCluster,
          newDataSourceConfigs,
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
      
      logger.info("Building group cluster dbkey configuration");
      
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
      
      logger.info("Attributes configuration update - reusing current snapshot");
      
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
      
      logger.info("Double write rules configuration update - reusing current snapshot");
      
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
      
      logger.info("White list rules configuration update - reusing current snapshot");
      
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
      
      logger.info("Self adjust rules configuration update - reusing current snapshot");
      
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
      
      logger.info("Test load rule configuration update - reusing current snapshot");
      
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
   */
  private void replaceConfigAtomically(VersionedConfigSnapshot newConfig) {
      configLock.writeLock().lock();
      try {
          VersionedConfigSnapshot oldConfig = currentConfig.get();
          currentConfig.set(newConfig);

          // Clean up obsolete data sources
          if (oldConfig != null) {
              cleanupObsoleteDataSources(oldConfig, newConfig);
          }

          logger.info("Configuration replaced atomically, deprecated indexes: {}", 
              newConfig.getDeprecatedElasticIndexes());

      } finally {
          configLock.writeLock().unlock();
      }
  }
  
  /**
   * Clean up obsolete data sources that are no longer used.
   */
  private void cleanupObsoleteDataSources(VersionedConfigSnapshot oldConfig, 
                                          VersionedConfigSnapshot newConfig) {
      Map<String, ObTableClient> oldDataSources = oldConfig.getAtomDataSources();
      Map<String, ObTableClient> newDataSources = newConfig.getAtomDataSources();
      
      if (oldDataSources == null || oldDataSources.isEmpty()) {
          return;
      }
      
      // Find dbkeys that are removed in the new configuration
      Set<String> removedDbkeys = new HashSet<>(oldDataSources.keySet());
      if (newDataSources != null) {
          removedDbkeys.removeAll(newDataSources.keySet());
      }
      
      // Close obsolete ObTableClient instances
      if (!removedDbkeys.isEmpty()) {
          logger.info("Cleaning up {} obsolete data sources: {}", 
              removedDbkeys.size(), removedDbkeys);
          
          for (String dbkey : removedDbkeys) {
              ObTableClient client = oldDataSources.get(dbkey);
              if (client != null) {
                  try {
                      client.close();
                      logger.info("Successfully closed obsolete ObTableClient for dbkey: {}", dbkey);
                  } catch (Exception e) {
                      logger.error("Failed to close ObTableClient for dbkey: {}", dbkey, e);
                  }
              }
          }
      }
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


  private Map<String, ExtendedDataSourceConfig> createDataSourceConfigsFromGroupCluster(
      GroupClusterConfig groupCluster, Map<String, ExtendedDataSourceConfig> availableConfigs) {
    Map<String, ExtendedDataSourceConfig> dataSourceConfigs = new ConcurrentHashMap<>();
    if (groupCluster == null || availableConfigs == null || availableConfigs.isEmpty()) {
      return dataSourceConfigs;
    }

    Map<String, GroupDataSourceConfig> groupMap = groupCluster.getGroupCluster();
    if (groupMap == null || groupMap.isEmpty()) {
      return dataSourceConfigs;
    }

    for (GroupDataSourceConfig groupConfig : groupMap.values()) {
      if (groupConfig == null) {
        continue;
      }

      GroupDataSourceWeight groupWeight = groupConfig.getGroupDataSourceWeight();
      if (groupWeight == null || groupWeight.getDataSourceReadWriteWeights() == null) {
        continue;
      }

      for (AtomDataSourceWeight weight : groupWeight.getDataSourceReadWriteWeights()) {
        if (weight == null) {
          continue;
        }

        String dbkey = weight.getDbkey();
        if (dbkey == null || dbkey.isEmpty()) {
          continue;
        }

        ExtendedDataSourceConfig config = availableConfigs.get(dbkey);
        if (config != null) {
          dataSourceConfigs.putIfAbsent(dbkey, config);
        } else {
          logger.warn("Missing ExtendedDataSourceConfig for dbkey {} while rebuilding group cluster", dbkey);
        }
      }
    }
    return dataSourceConfigs;
  }

  private Map<String, ExtendedDataSourceConfig> resolveLatestExtendedConfigs() {
    Map<String, ExtendedDataSourceConfig> configs = null;
    try {
      configs = extendedDataSourceSupplier.get();
    } catch (Exception e) {
      logger.warn("Failed to fetch extended data source configs from supplier", e);
    }
    if (configs == null || configs.isEmpty()) {
      VersionedConfigSnapshot snapshot = currentConfig.get();
      if (snapshot != null) {
        configs = snapshot.getDataSourceConfigs();
      }
    }
    return configs != null ? new ConcurrentHashMap<>(configs) : new ConcurrentHashMap<>();
  }
}

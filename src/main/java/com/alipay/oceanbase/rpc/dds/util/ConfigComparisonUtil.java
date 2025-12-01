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

package com.alipay.oceanbase.rpc.dds.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceWeight;
import com.alipay.sofa.dds.config.group.AtomDataSourceWeight;

/**
 * 配置比较工具类，用于判断数据源配置是否发生变化
 * 
 * @since 4.3.0
 */
public class ConfigComparisonUtil {

    private static final Logger logger          = LoggerFactory
                                                    .getLogger(ConfigComparisonUtil.class);
    private static final Logger ddsConfigLogger = TableClientLoggerFactory.getDDSConfigLogger();

    /**
     * 比较两个数据源配置是否完全相同
     * 
     * @param oldConfig 旧配置
     * @param newConfig 新配置
     * @return true表示配置相同，false表示配置不同
     */
    public static boolean isConfigEqual(ExtendedDataSourceConfig oldConfig, 
                                       ExtendedDataSourceConfig newConfig) {
        if (oldConfig == null && newConfig == null) {
            return true;
        }
        if (oldConfig == null || newConfig == null) {
            return false;
        }
        
        // 比较关键配置项
        return Objects.equals(safeGet(oldConfig::getUsername), safeGet(newConfig::getUsername))
            && Objects.equals(safeGet(oldConfig::getPassword), safeGet(newConfig::getPassword))
            && Objects.equals(safeGet(oldConfig::getJdbcUrl), safeGet(newConfig::getJdbcUrl))
            && Objects.equals(safeGet(oldConfig::getAppName), safeGet(newConfig::getAppName))
            && Objects.equals(safeGet(oldConfig::getAppDsName), safeGet(newConfig::getAppDsName));
    }

    /**
     * 比较两个GroupClusterConfig是否完全相同（包括权重配置）
     * 
     * @param oldConfig 旧配置
     * @param newConfig 新配置
     * @return true表示配置相同，false表示配置不同
     */
    public static boolean isGroupClusterConfigEqual(GroupClusterConfig oldConfig,
                                                    GroupClusterConfig newConfig) {
        if (oldConfig == null && newConfig == null) {
            return true;
        }
        if (oldConfig == null || newConfig == null) {
            return false;
        }

        Map<String, GroupDataSourceConfig> oldGroupMap = oldConfig.getGroupCluster();
        Map<String, GroupDataSourceConfig> newGroupMap = newConfig.getGroupCluster();

        if (oldGroupMap == null && newGroupMap == null) {
            return true;
        }
        if (oldGroupMap == null || newGroupMap == null || oldGroupMap.size() != newGroupMap.size()) {
            return false;
        }

        // 比较每个组的配置
        for (Map.Entry<String, GroupDataSourceConfig> entry : oldGroupMap.entrySet()) {
            String groupKey = entry.getKey();
            GroupDataSourceConfig oldGroupConfig = entry.getValue();
            GroupDataSourceConfig newGroupConfig = newGroupMap.get(groupKey);

            if (!isGroupDataSourceConfigEqual(oldGroupConfig, newGroupConfig)) {
                return false;
            }
        }

        return true;
    }

    /**
     * 比较单个GroupDataSourceConfig是否相同（包括权重）
     */
    private static boolean isGroupDataSourceConfigEqual(GroupDataSourceConfig oldConfig,
                                                        GroupDataSourceConfig newConfig) {
        if (oldConfig == null && newConfig == null) {
            return true;
        }
        if (oldConfig == null || newConfig == null) {
            return false;
        }

        // 比较组键和权重字符串
        if (!Objects.equals(oldConfig.getGroupKey(), newConfig.getGroupKey())
            || !Objects.equals(oldConfig.getWeightString(), newConfig.getWeightString())) {
            return false;
        }

        // 详细比较权重配置
        GroupDataSourceWeight oldWeight = oldConfig.getGroupDataSourceWeight();
        GroupDataSourceWeight newWeight = newConfig.getGroupDataSourceWeight();

        if (!isGroupDataSourceWeightEqual(oldWeight, newWeight)) {
            return false;
        }

        return true;
    }

    /**
     * 比较GroupDataSourceWeight是否相同（包括每个原子数据源的读写权重）
     */
    private static boolean isGroupDataSourceWeightEqual(GroupDataSourceWeight oldWeight,
                                                        GroupDataSourceWeight newWeight) {
        if (oldWeight == null && newWeight == null) {
            return true;
        }
        if (oldWeight == null || newWeight == null) {
            return false;
        }

        List<AtomDataSourceWeight> oldWeights = oldWeight.getDataSourceReadWriteWeights();
        List<AtomDataSourceWeight> newWeights = newWeight.getDataSourceReadWriteWeights();

        if (oldWeights == null && newWeights == null) {
            return true;
        }
        if (oldWeights == null || newWeights == null || oldWeights.size() != newWeights.size()) {
            return false;
        }

        // 比较每个原子数据源的权重配置
        // 按dbkey进行对齐比较，而不是按位置索引
        Map<String, AtomDataSourceWeight> newWeightsMap = new HashMap<>();
        for (AtomDataSourceWeight newWeightItem : newWeights) {
            newWeightsMap.put(newWeightItem.getDbkey(), newWeightItem);
        }
        
        for (AtomDataSourceWeight oldWeightItem : oldWeights) {
            String dbkey = oldWeightItem.getDbkey();
            AtomDataSourceWeight correspondingNewWeight = newWeightsMap.get(dbkey);
            
            // 检查新的配置中是否存在对应的dbkey
            if (correspondingNewWeight == null) {
                logger.info("Weight config removed for dbkey: {}", dbkey);
                return false;
            }
            
            // 比较权重配置
            if (!isAtomDataSourceWeightEqual(oldWeightItem, correspondingNewWeight)) {
                logger.info("Weight config changed for dbkey: {}", dbkey);
                return false;
            }
            
            // 从新权重映射中移除已比较的项，检查是否有新增的权重
            newWeightsMap.remove(dbkey);
        }
        
        // 检查是否有新增的权重配置
        if (!newWeightsMap.isEmpty()) {
            logger.info("New weight configs added for dbkeys: {}", newWeightsMap.keySet());
            return false;
        }

        return true;
    }

    /**
     * 比较AtomDataSourceWeight是否相同
     */
    private static boolean isAtomDataSourceWeightEqual(AtomDataSourceWeight oldWeight,
                                                       AtomDataSourceWeight newWeight) {
        if (oldWeight == null && newWeight == null) {
            return true;
        }
        if (oldWeight == null || newWeight == null) {
            return false;
        }

        return Objects.equals(oldWeight.getDbkey(), newWeight.getDbkey())
               && Objects.equals(oldWeight.getIndex(), newWeight.getIndex())
               && Objects.equals(oldWeight.getReadWeight(), newWeight.getReadWeight())
               && Objects.equals(oldWeight.getWriteWeight(), newWeight.getWriteWeight());
    }

    /**
     * 安全获取配置值，处理null值
     */
    private static String safeGet(SupplierWithException<String> supplier) {
        try {
            String value = supplier.get();
            return value != null ? value : "";
        } catch (Exception e) {
            logger.warn("Failed to get config value", e);
            return "";
        }
    }

    /**
     * 找出需要更新的数据源配置
     * 
     * @param oldConfigs 旧配置映射
     * @param newConfigs 新配置映射
     * @return 需要更新的配置映射（只包含真正需要更新的配置）
     */
    public static Map<String, ExtendedDataSourceConfig> findConfigsToUpdate(
            Map<String, ExtendedDataSourceConfig> oldConfigs,
            Map<String, ExtendedDataSourceConfig> newConfigs) {
        
        Map<String, ExtendedDataSourceConfig> configsToUpdate = new ConcurrentHashMap<>();
        
        if (newConfigs == null || newConfigs.isEmpty()) {
            ddsConfigLogger.info("DDS_CONFIG_COMPARE_RESULT," +
                    "oldConfigCount=0,newConfigCount=0,configsToUpdate=0," +
                    "configsToRemove=0,configsToAdd=0,updateType=NONE");
            return configsToUpdate;
        }
        
        int configsToAdd = 0;
        int configsToRemove = 0;
        int configsChanged = 0;
        
        // 找出新增的或变化的配置
        for (Map.Entry<String, ExtendedDataSourceConfig> entry : newConfigs.entrySet()) {
            String dbkey = entry.getKey();
            ExtendedDataSourceConfig newConfig = entry.getValue();
            
            ExtendedDataSourceConfig oldConfig = oldConfigs != null ? oldConfigs.get(dbkey) : null;
            if (oldConfig == null) {
                // 新增的配置
                configsToAdd++;
                logger.info("New data source config found for dbkey: {}", dbkey);
                ddsConfigLogger.info("DDS_CONFIG_ADD," +
                        "dbkey={},config={}", dbkey, newConfig.getAppDsName());
                configsToUpdate.put(dbkey, newConfig);
            } else if (!isConfigEqual(oldConfig, newConfig)) {
                // 配置发生变化
                configsChanged++;
                logger.info("Data source config changed for dbkey: {}", dbkey);
                ddsConfigLogger.info("DDS_CONFIG_CHANGE," +
                        "dbkey={},oldConfig={},newConfig={}", 
                        dbkey, oldConfig.getAppDsName(), newConfig.getAppDsName());
                configsToUpdate.put(dbkey, newConfig);
            }
            // 如果配置相同，则不添加到更新列表中（复用）
        }
        
        // 计算需要移除的配置
        if (oldConfigs != null) {
            for (String oldDbkey : oldConfigs.keySet()) {
                if (!newConfigs.containsKey(oldDbkey)) {
                    configsToRemove++;
                }
            }
        }
        
        String updateType = "PARTIAL";
        if (configsToAdd > 0 && configsChanged == 0 && configsToRemove == 0) {
            updateType = "ADD_ONLY";
        } else if (configsToAdd == 0 && configsChanged > 0 && configsToRemove == 0) {
            updateType = "CHANGE_ONLY";
        } else if (configsToAdd == 0 && configsChanged == 0 && configsToRemove > 0) {
            updateType = "REMOVE_ONLY";
        } else if (configsToAdd == 0 && configsChanged == 0 && configsToRemove == 0) {
            updateType = "NONE";
        }
        
        ddsConfigLogger.info("DDS_CONFIG_COMPARE_RESULT," +
                "oldConfigCount={},newConfigCount={},configsToUpdate={}," +
                "configsToRemove={},configsToAdd={},configsChanged={},updateType={}", 
                oldConfigs != null ? oldConfigs.size() : 0,
                newConfigs.size(), configsToUpdate.size(),
                configsToRemove, configsToAdd, configsChanged, updateType);
        
        return configsToUpdate;
    }

    /**
     * 找出需要清理的数据源
     * 
     * @param oldDbkeys 旧的dbkey集合
     * @param newConfigs 新配置映射
     * @return 需要清理的dbkey集合
     */
    public static java.util.Set<String> findConfigsToRemove(
            java.util.Set<String> oldDbkeys,
            Map<String, ExtendedDataSourceConfig> newConfigs) {
        
        java.util.Set<String> toRemove = new java.util.HashSet<>();
        
        if (oldDbkeys == null || oldDbkeys.isEmpty()) {
            return toRemove;
        }
        
        for (String dbkey : oldDbkeys) {
            if (newConfigs == null || !newConfigs.containsKey(dbkey)) {
                toRemove.add(dbkey);
                logger.info("Data source config removed for dbkey: {}", dbkey);
            }
        }
        
        return toRemove;
    }

    /**
     * 检查是否只需要更新规则（权重），连接配置保持不变
     * 
     * @param oldGroupCluster 旧集群配置
     * @param newGroupCluster 新集群配置
     * @param oldDataSourceConfigs 旧数据源配置
     * @param newDataSourceConfigs 新数据源配置
     * @return 更新类型：CONNECTIONS_AND_RULES（都变化）、RULES_ONLY（仅规则变化）、CONNECTIONS_ONLY（仅连接变化）、NONE（无变化）
     */
    public static ConfigUpdateType analyzeUpdateType(GroupClusterConfig oldGroupCluster,
                                                     GroupClusterConfig newGroupCluster,
                                                     Map<String, ExtendedDataSourceConfig> oldDataSourceConfigs,
                                                     Map<String, ExtendedDataSourceConfig> newDataSourceConfigs) {

        boolean groupClusterChanged = !isGroupClusterConfigEqual(oldGroupCluster, newGroupCluster);
        boolean dataSourceConfigsChanged = !findConfigsToUpdate(oldDataSourceConfigs,
            newDataSourceConfigs).isEmpty();

        if (groupClusterChanged && dataSourceConfigsChanged) {
            logger.info("Configuration update analysis: BOTH connections and rules changed");
            return ConfigUpdateType.CONNECTIONS_AND_RULES;
        } else if (groupClusterChanged) {
            logger
                .info("Configuration update analysis: ONLY rules (weights) changed, connections will be reused");
            return ConfigUpdateType.RULES_ONLY;
        } else if (dataSourceConfigsChanged) {
            logger
                .info("Configuration update analysis: ONLY connections changed, rules will be reused");
            return ConfigUpdateType.CONNECTIONS_ONLY;
        } else {
            logger
                .info("Configuration update analysis: NO changes detected, everything will be reused");
            return ConfigUpdateType.NONE;
        }
    }

    /**
     * 配置更新类型枚举
     */
    public enum ConfigUpdateType {
        CONNECTIONS_AND_RULES, // 连接和规则都变化
        RULES_ONLY, // 仅规则（权重）变化
        CONNECTIONS_ONLY, // 仅连接配置变化
        NONE // 无变化
    }

    @FunctionalInterface
    public interface SupplierWithException<T> {
        T get() throws Exception;
    }
}

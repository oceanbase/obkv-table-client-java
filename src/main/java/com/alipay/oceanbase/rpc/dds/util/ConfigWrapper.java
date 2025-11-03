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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.dds.group.ObTableClientGroup;
import com.alipay.oceanbase.rpc.dds.rule.LogicalTable;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.group.AtomDataSourceWeight;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceWeight;
import com.alipay.sofa.dds.config.rule.AppRule;
import com.alipay.sofa.dds.config.rule.ShardRule;

public class ConfigWrapper {

    /**
     * Wrap configuration into snapshot (basic version for initialization without data sources)
     */
    public static VersionedConfigSnapshot wrapConfig(GroupClusterConfig groupClusterConfig,
                                                     Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs,
                                                     AppRule appRule) throws Exception {
        return wrapConfig(groupClusterConfig, extendedDataSourceConfigs, appRule,
            Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Wrap configuration into snapshot (full version with all parameters)
     * @param groupClusterConfig cluster configuration
     * @param extendedDataSourceConfigs extended data source configurations
     * @param appRule application rules
     * @param atomDataSources atom data source mapping (optional, empty map if null)
     * @param groupDataSources client group mapping (optional, empty map if null)
     * @return configuration snapshot
     * @throws Exception if build fails
     */
    public static VersionedConfigSnapshot wrapConfig(GroupClusterConfig groupClusterConfig,
                                                     Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs,
                                                     AppRule appRule,
                                                     Map<String, ObTableClient> atomDataSources,
                                                     Map<Integer, ObTableClientGroup> groupDataSources)
                                                                                                       throws Exception {

        // 1. Identify deprecated elastic indexes
        Set<Integer> deprecatedElasticIndexes = identifyDeprecatedElasticIndexes(
            groupClusterConfig, extendedDataSourceConfigs);

        // 2. Build elastic configuration mapping
        Map<String, ElasticWeightConfig> elasticConfigs = buildElasticConfigs(groupClusterConfig,
            extendedDataSourceConfigs);

        // 3. Build logical table mapping (throw exception on failure)
        Map<String, LogicalTable> logicalTables = buildLogicalTables(appRule);

        // 4. Use provided data sources, or empty map if null
        Map<String, ObTableClient> finalAtomDataSources = atomDataSources != null ? atomDataSources
            : Collections.emptyMap();
        Map<Integer, ObTableClientGroup> finalGroupDataSources = groupDataSources != null ? groupDataSources
            : Collections.emptyMap();

        return new VersionedConfigSnapshot(System.currentTimeMillis(), groupClusterConfig,
            extendedDataSourceConfigs, appRule, deprecatedElasticIndexes, elasticConfigs,
            finalAtomDataSources, finalGroupDataSources, logicalTables);
    }

    /**
     * 识别需要废弃的弹性位
     */
    public static Set<Integer> identifyDeprecatedElasticIndexes(
            GroupClusterConfig groupClusterConfig,
            Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs) {
        
        Set<Integer> deprecated = new HashSet<>();
        
        // 比较新旧配置，找出被移除或权重变为0的弹性位
        for (GroupDataSourceConfig groupConfig : groupClusterConfig.getGroupCluster().values()) {
            GroupDataSourceWeight groupWeight = groupConfig.getGroupDataSourceWeight();
            for (AtomDataSourceWeight atomWeight : groupWeight.getDataSourceReadWriteWeights()) {
                // 检查写权重是否为0
                if (atomWeight.getWriteWeight() == 0) {
                    deprecated.add(atomWeight.getIndex());
                }
            }
        }
        
        return deprecated;
    }

    /**
     * 构建弹性位配置映射
     * @param groupClusterConfig 集群配置
     * @param extendedDataSourceConfigs 数据源配置
     * @return 弹性位配置映射
     */
    public static Map<String, ElasticWeightConfig> buildElasticConfigs(
            GroupClusterConfig groupClusterConfig,
            Map<String, ExtendedDataSourceConfig> extendedDataSourceConfigs) {
        
        Map<String, ElasticWeightConfig> elasticConfigs = new HashMap<>();
        
        for (GroupDataSourceConfig groupConfig : groupClusterConfig.getGroupCluster().values()) {
            GroupDataSourceWeight groupWeight = groupConfig.getGroupDataSourceWeight();
            for (AtomDataSourceWeight atomWeight : groupWeight.getDataSourceReadWriteWeights()) {
                String dbkey = atomWeight.getDbkey();
                ExtendedDataSourceConfig dataSourceConfig = extendedDataSourceConfigs.get(dbkey);
                
                if (dataSourceConfig != null) {
                    ElasticWeightConfig elasticConfig = new ElasticWeightConfig(
                        atomWeight.getIndex(),
                        dbkey,
                        atomWeight.getReadWeight(),
                        atomWeight.getWriteWeight(),
                        atomWeight.getWriteWeight() > 0,
                        System.currentTimeMillis()
                    );
                    elasticConfigs.put(dbkey, elasticConfig);
                }
            }
        }
        
        return elasticConfigs;
    }

    /**
     * 构建逻辑表映射
     * @param appRule 应用规则
     * @return 逻辑表映射
     * @throws Exception 初始化失败时抛出异常
     */
    public static Map<String, LogicalTable> buildLogicalTables(AppRule appRule) throws Exception {
        Map<String, LogicalTable> logicalTables = new ConcurrentHashMap<>();
        if (appRule != null && appRule.getShardRules() != null) {
            for (Map.Entry<String, ShardRule> ruleEntry : appRule.getShardRules().entrySet()) {
                // 1、Trim the table name to avoid the unexpected char
                // 2. Upper the table name to unify the usage
                String tableName = ruleEntry.getKey().trim().toUpperCase();
                ShardRule rule = ruleEntry.getValue();
                try {
                    LogicalTable table = new LogicalTable(tableName, rule.getTbRules(),
                        rule.getDbRules(), rule.getElasticRules(), rule.getTbNamePattern(),
                        rule.getRowKeyColumn());
                    table.init();
                    logicalTables.put(tableName, table);
                } catch (Exception e) {
                    throw new Exception("Failed to initialize logical table '" + tableName 
                                      + "' with tbRules=" + rule.getTbRules() 
                                      + ", dbRules=" + rule.getDbRules()
                                      + ", elasticRules=" + rule.getElasticRules()
                                      + ", tbNamePattern=" + rule.getTbNamePattern()
                                      + ", rowKeyColumn=" + rule.getRowKeyColumn(), e);
                }
            }
        }
        return logicalTables;
    }
}

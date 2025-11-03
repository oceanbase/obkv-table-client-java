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
import java.util.Map;
import java.util.Set;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.dds.group.ObTableClientGroup;
import com.alipay.oceanbase.rpc.dds.rule.LogicalTable;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.rule.AppRule;

public class VersionedConfigSnapshot {
    private final long                                  switchTimestamp;
    private final GroupClusterConfig                    groupCluster;
    private final Map<String, ExtendedDataSourceConfig> dataSourceConfigs;
    private final AppRule                               appRule;
    private final Set<Integer>                          deprecatedElasticIndexes;
    private final Map<String, ElasticWeightConfig>      elasticConfigs;
    private final Map<String, ObTableClient>            atomDataSources;
    private final Map<Integer, ObTableClientGroup>      groupDataSources;
    private final Map<String, LogicalTable>             logicalTables;

    public VersionedConfigSnapshot(long switchTimestamp,
                                 GroupClusterConfig groupCluster,
                                 Map<String, ExtendedDataSourceConfig> dataSourceConfigs,
                                 AppRule appRule,
                                 Set<Integer> deprecatedElasticIndexes,
                                 Map<String, ElasticWeightConfig> elasticConfigs,
                                 Map<String, ObTableClient> atomDataSources,
                                 Map<Integer, ObTableClientGroup> groupDataSources,
                                 Map<String, LogicalTable> logicalTables) {
    this.switchTimestamp = switchTimestamp;
    this.groupCluster = groupCluster;
    this.dataSourceConfigs = dataSourceConfigs;
    this.appRule = appRule;
    this.deprecatedElasticIndexes = deprecatedElasticIndexes;
    this.elasticConfigs = elasticConfigs;
    this.atomDataSources = atomDataSources != null
        ? Collections.unmodifiableMap(new HashMap<>(atomDataSources))
        : Collections.emptyMap();
    this.groupDataSources = groupDataSources != null
        ? Collections.unmodifiableMap(new HashMap<>(groupDataSources))
        : Collections.emptyMap();
    this.logicalTables = logicalTables != null
        ? Collections.unmodifiableMap(new HashMap<>(logicalTables))
        : Collections.emptyMap();
  }

    public long getSwitchTimestamp() {
        return switchTimestamp;
    }

    public GroupClusterConfig getGroupCluster() {
        return groupCluster;
    }

    public Map<String, ExtendedDataSourceConfig> getDataSourceConfigs() {
        return dataSourceConfigs;
    }

    public AppRule getAppRule() {
        return appRule;
    }

    public Set<Integer> getDeprecatedElasticIndexes() {
        return deprecatedElasticIndexes;
    }

    public Map<String, ElasticWeightConfig> getElasticConfigs() {
        return elasticConfigs;
    }

    public Map<String, ObTableClient> getAtomDataSources() {
        return atomDataSources;
    }

    public Map<Integer, ObTableClientGroup> getGroupDataSources() {
        return groupDataSources;
    }

    public Map<String, LogicalTable> getLogicalTables() {
        return logicalTables;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("VersionedConfigSnapshot{\n");
        sb.append("  switchTimestamp=").append(switchTimestamp)
          .append(" (").append(new java.util.Date(switchTimestamp)).append("),\n");
        
        // Group cluster info
        sb.append("  groupCluster=")
          .append(groupCluster != null ? groupCluster.getGroupCluster().size() + " groups" : "null")
          .append(",\n");
        
        // DataSource configs
        sb.append("  dataSourceConfigs=");
        if (dataSourceConfigs != null && !dataSourceConfigs.isEmpty()) {
            sb.append(dataSourceConfigs.size()).append(" configs [")
              .append(String.join(", ", dataSourceConfigs.keySet())).append("],\n");
        } else {
            sb.append("empty,\n");
        }
        
        // AppRule
        sb.append("  appRule=");
        if (appRule != null && appRule.getShardRules() != null) {
            sb.append(appRule.getShardRules().size()).append(" rules");
        } else {
            sb.append("null");
        }
        sb.append(",\n");
        
        // Deprecated elastic indexes
        sb.append("  deprecatedElasticIndexes=");
        if (deprecatedElasticIndexes != null && !deprecatedElasticIndexes.isEmpty()) {
            sb.append(deprecatedElasticIndexes).append(",\n");
        } else {
            sb.append("empty,\n");
        }
        
        // Elastic configs
        sb.append("  elasticConfigs=")
          .append(elasticConfigs != null ? elasticConfigs.size() + " configs" : "null")
          .append(",\n");
        
        // Atom data sources
        sb.append("  atomDataSources=");
        if (atomDataSources != null && !atomDataSources.isEmpty()) {
            sb.append(atomDataSources.size()).append(" sources [")
              .append(String.join(", ", atomDataSources.keySet())).append("],\n");
        } else {
            sb.append("empty,\n");
        }
        
        // Group data sources
        sb.append("  groupDataSources=");
        if (groupDataSources != null && !groupDataSources.isEmpty()) {
            sb.append(groupDataSources.size()).append(" groups [")
              .append(String.join(", ", groupDataSources.values().stream()
                  .map(ObTableClientGroup::getGroupKey)
                  .toArray(String[]::new))).append("],\n");
        } else {
            sb.append("empty,\n");
        }
        
        // Logical tables
        sb.append("  logicalTables=");
        if (logicalTables != null && !logicalTables.isEmpty()) {
            sb.append(logicalTables.size()).append(" tables [")
              .append(String.join(", ", logicalTables.keySet())).append("]\n");
        } else {
            sb.append("empty\n");
        }
        
        sb.append("}");
        return sb.toString();
    }
}
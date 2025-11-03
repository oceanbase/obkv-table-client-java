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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.dds.group.ObTableClientGroup;
import static com.alipay.oceanbase.rpc.property.Property.RPC_EXECUTE_TIMEOUT;
import com.alipay.oceanbase.rpc.table.ConcurrentTask;
import com.alipay.oceanbase.rpc.table.ConcurrentTaskExecutor;
import com.alipay.sofa.common.thread.SofaThreadPoolExecutor;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.group.AtomDataSourceWeight;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupDataSourceWeight;

/**
 * 数据源工厂类，统一处理原子数据源和客户端组的创建逻辑
 * 用于初始化和配置变更场景
 */
public class DataSourceFactory {

    private static final Logger logger = LoggerFactory.getLogger(DataSourceFactory.class);

    /**
     * 创建原子数据源
     * 
     * @param dataSourceConfigs 数据源配置映射
     * @param runningMode 运行模式
     * @param tableClientProperty 客户端属性配置
     * @return 原子数据源映射
     * @throws Exception 创建失败时抛出异常
     */
    public static Map<String, ObTableClient> createAtomDataSources(
            Map<String, ExtendedDataSourceConfig> dataSourceConfigs,
            ObTableClient.RunningMode runningMode,
            Properties tableClientProperty) throws Exception {

        if (dataSourceConfigs == null || dataSourceConfigs.isEmpty()) {
            return new ConcurrentHashMap<>();
        }

        Map<String, ObTableClient> atomDataSources = new ConcurrentHashMap<>();

        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        ThreadPoolExecutor executorService = new SofaThreadPoolExecutor(
            dataSourceConfigs.size(),
            dataSourceConfigs.size(),
            0L, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            threadFactory
        );

        final ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(
            executorService, dataSourceConfigs.size());

        for (Map.Entry<String, ExtendedDataSourceConfig> configEntry : dataSourceConfigs.entrySet()) {
            executor.execute(new ConcurrentTask() {
                @Override
                public void doTask() {
                    String dbkey = configEntry.getKey();
                    ExtendedDataSourceConfig config = configEntry.getValue();

                    logger.info("Creating AtomDataSource: {}, config: {}", dbkey, config);
                    
                    // 创建 ObTableClient
                    ObTableClient client = new ObTableClient();
                    client.setFullUserName(config.getUsername());
                    client.setPassword(config.getPassword());
                    client.setSysUserName("proxyro");
                    client.setSysPassword("3u^0kCdpE");
                    client.setAppName(config.getAppName());
                    client.setAppDataSourceName(config.getAppDsName());
                    client.setParamURL(config.getJdbcUrl());
                    client.setRunningMode(runningMode);
                    client.setProperties(tableClientProperty);
                    try {
                        logger.info("Initializing AtomDataSource: {}", dbkey);
                        client.init();
                        atomDataSources.put(dbkey, client);
                        logger.info("Successfully created AtomDataSource: {}", dbkey);
                    } catch (Exception e) {
                        logger.error("Failed to create AtomDataSource: {}, config: {}, error: {}", 
                            dbkey, config, e.getMessage(), e);
                        throw new RuntimeException("Failed to create AtomDataSource: " + dbkey, e);
                    }
                }
            });
        }

        executor.waitComplete(RPC_EXECUTE_TIMEOUT.getDefaultLong(), TimeUnit.MILLISECONDS);
        executorService.shutdown();

        return atomDataSources;
    }

    /**
     * 创建客户端组
     * 
     * @param groupClusterConfig 集群配置
     * @param atomDataSources 原子数据源映射
     * @return 客户端组映射
     * @throws Exception 创建失败时抛出异常
     */
    public static Map<Integer, ObTableClientGroup> createGroupDataSources(
            GroupClusterConfig groupClusterConfig,
            Map<String, ObTableClient> atomDataSources) throws Exception {

        if (groupClusterConfig == null || atomDataSources == null) {
            return new ConcurrentHashMap<>();
        }

        Map<Integer, ObTableClientGroup> groupDataSources = new ConcurrentHashMap<>();

        for (GroupDataSourceConfig config : groupClusterConfig.getGroupCluster().values()) {
            String groupKey = config.getGroupKey();
            String weightString = config.getWeightString();
            GroupDataSourceWeight groupDataSourceWeight = config.getGroupDataSourceWeight();

            // 创建原子数据源映射
            Map<String, ObTableClient> atomDataSourceInGroup = new ConcurrentHashMap<>();
            List<AtomDataSourceWeight> weights = groupDataSourceWeight.getDataSourceReadWriteWeights();
            
            if (weights != null) {
                for (AtomDataSourceWeight weight : weights) {
                    String dbkey = weight.getDbkey();
                    logger.info("Processing weight for group '{}': dbkey={}, index={}, readWeight={}, writeWeight={}",
                            groupKey, dbkey, weight.getIndex(), weight.getReadWeight(), weight.getWriteWeight());
                    
                    ObTableClient client = atomDataSources.get(dbkey);
                    if (client != null) {
                        atomDataSourceInGroup.put(dbkey, client);
                        logger.info("Successfully mapped dbkey '{}' to client for group '{}'", dbkey, groupKey);
                    } else {
                        logger.error("CRITICAL: dbkey '{}' required by group '{}' NOT FOUND in atomDataSources. Available: {}",
                                dbkey, groupKey, atomDataSources.keySet());
                        logger.error("This will cause group '{}' to have incomplete atom data sources!", groupKey);
                    }
                }
            }
            
            logger.info("Group '{}' atom data source mapping completed: expected={}, actual={}, missing={}",
                    groupKey, 
                    weights != null ? weights.size() : 0,
                    atomDataSourceInGroup.size(),
                    weights != null ? weights.size() - atomDataSourceInGroup.size() : 0);
            
            if (atomDataSourceInGroup.isEmpty()) {
                logger.error("FATAL: Group '{}' has NO atom data sources! This group will be unusable.", groupKey);
            }

            // 创建客户端组
            ObTableClientGroup obTableClientGroup = new ObTableClientGroup(
                groupKey, weightString, groupDataSourceWeight, atomDataSourceInGroup);
            obTableClientGroup.init();

            groupDataSources.put(obTableClientGroup.getGroupIndex(), obTableClientGroup);
        }

        return groupDataSources;
    }
}

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
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import static com.alipay.oceanbase.rpc.property.Property.RPC_LOGIN_TIMEOUT;
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

    private static final Logger logger          = LoggerFactory.getLogger(DataSourceFactory.class);
    private static final Logger ddsConfigLogger = TableClientLoggerFactory.getDDSConfigLogger();

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
            ddsConfigLogger.info("DDS_DATASOURCE_CREATE," +
                    "configCount=0,status=SKIPPED");
            return new ConcurrentHashMap<>();
        }

        long startTime = System.currentTimeMillis();
        Map<String, ObTableClient> atomDataSources = new ConcurrentHashMap<>();
        int totalConfigs = dataSourceConfigs.size();
        final int[] successCount = {0};
        final int[] failureCount = {0};

        ddsConfigLogger.info("DDS_DATASOURCE_CREATE_START," +
                "configCount={},status=STARTED", totalConfigs);

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

                    ddsConfigLogger.info("DDS_DATASOURCE_INIT," +
                            "dbkey={},appName={},status=STARTED", 
                            dbkey, config.getAppName());
                    
                    try {
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
                        
                        logger.info("Initializing AtomDataSource: {}", dbkey);
                        client.init();
                        atomDataSources.put(dbkey, client);
                        
                        successCount[0]++;
                        ddsConfigLogger.info("DDS_DATASOURCE_INIT_SUCCESS," +
                                "dbkey={},appName={},status=SUCCESS", 
                                dbkey, config.getAppName());
                        
                        logger.info("Successfully created AtomDataSource: {}", dbkey);
                    } catch (Exception e) {
                        failureCount[0]++;
                        ddsConfigLogger.error("DDS_DATASOURCE_INIT_FAILED," +
                                "dbkey={},appName={},errorMessage={},status=FAILED", 
                                dbkey, config.getAppName(), e.getMessage());
                        
                        logger.error("Failed to create AtomDataSource: {}, config: {}, error: {}", 
                            dbkey, config, e.getMessage(), e);
                        ddsConfigLogger.debug("Collecting exception for dbkey: {}", dbkey, e);
                        throw new RuntimeException("Failed to create AtomDataSource: " + dbkey, e);
                    }
                }
            });
        }

        executor.waitComplete(Math.max(RPC_LOGIN_TIMEOUT.getDefaultLong(), 10000L /* 10 seconds */), TimeUnit.MILLISECONDS);
        executorService.shutdown();
        
        // 检查是否有任务执行失败
        if (!executor.getThrowableList().isEmpty()) {
            Throwable firstError = executor.getThrowableList().get(0);
            ddsConfigLogger.error("DDS_DATASOURCE_CREATE_FAILED," +
                    "configCount={},successCount={},failureCount={},errorMessage={},status=FAILED", 
                    totalConfigs, successCount[0], failureCount[0], firstError.getMessage());
            throw new RuntimeException("Failed to create some AtomDataSources. Success: " + successCount[0] + 
                ", Failed: " + failureCount[0], firstError);
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        ddsConfigLogger.info("DDS_DATASOURCE_CREATE_COMPLETE," +
                "configCount={},successCount={},failureCount={},duration={},status=COMPLETED", 
                totalConfigs, successCount[0], failureCount[0], duration);

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
            ddsConfigLogger.info("DDS_GROUP_CREATE," +
                    "groupCount=0,status=SKIPPED");
            return new ConcurrentHashMap<>();
        }

        long startTime = System.currentTimeMillis();
        Map<Integer, ObTableClientGroup> groupDataSources = new ConcurrentHashMap<>();
        int totalGroups = groupClusterConfig.getGroupCluster().size();
        final int[] successGroups = {0};
        final int[] failureGroups = {0};

        ddsConfigLogger.info("DDS_GROUP_CREATE_START," +
                "groupCount={},atomDataSourceCount={},status=STARTED", 
                totalGroups, atomDataSources.size());

        for (GroupDataSourceConfig config : groupClusterConfig.getGroupCluster().values()) {
            String groupKey = config.getGroupKey();
            String weightString = config.getWeightString();
            GroupDataSourceWeight groupDataSourceWeight = config.getGroupDataSourceWeight();

            ddsConfigLogger.info("DDS_GROUP_INIT," +
                    "groupKey={},weightString={},status=STARTED", 
                    groupKey, weightString);
            
            try {
                // 创建原子数据源映射
                Map<String, ObTableClient> atomDataSourceInGroup = new ConcurrentHashMap<>();
                List<AtomDataSourceWeight> weights = groupDataSourceWeight.getDataSourceReadWriteWeights();
                
                if (weights != null) {
                    for (AtomDataSourceWeight weight : weights) {
                        String dbkey = weight.getDbkey();
                        ddsConfigLogger.debug("DDS_GROUP_WEIGHT_PROCESS," +
                                "groupKey={},dbkey={},readWeight={},writeWeight={}", 
                                groupKey, dbkey, weight.getReadWeight(), weight.getWriteWeight());
                        
                        ObTableClient client = atomDataSources.get(dbkey);
                        if (client != null) {
                            atomDataSourceInGroup.put(dbkey, client);
                            ddsConfigLogger.debug("DDS_GROUP_DATASOURCE_MAPPED," +
                                    "groupKey={},dbkey={},status=MAPPED", 
                                    groupKey, dbkey);
                        } else {
                            ddsConfigLogger.error("DDS_GROUP_DATASOURCE_MISSING," +
                                    "groupKey={},dbkey={},status=MISSING", 
                                    groupKey, dbkey);
                            logger.error("CRITICAL: dbkey '{}' required by group '{}' NOT FOUND in atomDataSources. Available: {}",
                                    dbkey, groupKey, atomDataSources.keySet());
                        }
                    }
                }
                
                if (atomDataSourceInGroup.isEmpty()) {
                    failureGroups[0]++;
                    ddsConfigLogger.error("DDS_GROUP_INIT_FAILED," +
                            "groupKey={},status=NO_DATASOURCES", groupKey);
                    logger.error("FATAL: Group '{}' has NO atom data sources! This group will be unusable.", groupKey);
                    continue;
                }

                // 创建客户端组
                ObTableClientGroup obTableClientGroup = new ObTableClientGroup(
                    groupKey, weightString, groupDataSourceWeight, atomDataSourceInGroup);
                obTableClientGroup.init();

                groupDataSources.put(obTableClientGroup.getGroupIndex(), obTableClientGroup);
                successGroups[0]++;
                
                ddsConfigLogger.info("DDS_GROUP_INIT_SUCCESS," +
                        "groupKey={},groupIndex={},atomDataSourceCount={},status=SUCCESS", 
                        groupKey, obTableClientGroup.getGroupIndex(), atomDataSourceInGroup.size());
                
            } catch (Exception e) {
                failureGroups[0]++;
                ddsConfigLogger.error("DDS_GROUP_INIT_FAILED," +
                        "groupKey={},errorMessage={},status=FAILED", 
                        groupKey, e.getMessage());
                logger.error("Failed to create group data source for group: {}", groupKey, e);
            }
        }
        
        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        
        ddsConfigLogger.info("DDS_GROUP_CREATE_COMPLETE," +
                "groupCount={},successGroups={},failureGroups={},duration={},status=COMPLETED", 
                totalGroups, successGroups[0], failureGroups[0], duration);

        return groupDataSources;
    }
}

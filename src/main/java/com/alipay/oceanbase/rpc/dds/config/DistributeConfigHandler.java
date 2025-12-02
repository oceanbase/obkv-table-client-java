/*-
 * #%L
 * * OceanBase Table Client Framework
 * *
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
 * *
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

import java.util.Map;
import java.util.Objects;

import com.alipay.oceanbase.rpc.Lifecycle;
import static com.alipay.oceanbase.rpc.constant.Constants.DDS_LOCAL_CONFIG_PATH_KEY;
import static com.alipay.oceanbase.rpc.constant.Constants.DDS_USE_LOCAL_CONFIG_KEY;
import com.alipay.sofa.dds.config.ExtendedDataSourceConfig;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.rule.AppRule;
import com.alipay.sofa.dds.sdk.DdsSDK;

/**
 * @author zhiqi.zzq
 * @since 2021/7/7 下午8:05
 */
public class DistributeConfigHandler implements Lifecycle {

    private final String                 appName;
    private final String                 appDsName;
    private final String                 version;
    private final DdsConfigUpdateHandler dynamicHandler;
    private final Long                   timeout;

    private DdsSDK                       ddsSDK;

    public DistributeConfigHandler(String appName, String appDsName, String version,
                                   long configFetchOnceTimeoutMillis,
                                   DdsConfigUpdateHandler dynamicHandler) {
        this.appName = appName;
        this.appDsName = appDsName;
        this.version = version;
        this.timeout = configFetchOnceTimeoutMillis;
        this.dynamicHandler = dynamicHandler;
    }

    @Override
    public void init() throws Exception {
        this.ddsSDK = new DdsSDK();
        ddsSDK.setAppName(appName);
        ddsSDK.setAppDataSourceName(appDsName);
        ddsSDK.setVersion(version);
        ddsSDK.setDynamicConfigHandler(dynamicHandler);
        ddsSDK.setConfigFetchOnceTimeoutMillis(this.timeout);

        String useLocalConfig = System.getProperty(DDS_USE_LOCAL_CONFIG_KEY);
        if (Objects.nonNull(useLocalConfig)) {
            ddsSDK.setUseLocalConfigOnly(Boolean.parseBoolean(useLocalConfig));
        } else {
            ddsSDK.setUseLocalConfigOnly(false);
        }

        String localConfigPath = System.getProperty(DDS_LOCAL_CONFIG_PATH_KEY);
        if (Objects.nonNull(localConfigPath)) {
            ddsSDK.setConfigPath(localConfigPath);
        }
        ddsSDK.init();
    }

    @Override
    public void close() throws Exception {

    }

    public Map<String, ExtendedDataSourceConfig> getExtendedDataSourceConfigs() {
        return ddsSDK.getDdsDataSourceConfig().getExtendedDataSourceConfigs();
    }

    public GroupClusterConfig getGroupClusterConfig() {
        return ddsSDK.getDdsDataSourceConfig().getGroupClusterConfig();
    }

    public AppRule getAppRule() {
        return ddsSDK.getDdsDataSourceConfig().getAppRule();
    }

    public String getAppName() {
        return appName;
    }

    public String getAppDsName() {
        return appDsName;
    }

    public String getVersion() {
        return version;
    }

}

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

package com.alipay.oceanbase.rpc.bolt.transport;

import com.alipay.remoting.*;
import com.alipay.remoting.config.ConfigManager;
import com.alipay.remoting.config.ConfigurableInstance;
import com.alipay.remoting.config.configs.ConfigContainer;
import com.alipay.remoting.config.configs.ConfigItem;
import com.alipay.remoting.config.configs.ConfigType;
import com.alipay.remoting.config.configs.DefaultConfigContainer;
import com.alipay.remoting.config.switches.GlobalSwitch;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcResponseFuture;
import com.alipay.remoting.rpc.protocol.UserProcessor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ObConfigurableInstance implements ConfigurableInstance {
    private ConfigContainer configContainer = new DefaultConfigContainer();
    private GlobalSwitch    globalSwitch    = new GlobalSwitch();
    private ConfigType      configType;

    /*
     * Ob configurable instance.
     */
    public ObConfigurableInstance() {
        this.configType = ConfigType.CLIENT_SIDE;
    }

    @Override
    public ConfigContainer conf() {
        return this.configContainer;
    }

    @Override
    public GlobalSwitch switches() {
        return this.globalSwitch;
    }

    @Override
    public void initWriteBufferWaterMark(int low, int high) {
        this.configContainer.set(configType, ConfigItem.NETTY_BUFFER_LOW_WATER_MARK, low);
        this.configContainer.set(configType, ConfigItem.NETTY_BUFFER_HIGH_WATER_MARK, high);
    }

    @Override
    public int netty_buffer_low_watermark() {
        if (null != configContainer
            && configContainer.contains(configType, ConfigItem.NETTY_BUFFER_LOW_WATER_MARK)) {
            return (Integer) configContainer
                .get(configType, ConfigItem.NETTY_BUFFER_LOW_WATER_MARK);
        } else {
            return ConfigManager.netty_buffer_low_watermark();
        }
    }

    @Override
    public int netty_buffer_high_watermark() {
        if (null != configContainer
            && configContainer.contains(configType, ConfigItem.NETTY_BUFFER_HIGH_WATER_MARK)) {
            return (Integer) configContainer.get(configType,
                ConfigItem.NETTY_BUFFER_HIGH_WATER_MARK);
        } else {
            return ConfigManager.netty_buffer_high_watermark();
        }
    }
}

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

import com.alipay.remoting.config.ConfigurableInstance;
import com.alipay.remoting.config.configs.ConfigContainer;
import com.alipay.remoting.config.configs.ConfigType;
import com.alipay.remoting.config.switches.GlobalSwitch;

public class ObConfigurableInstance implements ConfigurableInstance {
    private int lowWatermark  = 32 * 1024; // 32KB
    private int highWatermark = 64 * 1024; // 64KB

    /*
     * Ob configurable instance.
     */
    public ObConfigurableInstance() {
        // 在bolt 1.6.5中，ConfigurableInstance是一个接口
    }

    @Override
    public ConfigContainer conf() {
        // 返回默认配置容器
        return null;
    }

    @Override
    public GlobalSwitch switches() {
        // 返回全局开关
        return null;
    }

    @Override
    public void initWriteBufferWaterMark(int low, int high) {
        this.lowWatermark = low;
        this.highWatermark = high;
    }

    @Override
    public int netty_buffer_low_watermark() {
        return lowWatermark;
    }

    @Override
    public int netty_buffer_high_watermark() {
        return highWatermark;
    }
}

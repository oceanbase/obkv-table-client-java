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

import com.alipay.sofa.dds.config.AttributesConfig;
import com.alipay.sofa.dds.config.advanced.DoubleWriteRule;
import com.alipay.sofa.dds.config.advanced.TestLoadRule;
import com.alipay.sofa.dds.config.dynamic.DynamicConfigHandler;
import com.alipay.sofa.dds.config.group.GroupClusterConfig;
import com.alipay.sofa.dds.config.group.GroupClusterDbkeyConfig;

import java.util.Map;

/**
* @author zhiqi.zzq
* @since 2021/7/7 下午8:35
*/
public class DistributeDynamicHandler implements DynamicConfigHandler {
    /**
    *
    * @param groupCluster
    */
    @Override
    public void resetGroupCluster(GroupClusterConfig groupCluster) {

    }

    /**
    *
    * @param groupClusterDbkey
    * @return
    */
    @Override
    public boolean resetGroupClusterDbkey(GroupClusterDbkeyConfig groupClusterDbkey) {
        return false;
    }

    /**
    *
    * @param attributes
    */
    @Override
    public void resetAttributes(AttributesConfig attributes) {

    }

    /**
    *
    * @param doubleWriteRules
    */
    @Override
    public void resetDoubleWriteRules(Map<String, DoubleWriteRule> doubleWriteRules) {

    }

    /**
    *
    * @param whiteListRules
    */
    @Override
    public void resetWhiteListRules(Map<String, String> whiteListRules) {

    }

    /**
    *
    * @param selfAdjustRules
    */
    @Override
    public void resetSelfAdjustRules(Map<String, String> selfAdjustRules) {

    }

    /**
    *
    * @param testLoadRule
    */
    @Override
    public void resetTestLoadRule(TestLoadRule testLoadRule) {

    }
}

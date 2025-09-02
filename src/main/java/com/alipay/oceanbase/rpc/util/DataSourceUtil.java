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

package com.alipay.oceanbase.rpc.util;

import com.alibaba.fastjson.JSON;
import com.alipay.data.conf.client.ConfKeeperClient;
import com.alipay.data.conf.core.Configuration;

import java.util.Map;

/**
* @author hongwei.yhw
* @since 2018-May-08
*/
public class DataSourceUtil {

    private ConfKeeperClient client;

    /**
    * Data source util.
    */
    public DataSourceUtil() {
        client = new ConfKeeperClient();
        client.properties().setAppName("obtable");
        client.init();
    }

    public Map<String, String> fetchDataSourceInfo(String dataSourceName) {
        Configuration conf = client.locate("com.alipay.zdal.config.dbKey." + dataSourceName);
        return (Map<String, String>) JSON.parse(conf.getContent());
    }

}

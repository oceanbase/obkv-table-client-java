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

package com.alipay.oceanbase.rpc.util;

import com.alipay.oceanbase.rpc.ObTableClient;

public class ObTableClientTestUtil {
    public static String FULL_USER_NAME          = "root@mysql#ob12.shenyunlong.syl.11.158.77.144";
    public static String PARAM_URL               = "http://ocp-cfg.alibaba.net:8080/services?User_ID=alibaba&UID=test&Action=ObRootServiceInfo&ObCluster=ob12.shenyunlong.syl.11.158.77.144&database=test";
    public static String PASSWORD                = "";
    public static String PROXY_SYS_USER_NAME     = "root";
    public static String PROXY_SYS_USER_PASSWORD = "";

    public static ObTableClient newTestClient() throws Exception {
        ObTableClient obTableClient = new ObTableClient();
        // for observer directly mode
//        obTableClient.setFullUserName(FULL_USER_NAME);
//        obTableClient.setParamURL(PARAM_URL);
//        obTableClient.setPassword(PASSWORD);
//        obTableClient.setSysUserName(PROXY_SYS_USER_NAME);
//        obTableClient.setSysPassword(PROXY_SYS_USER_PASSWORD);

        // baigui, for test key and range partition
        obTableClient.setOdpAddr("100.88.147.30");
        obTableClient.setOdpPort(2885);
        obTableClient.setFullUserName("test@mysql#ob96.heshi.zhs");
        obTableClient.setPassword("test");
        obTableClient.setOdpMode(true);
        obTableClient.setDatabase("test");

        // zhiyun hash partition
//        obTableClient.setOdpAddr("100.88.147.119");
//        obTableClient.setOdpPort(38887);
//        obTableClient.setFullUserName("test@mysql#ob96.heshi.zhs");
//        obTableClient.setPassword("test");
//        obTableClient.setOdpMode(true);
//        obTableClient.setDatabase("test");
        return obTableClient;
    }
}

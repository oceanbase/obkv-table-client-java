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
    public static String FULL_USER_NAME              = "full-user-name";
    public static String PARAM_URL                   = "config-url";
    public static String PASSWORD                    = "password";
    public static String PROXY_SYS_USER_NAME         = "sys-user-name";
    public static String PROXY_SYS_USER_PASSWORD = "sys-user-password";

    public static ObTableClient newTestClient() throws Exception {
        ObTableClient obTableClient = new ObTableClient();
        obTableClient.setFullUserName(FULL_USER_NAME);
        obTableClient.setParamURL(PARAM_URL);
        obTableClient.setPassword(PASSWORD);
        obTableClient.setSysUserName(PROXY_SYS_USER_NAME);
        obTableClient.setSysPassword(PROXY_SYS_USER_PASSWORD);
        return obTableClient;
    }
}

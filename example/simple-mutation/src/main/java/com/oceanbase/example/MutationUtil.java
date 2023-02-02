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

package com.oceanbase.example;

import com.alipay.oceanbase.rpc.ObTableClient;

import java.util.Map;
import java.util.List;

/* table schema:
CREATE TABLE IF NOT EXISTS `kv_table` (
    `key` varchar(20) NOT NULL,
    `val` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`key`)
);
*/

public class MutationUtil {
    private ObTableClient obTableClient = null;
    private String tableName = "kv_table";

    public boolean initial() {
        try {
            obTableClient = new ObTableClient();
            obTableClient.setFullUserName("your user name"); // e.g. root@sys#ocp
            obTableClient.setParamURL("your configurl + database=xxx"); // e.g. http://ip:port/services?Action=ObRootServiceInfo&ObRegion=ocp&database=test
            obTableClient.setPassword("your user passwd");
            obTableClient.setSysUserName("your sys user"); // e.g. proxyro@sys
            obTableClient.setSysPassword("your sys user passwd");
            obTableClient.init();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("fail to init OBKV client, " + e);
            obTableClient = null;
            return false;
        }
        System.out.println("initial OBKV client success");
        return true;
    }

    public void close() {
        if (obTableClient == null) {
            return;
        }
        try {
            obTableClient.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("fail to close OBKV client, " + e);
        }
    }

    public ObTableClient getClient() {
        return this.obTableClient;
    }
}

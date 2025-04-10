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

package com.alipay.oceanbase.rpc.location.model;

import com.alipay.oceanbase.rpc.table.*;

import java.util.Map;
import java.util.Properties;

public class OdpInfo {
    private String  addr    = "127.0.0.1";
    private int     port    = 2883;
    private ObTable obTable = null;

    public OdpInfo(String addr, int port) {
        this.addr = addr;
        this.port = port;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void buildOdpTable(String tenantName, String fullUserName, String password,
                              String database, ObTableClientType clientType, Properties properties,
                              Map<String, Object> tableConfigs) throws Exception {
        this.obTable = new ObTable.Builder(addr, port)
            .setLoginInfo(tenantName, fullUserName, password, database, clientType)
            .setProperties(properties).setConfigs(tableConfigs).build();
    }

    public ObTable getObTable() {
        return this.obTable;
    }
}

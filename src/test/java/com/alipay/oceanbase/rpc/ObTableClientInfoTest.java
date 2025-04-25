/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

package com.alipay.oceanbase.rpc;

import com.alibaba.fastjson.JSON;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

import static com.alipay.oceanbase.rpc.property.Property.RUNTIME_RETRY_TIMES;

public class ObTableClientInfoTest {
    public ObTableClient[] clients;
    private int            connCnt            = 10;
    private int            clientCnt          = 10;
    private Long           connMaxExpiredTime = 1L;
    String                 tableName          = "test_varchar_table";

    /**
     CREATE TABLE `test_varchar_table` (
     `c1` varchar(20) NOT NULL,
     `c2` varchar(20) DEFAULT NULL,
     PRIMARY KEY (`c1`)
     );
     **/

    @Before
    public void setup() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");
        clients = new ObTableClient[clientCnt];
        for (int i = 0; i < clientCnt; i++) {
            clients[i] = ObTableClientTestUtil.newTestClient();
            clients[i].addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(),
                Integer.toString(connCnt));
            clients[i].addProperty(Property.RUNTIME_RETRY_TIMES.getKey(), Integer.toString(i + 3));
            clients[i].addProperty(Property.MAX_CONN_EXPIRED_TIME.getKey(),
                Long.toString(connMaxExpiredTime));
            clients[i].init();
        }
    }

    @Test
    public void testConnection() throws Exception {
        String tenantName = ObTableClientTestUtil.getTenantName(); // mysql
        String userName = ObTableClientTestUtil.getUserName(); // root
        for (int i = 0; i < clientCnt; i++) {
            doGet(clients[i]);
        }
        // check result of GV$OB_KV_CLIENT_INFO in mysql tenant: mysql can only get itself
        Connection mysql_conn = ObTableClientTestUtil.getConnection();
        checkGvClientInfo(mysql_conn, tenantName, userName, false /* is_update */);

        // check result of GV$OB_KV_CLIENT_INFO in sys tenant: sys tenant can get all tenants result
        Connection sys_conn = ObTableClientTestUtil.getSysConnection();
        checkGvClientInfo(sys_conn, tenantName, userName, false /* is_update */);

        System.out.println("wait to reconnect and login...");
        Thread.sleep(90000); // 90s

        // last_login_ts will be refreshed
        checkGvClientInfo(mysql_conn, tenantName, userName, true /* is_update */);
        checkGvClientInfo(sys_conn, tenantName, userName, true /* is_update */);
    }

    private String genClientIdStr() {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < clientCnt - 1; i++) {
            sb.append(clients[i].getClientId() + ",");
        }
        sb.append(clients[clientCnt - 1].getClientId() + ")");
        return sb.toString();
    }

    private void checkGvClientInfo(Connection conn, String tenantName, String userName,
                                   boolean is_update) throws Exception {
        if (conn == null) {
            throw new NullPointerException();
        }
        Statement statement = conn.createStatement();
        String SQL = "select a.client_id AS CLIENT_ID, b.tenant_name as TENANT_NAME, a.user_name AS USER_NAME, a.client_info AS CLIENT_INFO, "
                     + "a.first_login_ts AS FIRST_LOGIN_TS, a.last_login_ts AS LAST_LOGIN_TS "
                     + "from oceanbase.GV$OB_KV_CLIENT_INFO a inner join oceanbase.DBA_OB_TENANTS b on a.tenant_id = b.tenant_id "
                     + "where a.client_id in " + genClientIdStr();
        statement.execute(SQL);
        ResultSet resultSet = statement.getResultSet();

        int resCount = 0;
        Map<Long, String> resultMap = new HashMap<Long, String>();
        while (resultSet.next()) {
            resCount++;
            resultMap.put(resultSet.getLong("CLIENT_ID"), resultSet.getString("CLIENT_INFO"));
            Assert.assertEquals(userName, resultSet.getString("USER_NAME"));
            Assert.assertEquals(tenantName, resultSet.getString("TENANT_NAME"));
            Timestamp first_ts = resultSet.getTimestamp("FIRST_LOGIN_TS");
            Timestamp last_ts = resultSet.getTimestamp("LAST_LOGIN_TS");
            if (is_update) {
                Assert.assertTrue(first_ts.before(last_ts));
            } else {
                Assert.assertTrue(first_ts.equals(last_ts));
            }
        }
        Assert.assertEquals(clientCnt, resCount);

        // check json str if is right
        for (int i = 0; i < clientCnt; i++) {
            String json_config_str = resultMap.get(clients[i].getClientId());
            Assert.assertTrue(json_config_str != null);
            Map<String, Object> config_map = JSON.parseObject(json_config_str);
            Long srcClientId = (Long) clients[i].getTableConfigs().get("client_id");
            Long dstClientId = (Long) config_map.get("client_id");
            Assert.assertEquals(srcClientId, dstClientId);

            // sample check another object result : RUNTIME_RETRY_TIMES
            Map<String, String> srcRouteMap = (Map<String, String>) clients[i].getTableConfigs()
                .get("runtime");
            Map<String, String> dstRouteMap = (Map<String, String>) config_map.get("runtime");
            Assert.assertEquals(srcRouteMap.get(RUNTIME_RETRY_TIMES.getKey()),
                dstRouteMap.get(RUNTIME_RETRY_TIMES.getKey()));
        }
    }

    private void doGet(ObTableClient client) {
        try {
            client.get(tableName, new String[] { "k1" }, new String[] { "c1" });
        } catch (Exception e) {
            Assert.assertTrue(true); //  table is not exist
        }
    }

}

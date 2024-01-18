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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import static com.alipay.oceanbase.rpc.ObGlobal.OB_VERSION;
import static com.alipay.oceanbase.rpc.ObGlobal.calcVersion;

public class ObTableClientTestUtil {
    public static String  FULL_USER_NAME          = "full-user-name";
    public static String  PARAM_URL               = "config-url";
    public static String  PASSWORD                = "password";
    public static String  PROXY_SYS_USER_NAME     = "sys-user-name";
    public static String  PROXY_SYS_USER_PASSWORD = "sys-user-password";

    public static boolean USE_ODP                 = false;
    public static String  ODP_IP                  = "ip-addr";
    public static int     ODP_PORT                = 0;
    public static String  ODP_DATABASE            = "database-name";

    public static String  JDBC_URL                = "jdbc-url";

    public static ObTableClient newTestClient() throws Exception {
        ObTableClient obTableClient = new ObTableClient();
        if (!USE_ODP) {
            obTableClient.setFullUserName(FULL_USER_NAME);
            obTableClient.setParamURL(PARAM_URL);
            obTableClient.setPassword(PASSWORD);
            obTableClient.setSysUserName(PROXY_SYS_USER_NAME);
            obTableClient.setSysPassword(PROXY_SYS_USER_PASSWORD);
        } else {
            obTableClient.setOdpMode(true);
            obTableClient.setFullUserName(FULL_USER_NAME);
            obTableClient.setOdpAddr(ODP_IP);
            obTableClient.setOdpPort(ODP_PORT);
            obTableClient.setDatabase(ODP_DATABASE);
            obTableClient.setPassword(PASSWORD);
        }

        return obTableClient;
    }

    public static Connection getConnection() throws SQLException {
        String[] userNames = FULL_USER_NAME.split("#");
        return DriverManager.getConnection(JDBC_URL, userNames[0], PASSWORD);
    }

    public static void cleanTable(String tableName) throws Exception {
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("delete from " + tableName);
    }

    public static String generateRandomStringByUUID(int times) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < times; i++) {
            sb.append(UUID.randomUUID().toString().replaceAll("-", ""));
        }
        return sb.toString();
    }

    public static boolean isOBVersionGreaterEqualThan(long targetVersion) {
        return OB_VERSION >= targetVersion;
    }

    public static boolean isOBVersionGreaterThan(long targetVersion) {
        return OB_VERSION >= targetVersion;
    }

    public static boolean isOBVersionLessEqualThan(long targetVersion) {
        return OB_VERSION <= targetVersion;
    }

    public static boolean isOBVersionLessThan(long targetVersion) {
        return OB_VERSION <= targetVersion;
    }

    public static long obVsn4000 = calcVersion(4, (short) 0, (byte) 0, (byte) 0);

    static {
        System.setProperty("logging.path", System.getProperty("user.dir") + "/logs");
    }
}

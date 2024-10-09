/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableFilterList;
import com.alipay.oceanbase.rpc.filter.ObTableValueFilter;
import com.alipay.oceanbase.rpc.mutation.Append;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Insert;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import static com.alipay.oceanbase.rpc.filter.ObTableFilterFactory.andList;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.*;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ObTableModeTest {
    ObTableClient        client;
    public static String tableName        = "test_varchar_table";
    public static String mysqlCompatMode  = "mysql";
    public static String oracleCompatMode = "oracle";

    public static String allKVMode        = "ALL";
    public static String tableKVMode      = "TABLEAPI";
    public static String hbaseKVMode      = "HBASE";
    public static String RedisKVMode      = "REDIS";
    public static String noneKVMode       = "NONE";

    public static String tpUnit           = "tpUnit";
    public static String tpPool           = "tpPool";
    public static String tpTenant         = "tpTenant";

    public static String hbaseUnit        = "hbaseUnit";
    public static String hbasePool        = "hbasePool";
    public static String hbaseTenant      = "hbaseTenant";

    public static String tableUnit        = "tableUnit";
    public static String tablePool        = "tablePool";
    public static String tableTenant      = "tableTenant";

    public static String redisUnit        = "redisUnit";
    public static String redisPool        = "redisPool";
    public static String redisTenant      = "redisTenant";

    @Before
    public void setup() throws Exception {
    }

    public static String extractUserName(String input) {
        int atSymbolIndex = input.indexOf("@");
        if (atSymbolIndex != -1) {
            return input.substring(0, atSymbolIndex);
        } else {
            return "";
        }
    }

    public static String extractClusterName(String input) {
        int hashSymbolIndex = input.lastIndexOf("#");
        if (hashSymbolIndex != -1) {
            return input.substring(hashSymbolIndex + 1);
        } else {
            return "";
        }
    }

    public void createTable(String userName, String tenantName) throws Exception {
        String user = userName + "@" + tenantName;
        String url = "jdbc:mysql://" + JDBC_IP + ":" + JDBC_PORT + "/ " + "test" + "?"
                     + "rewriteBatchedStatements=TRUE&" + "allowMultiQueries=TRUE&"
                     + "useLocalSessionState=TRUE&" + "useUnicode=TRUE&"
                     + "characterEncoding=utf-8&" + "socketTimeout=3000000&"
                     + "connectTimeout=60000";
        Connection conn = DriverManager.getConnection(url, user, PASSWORD);
        Statement statement = conn.createStatement();
        statement.execute("CREATE TABLE IF NOT EXISTS `test_varchar_table` ("
                          + "     `c1` varchar(20) NOT NULL,"
                          + "     `c2` varchar(20) DEFAULT NULL," + "     PRIMARY KEY (`c1`)"
                          + "     );");
    }

    public void createResourceUnit(String unitName) throws Exception {
        Connection conn = ObTableClientTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("create resource unit " + unitName + " max_cpu 1, memory_size '1G';");
    }

    public void createResourcePool(String unitName, String poolName) throws Exception {
        createResourceUnit(unitName);
        Connection conn = ObTableClientTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("create resource pool " + poolName + " unit = '" + unitName
                          + "', unit_num = 1;");
    }

    public void createTenant(String unitName, String poolName, String tenantName,
                             String compatMode, String kvMode) throws Exception {
        createResourcePool(unitName, poolName);
        Connection conn = ObTableClientTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("create tenant " + tenantName + " replica_num = 1, resource_pool_list=('"
                          + poolName + "') "
                          + "set ob_tcp_invited_nodes='%', ob_compatibility_mode='" + compatMode
                          + "', ob_kv_mode='" + kvMode + "';");
    }

    public void dropResourceUnit(String unitName) throws Exception {
        Connection conn = ObTableClientTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("drop resource unit if exists " + unitName + ";");
    }

    public void dropResourcePool(String poolName) throws Exception {
        Connection conn = ObTableClientTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("drop resource pool if exists " + poolName + ";");
    }

    public void dropTenant(String tenantName) throws Exception {
        Connection conn = ObTableClientTestUtil.getSysConnection();
        Statement statement = conn.createStatement();
        statement.execute("drop tenant if exists " + tenantName + " force;");
    }

    /**
     CREATE TABLE IF NOT EXISTS `test_varchar_table` (
     `c1` varchar(20) NOT NULL,
     `c2` varchar(20) DEFAULT NULL,
     PRIMARY KEY (`c1`)
     );
     **/
    @Test
    public void testTpTenant() throws Exception {
        try {
            createTenant(tpUnit, tpPool, tpTenant, mysqlCompatMode, noneKVMode);

            String tenantName = tpTenant;
            String userName = extractUserName(FULL_USER_NAME);
            String clusterName = extractClusterName(FULL_USER_NAME);
            String fullName = userName + "@" + tenantName + "#" + clusterName;

            createTable(userName, tenantName);

            ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
            obTableClient.setFullUserName(fullName);
            obTableClient.init();
            this.client = obTableClient;
            client.addRowKeyElement(tableName, new String[] { "c1" });

            ObTableException thrown = assertThrows(
                    ObTableException.class,
                    () -> {
                        client.insert(tableName).setRowKey(colVal("c1", "a"))
                                .addMutateColVal(colVal("c2", "a"))
                                .execute();
                    }
            );
            System.out.println(thrown.getMessage());
            assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][As the ob_kv_mode variable has been set to 'NONE', your current interfaces not supported]"));
        } finally {
            dropTenant(tpTenant);
            dropResourcePool(tpPool);
            dropResourceUnit(tpUnit);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS `test_varchar_table` (
     `c1` varchar(20) NOT NULL,
     `c2` varchar(20) DEFAULT NULL,
     PRIMARY KEY (`c1`)
     );
     **/
    @Test
    public void testHbaseTenant() throws Exception {
        try {
            String tenantName = hbaseTenant;
            createTenant(hbaseUnit, hbasePool, tenantName, mysqlCompatMode, hbaseKVMode);

            String userName = extractUserName(FULL_USER_NAME);
            String clusterName = extractClusterName(FULL_USER_NAME);
            String fullName = userName + "@" + tenantName + "#" + clusterName;

            createTable(userName, tenantName);

            ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
            obTableClient.setFullUserName(fullName);
            obTableClient.init();
            this.client = obTableClient;
            client.addRowKeyElement(tableName, new String[] { "c1" });

            ObTableException thrown = assertThrows(
                    ObTableException.class,
                    () -> {
                        client.insert(tableName).setRowKey(colVal("c1", "a"))
                                .addMutateColVal(colVal("c2", "a"))
                                .execute();
                    }
            );
            System.out.println(thrown.getMessage());
            assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][As the ob_kv_mode variable has been set to 'HBASE', your current interfaces not supported]"));
        } finally {
            dropTenant(hbaseTenant);
            dropResourcePool(hbasePool);
            dropResourceUnit(hbaseUnit);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS `test_varchar_table` (
     `c1` varchar(20) NOT NULL,
     `c2` varchar(20) DEFAULT NULL,
     PRIMARY KEY (`c1`)
     );
     **/
    @Test
    public void testTableTenant() throws Exception {
        try {
            String tenantName = tableTenant;
            createTenant(tableUnit, tablePool, tenantName, mysqlCompatMode, tableKVMode);

            String userName = extractUserName(FULL_USER_NAME);
            String clusterName = extractClusterName(FULL_USER_NAME);
            String fullName = userName + "@" + tenantName + "#" + clusterName;

            createTable(userName, tenantName);

            ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
            obTableClient.setFullUserName(fullName);
            obTableClient.init();
            this.client = obTableClient;
            client.addRowKeyElement(tableName, new String[] { "c1" });

            client.insert(tableName).setRowKey(colVal("c1", "a"))
                .addMutateColVal(colVal("c2", "a")).execute();
        } finally {
            dropTenant(tableTenant);
            dropResourcePool(tablePool);
            dropResourceUnit(tableUnit);
        }
    }

    /**
     CREATE TABLE IF NOT EXISTS `test_varchar_table` (
     `c1` varchar(20) NOT NULL,
     `c2` varchar(20) DEFAULT NULL,
     PRIMARY KEY (`c1`)
     );
     **/
    @Test
    public void testRedisTenant() throws Exception {
        try {
            String tenantName = redisTenant;
            createTenant(redisUnit, redisPool, tenantName, mysqlCompatMode, RedisKVMode);

            String userName = extractUserName(FULL_USER_NAME);
            String clusterName = extractClusterName(FULL_USER_NAME);
            String fullName = userName + "@" + tenantName + "#" + clusterName;

            createTable(userName, tenantName);

            ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
            obTableClient.setFullUserName(fullName);
            obTableClient.init();
            this.client = obTableClient;
            client.addRowKeyElement(tableName, new String[] { "c1" });

            ObTableException thrown = assertThrows(
                    ObTableException.class,
                    () -> {
                        client.insert(tableName).setRowKey(colVal("c1", "a"))
                                .addMutateColVal(colVal("c2", "a"))
                                .execute();
                    }
            );
            System.out.println(thrown.getMessage());
            assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][As the ob_kv_mode variable has been set to 'REDIS', your current interfaces not supported]"));
        } finally {
            dropTenant(redisTenant);
            dropResourcePool(redisPool);
            dropResourceUnit(redisUnit);
        }
    }
}

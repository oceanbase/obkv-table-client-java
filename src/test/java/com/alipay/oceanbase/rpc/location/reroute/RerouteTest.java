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
package com.alipay.oceanbase.rpc.location.reroute;

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.location.reroute.util.ReplicaOperation;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.*;

public class RerouteTest {

    public ObTableClient moveClient;
    public ObTableClient clientNotReroute;
    private static final boolean passReroutingTest = false;
    private static final String tenantName = "";
    private static final String databaseName = "";
    private static final int partNum = 2;
    private static final String testInt32RerouteTableName = "test_int32_reroute";
    private static final String testInt32RerouteCreateStatement = "create table if not exists `test_int32_reroute`(`c1` int(12) not null,`c2` int(12) default null,primary key (`c1`)) partition by hash(c1) partitions 2;";

    private ReplicaOperation replicaOperation;
    @Before
    public void setup() throws Exception {

        moveClient = ObTableClientTestUtil.newTestClient();
        moveClient.addProperty(Property.SERVER_ENABLE_REROUTING.getKey(), "true");
        moveClient.init();

        clientNotReroute = ObTableClientTestUtil.newTestClient();
        clientNotReroute.addProperty(Property.SERVER_ENABLE_REROUTING.getKey(), "false");
        clientNotReroute.init();
        Connection connection = getConnection();
        PreparedStatement statement = connection.prepareStatement(testInt32RerouteCreateStatement);
        statement.execute();
        replicaOperation = new ReplicaOperation();
    }

    private Connection getConnection() throws SQLException {
        String[] userNames = FULL_USER_NAME.split("#");
        return DriverManager.getConnection("jdbc:mysql://" + JDBC_IP + ":" + JDBC_PORT
                +  "/" + databaseName + "?"
                + "rewriteBatchedStatements=TRUE&"
                + "allowMultiQueries=TRUE&"
                + "useLocalSessionState=TRUE&"
                + "useUnicode=TRUE&"
                + "characterEncoding=utf-8&"
                + "socketTimeout=3000000&"
                + "connectTimeout=60000", userNames[0], PASSWORD);
    }

    public static void setReroutingEnable(boolean enable) throws SQLException {
        String sql = "alter system set _obkv_feature_mode='rerouting=%s'";
        String targetSql;
        if (enable) {
            targetSql = String.format(sql, "on");
        } else {
            targetSql = String.format(sql, "off");
        }
        PreparedStatement ps = getSysConnection().prepareStatement(targetSql);
        ps.execute();
        ps.close();
    }
    @Test
    public void testMoveReplicaSingleOp() throws Exception {
        try {
            if (passReroutingTest) {
                System.out.println("Please run Rerouting tests manually!!!");
                System.out.println("Change passReroutingTest to false in src/test/java/com/alipay/oceanbase/rpc/location/model/RerouteTest.java to run rerouting tests.");
                Assert.assertFalse(passReroutingTest);
            }
            setReroutingEnable(true);
            moveClient.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});
            long affectRow = moveClient.insert(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(1, affectRow);

            Map<String, Object> result = moveClient.get(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"});

            Assert.assertEquals(0, result.get("c2"));
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }

            Thread.sleep(5000);

            result = moveClient.get(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"});

            Assert.assertEquals(0, result.get("c2"));
        } finally {
            moveClient.delete(testInt32RerouteTableName, new Object[] {0});
        }
    }

    @Test
    public void testMoveReplicaSingleInsertUp() throws Exception {
        try {
            if (passReroutingTest) {
                System.out.println("Please run Rerouting tests manually!!!");
                System.out.println("Change passReroutingTest to false in src/test/java/com/alipay/oceanbase/rpc/location/model/RerouteTest.java to run rerouting tests.");
                Assert.assertFalse(passReroutingTest);
            }
            setReroutingEnable(true);
            moveClient.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});

            long affectRow = moveClient.insert(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(affectRow, 1);
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);

            affectRow = moveClient.insertOrUpdate(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(affectRow, 1);
        } finally {
            moveClient.delete(testInt32RerouteTableName, new Object[] {0});
        }
    }

    @Test
    public void testMoveReplicaSingleBatch() throws Exception {
        try {
            if (passReroutingTest) {
                System.out.println("Please run Rerouting tests manually!!!");
                System.out.println("Change passReroutingTest to false in src/test/java/com/alipay/oceanbase/rpc/location/model/RerouteTest.java to run rerouting tests.");
                Assert.assertFalse(passReroutingTest);
            }
            setReroutingEnable(true);
            moveClient.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});

            long affectRow = moveClient.insert(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(affectRow, 1);
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);

            TableBatchOps tableBatchOps = moveClient.batch(testInt32RerouteTableName);
            tableBatchOps.get(new Object[] {0}, new String[] {"c1", "c2"});
            List<Object> batchResult = tableBatchOps.execute();
            Assert.assertEquals(batchResult.size(), 1);
            Assert.assertEquals(((HashMap<String, Integer>) batchResult.get(0)).get("c1").intValue(), 0);
            Assert.assertEquals(((HashMap<String, Integer>) batchResult.get(0)).get("c2").intValue(), 0);
        } finally {
            moveClient.delete(testInt32RerouteTableName, new Object[] {0});
        }
    }

    @Test
    public void testMoveReplicaSingleBatchInsertUp() throws Exception {
        try {
            if (passReroutingTest) {
                System.out.println("Please run Rerouting tests manually!!!");
                System.out.println("Change passReroutingTest to false in src/test/java/com/alipay/oceanbase/rpc/location/model/RerouteTest.java to run rerouting tests.");
                Assert.assertFalse(passReroutingTest);
            }
            setReroutingEnable(true);
            moveClient.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});

            long affectRow = moveClient.insert(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(affectRow, 1);
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);

            TableBatchOps tableBatchOps = moveClient.batch(testInt32RerouteTableName);
            tableBatchOps.insertOrUpdate(new Object[] {0}, new String[] {"c2"}, new Object[] {5});
            List<Object> result = tableBatchOps.execute();

            Assert.assertEquals(1, result.size());
            Assert.assertEquals((long)1, result.get(0));
        } finally {
            moveClient.delete(testInt32RerouteTableName, new Object[] {0});
        }
    }

    @Test
    public void testMoveReplicaQuery() throws Exception {
        try {
            if (passReroutingTest) {
                System.out.println("Please run Rerouting tests manually!!!");
                System.out.println("Change passReroutingTest to false in src/test/java/com/alipay/oceanbase/rpc/location/model/RerouteTest.java to run rerouting tests.");
                Assert.assertFalse(passReroutingTest);
            }
            setReroutingEnable(true);
            moveClient.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});

            long affectRow = moveClient.insert(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(affectRow, 1);
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);

            QueryResultSet result = moveClient.query(testInt32RerouteTableName).select("c1", "c2").addScanRange(new Object[] {0}, new Object[] {0}).execute();

            while (result.next()) {
                Assert.assertEquals(0, result.getRow().get("c1"));
                Assert.assertEquals(0, result.getRow().get("c2"));
            }
        } finally {
            moveClient.delete(testInt32RerouteTableName, new Object[] {0});
        }
    }

    @Test
    public void testMoveReplicaQueryAndMutate() throws Exception {
        try {
            if (passReroutingTest) {
                System.out.println("Please run Rerouting tests manually!!!");
                System.out.println("Change passReroutingTest to false in src/test/java/com/alipay/oceanbase/rpc/location/model/RerouteTest.java to run rerouting tests.");
                Assert.assertFalse(passReroutingTest);
            }
            setReroutingEnable(true);
            moveClient.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});

            long affectRow = moveClient.insert(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(affectRow, 1);
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);

            affectRow = moveClient.update(testInt32RerouteTableName, new Object[]{0}, new String[] {"c2"}, new Object[] {5});
            Assert.assertEquals(1, affectRow);
        } finally {
            moveClient.delete(testInt32RerouteTableName, new Object[] {0});
        }
    }

    // 在服务端设置不允许重路由
    @Test
    public void testMoveReplicaServerReroutingOff() throws Exception {
        try {
            if (passReroutingTest) {
                System.out.println("Please run Rerouting tests manually!!!");
                System.out.println("Change passReroutingTest to false in src/test/java/com/alipay/oceanbase/rpc/location/model/RerouteTest.java to run rerouting tests.");
                Assert.assertFalse(passReroutingTest);
            }
            setReroutingEnable(false);
            moveClient.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});

            long affectRow = moveClient.insert(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(affectRow, 1);
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);

            // single get
            Map<String, Object> rs= moveClient.get(testInt32RerouteTableName, new Object[] {0}, new String[] {"c2"});
            Assert.assertEquals(0, rs.get("c2"));
            // multi get
            TableBatchOps tableBatchOps = moveClient.batch(testInt32RerouteTableName);
            tableBatchOps.get(new Object[] {0}, new String[] {"c2"});
            List<Object> res =  tableBatchOps.execute();
            Assert.assertEquals(1, res.size());
            Assert.assertEquals(0, ((Map<String, Integer>)res.get(0)).get("c2").intValue());

            // 所有操作前都进行切主，为了防止前边刷新路由表之后，后边对切主无感。
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);
            // query
            TableQuery tableQuery = moveClient.query(testInt32RerouteTableName).select("c1", "c2").addScanRange(new Object[] {0}, new Object[] {0});
            QueryResultSet queryResultSet = tableQuery.execute();
            while (queryResultSet.next()) {
                Assert.assertEquals(0, queryResultSet.getRow().get("c1"));
                Assert.assertEquals(0, queryResultSet.getRow().get("c2"));
            }

            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);
            // update
            affectRow = moveClient.update(testInt32RerouteTableName, new Object[]{0}, new String[] {"c2"}, new Object[] {5});
            Assert.assertEquals(1, affectRow);
        } finally {
            setReroutingEnable(true);
            Thread.sleep(1000);
            moveClient.delete(testInt32RerouteTableName, new Object[] {0});
        }
    }

    // 在客户端设置不允许重路由
    // 即使不允许重路由，现阶段的修改还是保留了原有的同步等待刷新表的流程，所以下边的操作不会抛异常，只是时间会很久
    @Test
    public void testMoveReplicaClientReroutingOff() throws Exception {
        try {
            if (passReroutingTest) {
                System.out.println("Please run Rerouting tests manually!!!");
                System.out.println("Change passReroutingTest to false in src/test/java/com/alipay/oceanbase/rpc/location/model/RerouteTest.java to run rerouting tests.");
                Assert.assertFalse(passReroutingTest);
            }
            setReroutingEnable(true);
            clientNotReroute.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});

            long affectRow = clientNotReroute.insert(testInt32RerouteTableName, new Object[]{0}, new String[]{"c2"}, new Object[]{0});
            Assert.assertEquals(affectRow, 1);
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);

            // single get
            Map<String, Object> rs= clientNotReroute.get(testInt32RerouteTableName, new Object[] {0}, new String[] {"c2"});
            Assert.assertEquals(0, rs.get("c2"));
            // multi get
            TableBatchOps tableBatchOps = clientNotReroute.batch(testInt32RerouteTableName);
            tableBatchOps.get(new Object[] {0}, new String[] {"c2"});
            List<Object> res =  tableBatchOps.execute();
            Assert.assertEquals(1, res.size());
            Assert.assertEquals(0, ((Map<String, Integer>)res.get(0)).get("c2").intValue());

            // 所有操作前都进行切主，为了防止前边刷新路由表之后，后边对切主无感。
            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);
            // query
            TableQuery tableQuery = clientNotReroute.query(testInt32RerouteTableName).select("c1", "c2").addScanRange(new Object[] {0}, new Object[] {0});
            QueryResultSet queryResultSet = tableQuery.execute();
            while (queryResultSet.next()) {
                Assert.assertEquals(0, queryResultSet.getRow().get("c1"));
                Assert.assertEquals(0, queryResultSet.getRow().get("c2"));
            }

            if (ObGlobal.OB_VERSION < 4) {
                replicaOperation.switchReplicaLeaderRandomly(tenantName, databaseName, testInt32RerouteTableName, partNum);
            } else {
                replicaOperation.switchReplicaLeaderRandomly4x(tenantName, databaseName, testInt32RerouteTableName);
            }
            Thread.sleep(5000);
            // update
            affectRow = clientNotReroute.update(testInt32RerouteTableName, new Object[]{0}, new String[] {"c2"}, new Object[] {5});
            Assert.assertEquals(1, affectRow);
        } finally {
            moveClient.addRowKeyElement(testInt32RerouteTableName, new String[]{"c1"});
            moveClient.delete(testInt32RerouteTableName, new Object[] {0});
        }
    }
}
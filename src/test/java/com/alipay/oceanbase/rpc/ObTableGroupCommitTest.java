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

import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static org.junit.Assert.assertEquals;

public class ObTableGroupCommitTest {
    public ObTableClient client;

    @Before
    public void setup() throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();

        this.client = obTableClient;
    }

    /**
     CREATE TABLE IF NOT EXISTS `test_varchar_table` (
     `c1` varchar(20) NOT NULL,
     `c2` varchar(20) DEFAULT NULL,
     PRIMARY KEY (`c1`)
     );
     **/
    @Test
    public void test_group_commit() throws Exception {
        Assume.assumeTrue("support after ob version 4.2.5", ObGlobal.OB_VERSION >= ObGlobal.calcVersion(4,(short)2,(byte)5,(byte)0));
        String tableName = "test_varchar_table";
        long executeTime = 10000; // 10s
        int threadCnt = 100;
        ArrayList<Thread> workers = new ArrayList<>();
        deleteTable(tableName);
        switchGroupCommit(true);
        for (int i = 0; i < threadCnt; i++) {
            InsertWorker worker = new InsertWorker(i, tableName, this.client, executeTime);
            workers.add(worker);
        }

        for (int i = 0; i < threadCnt; i++) {
            GetWorker worker = new GetWorker(i, tableName, this.client, executeTime);
            workers.add(worker);
        }

        for (int i = 0; i < 2*threadCnt; i++) {
            workers.get(i).start();
        }

        for (int i = 0; i < 2*threadCnt; i++) {
            workers.get(i).join();
        }
        checkSystemView();
        deleteTable(tableName);
        switchGroupCommit(false);
        this.client.close();
    }

    private void checkSystemView() throws SQLException {
        // mysql tenant
        Connection mysql_conn = ObTableClientTestUtil.getConnection();
        Statement statement = mysql_conn.createStatement();
        statement.execute("select b.tenant_id as tenant_id, b.tenant_name as tenant_name, a.group_type as group_type, a.batch_size as batch_size " +
                " from oceanbase.GV$OB_KV_GROUP_COMMIT_STATUS a inner join " +
                "oceanbase.DBA_OB_TENANTS b on a.tenant_id = b.tenant_id group by a.group_type");
        ResultSet resultSet = statement.getResultSet();
        int resCount = 0;
        System.out.println("visit by mysql tenant:");
        while (resultSet.next()) {
            long tenant_id = resultSet.getLong("tenant_id");
            String tenant_name = resultSet.getString("tenant_name");
            String group_type = resultSet.getString("group_type");
            long batch_size = resultSet.getLong("batch_size");
            System.out.println("tenant_id:" + tenant_id+", tenant_name: "+ tenant_name +", group_type: "+group_type+", batch_size: "+batch_size);
            resCount++;
        }
        Assert.assertTrue(resCount >= 3);
        mysql_conn.close();

        // sys tenant
        Connection sys_conn = ObTableClientTestUtil.getSysConnection();
        Statement statement2 = sys_conn.createStatement();
        statement2.execute("select b.tenant_id as tenant_id, b.tenant_name as tenant_name, a.group_type as group_type, a.batch_size as batch_size " +
                " from oceanbase.GV$OB_KV_GROUP_COMMIT_STATUS a inner join " +
                "oceanbase.__all_tenant b on a.tenant_id = b.tenant_id where b.tenant_name in ('sys', '"+ObTableClientTestUtil.getTenantName()+"') group by b.tenant_name, a.group_type;");
        resultSet = statement2.getResultSet();
        resCount = 0;
        System.out.println("visit by sys tenant:");
        while (resultSet.next()) {
            long tenant_id = resultSet.getLong("tenant_id");
            String tenant_name = resultSet.getString("tenant_name");
            String group_type = resultSet.getString("group_type");
            long batch_size = resultSet.getLong("batch_size");
            System.out.println("tenant_id:" + tenant_id+", tenant_name: "+ tenant_name +", group_type: "+group_type+", batch_size: "+batch_size);
            resCount++;
        }
        Assert.assertTrue(resCount >= 4);
        sys_conn.close();
    }

    class InsertWorker extends Thread {
        private int           id;
        private String        tableName;
        private ObTableClient obTableClient;
        private long          executeTime;  // in millisecond

        public InsertWorker(int id, String tableName, ObTableClient obTableClient, long executeTime) {
            this.id = id;
            this.tableName = tableName;
            this.obTableClient = obTableClient;
            this.executeTime = executeTime;
        }

        public void run() {
            long start = System.currentTimeMillis();
            int counter = 0;
            while ((System.currentTimeMillis() - start) < executeTime) {
                try {
                    String c1 = String.format("rk_%d_%d", id, counter);
                    String c2 = String.format("col_%d_%d", id, counter);
                    obTableClient.insert(tableName).setRowKey(row(colVal("c1", c1)))
                            .addMutateRow(row(colVal("c2",c2))).execute();
                    counter++;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("thread " + id + " get occurs exception !");
                }
            }
        }
    }

    class GetWorker extends Thread {
        private int           id;
        private String        tableName;
        private ObTableClient obTableClient;
        private long          executeTime;  // in millisecond

        public GetWorker(int id, String tableName, ObTableClient obTableClient, long executeTime) {
            this.id = id;
            this.tableName = tableName;
            this.obTableClient = obTableClient;
            this.executeTime = executeTime;
        }

        public void run() {
            long start = System.currentTimeMillis();
            int counter = 0;
            while ((System.currentTimeMillis() - start) < executeTime) {
                try {
                    String c1 = String.format("rk_%d_%d", id, counter);
                    obTableClient.get(tableName, new String[] { "c1" }, new String[] { c1 });
                    counter++;
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("thread " + id + " get occurs exception !");
                }
            }
        }
    }

    private void switchGroupCommit(boolean is_enable) throws SQLException {
        int batch_size = is_enable ? 10 : 1;
        Connection mysql_conn = ObTableClientTestUtil.getConnection();
        Statement statement = mysql_conn.createStatement();
        statement.execute("alter system set kv_group_commit_batch_size = "+ batch_size);
    }

    private void deleteTable(String tableName) throws SQLException {
        Connection mysql_conn = ObTableClientTestUtil.getConnection();
        Statement statement = mysql_conn.createStatement();
        statement.execute("delete from "+ tableName);
    }
}

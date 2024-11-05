/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.location.reroute.util;

import com.alipay.oceanbase.rpc.location.model.ObServerRole;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ReplicaOperation {
    // tenant 跟 tenant_id修改为实际的参数

    private static final String getReplicaSql      = "SELECT A.table_id as table_id, A.partition_id as partition_id, A.svr_ip as svr_ip, B.svr_port as svr_port, A.role as role FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port WHERE tenant_name = ? and database_name = ? and table_name = ? and partition_id in ";
    private static final String getLDIdSql         = "SELECT ls_id FROM oceanbase.cdb_ob_table_locations WHERE tenant_id = 1004 and database_name = ? and table_name = ? and role = 'LEADER';";
    private static final String getFollowerAddrSql = "SELECT concat(svr_ip,':',svr_port) AS host FROM oceanbase.__all_virtual_ls_meta_table WHERE tenant_id = 1004 and ls_id = ? and role = 2 and replica_status = 'NORMAL' limit 1;";
    private static final String switch2FollwerSql  = "ALTER SYSTEM SWITCH REPLICA LEADER ls = ? server= ? tenant='java_client'";

    private Connection          connection;

    public ReplicaOperation() throws SQLException {
        connection = ObTableClientTestUtil.getSysConnection();
    }

    public String createInStatement(int partNum) {
        long[] values = new long[partNum];
        for (int i = 0; i < partNum; i++) {
            values[i] = i;
        }

        // Create inStatement "(0,1,2...partNum);".
        StringBuilder inStatement = new StringBuilder();
        inStatement.append("(");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                inStatement.append(", ");
            }
            inStatement.append(values[i]);
        }
        inStatement.append(");");
        return inStatement.toString();
    }

    public void getPartitions(String tenantName, String databaseName, String tableName,
                              int partNum, List<Partition> partitions) throws SQLException {
        String sql = getReplicaSql + createInStatement(partNum);
        Connection connection = ObTableClientTestUtil.getConnection();
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setString(1, tenantName);
        ps.setString(2, databaseName);
        ps.setString(3, tableName);
        ResultSet rs = ps.executeQuery();

        long tableId;
        long partId;
        String ip;
        int port;
        int role;

        boolean isEmpty = true;
        while (rs.next()) {
            if (isEmpty) {
                isEmpty = false;
            }
            tableId = rs.getLong("TABLE_ID");
            partId = rs.getLong("PART_ID");
            ip = rs.getString("IP");
            port = rs.getInt("PORT");
            role = rs.getInt("ROLE");

            Replica replica = new Replica(tableId, partId, ip, port, ObServerRole.getRole(role));

            if (replica.getRole() == ObServerRole.LEADER) {
                for (Partition partition : partitions) {
                    if (partition.getPartId() == partId) {
                        partition.setTableId(tableId);
                        partition.setLeader(replica);
                        partition.addReplicaNum(1);
                    }
                }
            } else {
                for (Partition partition : partitions) {
                    if (partition.getPartId() == partId) {
                        partition.appendFollower(replica);
                        partition.addReplicaNum(1);
                    }
                }
            }
        }
        if (isEmpty) {
            throw new RuntimeException("empty set when get partitions");
        }
    }

    public void disableAutoReplicaSwitch() throws SQLException {
        String sql = "alter system set enable_auto_leader_switch = 'false';";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.executeQuery();
        ps.close();
    }

    public void switchLeader(List<Partition> partitions) throws SQLException {
        int partNum = partitions.size();
        for (Partition partition : partitions) {
            if (partition.getFollower().size() != 2) {
                throw new RuntimeException("invalid follower num: "
                                           + partition.getFollower().size());
            }
        }

        // get a follower ip and port
        String ip = partitions.get(0).getFollower().get(0).ip;
        int port = partitions.get(0).getFollower().get(0).port;
        String server = ip + ":" + port;

        // switch all replica leader to ip:port
        for (Partition partition : partitions) {
            String partIdStr = String.format("%d%%%d@%d", partition.getPartId(), partNum,
                partition.getTableId());
            String sql = String.format(
                "ALTER SYSTEM SWITCH REPLICA LEADER PARTITION_ID '%s' SERVER '%s';", partIdStr,
                server);
            PreparedStatement ps = connection.prepareStatement(sql);
            ps.executeQuery();
        }
    }

    public void switchReplicaLeaderRandomly(String tenantName, String databaseName, String tableName, int partNum) {
        if (partNum != 2) {
            String errMsg = "invalid partition num:" + partNum;
            throw new RuntimeException(errMsg);
        }

        try {
            // 1.disable auto replica switch
            disableAutoReplicaSwitch();

            // 2.query replica
            List<Partition> partitions = new ArrayList<>(partNum);
            for (int i = 0; i < partNum; ++i) {
                partitions.add(new Partition(i));
            }
            getPartitions(tenantName, databaseName, tableName, partNum, partitions);

            // 3.check replica
            if (partitions.size() != partNum) {
                String errMsg = "invalid partition num:" + partitions.size();
                throw new RuntimeException(errMsg);
            }

            // 4 switch leader
            switchLeader(partitions);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public long getLsId(String databaseName, String tableName) throws SQLException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(getLDIdSql);
            ps.setString(1, databaseName);
            ps.setString(2, tableName);
            rs = ps.executeQuery();

            long lsId = -1, prevLsId;
            prevLsId = Long.MAX_VALUE;
            boolean isEmpty = true;
            while (rs.next()) {
                if (isEmpty) {
                    isEmpty = false;
                }
                lsId = rs.getLong("LS_ID");

                if (prevLsId == Long.MAX_VALUE) {
                    prevLsId = lsId;
                } else {
                    if (lsId != prevLsId) {
                        return -1;
                    }
                }
            }

            if (isEmpty) {
                return -1;
            }
            return lsId;
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
                if (null != ps) {
                    ps.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public String getFollowerAddr(long lsId) {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(getFollowerAddrSql);
            ps.setString(1, String.valueOf(lsId));
            rs = ps.executeQuery();
            String addr = "";
            boolean isEmpty = true;

            while (rs.next()) {
                if (isEmpty) {
                    isEmpty = false;
                }
                addr = rs.getString("host");
            }
            if (isEmpty) {
                throw new RuntimeException("empty set when get follower addr");
            }
            return addr;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
                if (null != ps) {
                    ps.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private void switch2Follower(long lsId, String addr) {
        PreparedStatement ps = null;
        try {
            ps = connection.prepareStatement(switch2FollwerSql);
            ps.setLong(1, lsId);
            ps.setString(2, addr);
            ps.execute();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (null != ps) {
                    ps.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    public void switchReplicaLeaderRandomly4x(String tenantName, String databaseName,
                                              String tableName) throws SQLException {
        long lsId = getLsId(databaseName, tableName);
        String addr = getFollowerAddr(lsId);
        switch2Follower(lsId, addr);
    }
}

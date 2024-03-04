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

package com.alipay.oceanbase.rpc.location;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.ParserConfig;
import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.constant.Constants;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.model.*;
import com.alipay.oceanbase.rpc.location.model.partition.*;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObSimpleColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObIndexType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnExpressParser;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.text.MessageFormat;
import java.util.*;

import static com.alipay.oceanbase.rpc.location.model.partition.ObPartitionKey.MAX_PARTITION_ELEMENT;
import static com.alipay.oceanbase.rpc.location.model.partition.ObPartitionKey.MIN_PARTITION_ELEMENT;
import static com.alipay.oceanbase.rpc.util.RandomUtil.getRandomNum;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;
import static java.lang.String.format;
import static com.alipay.oceanbase.rpc.protocol.payload.Constants.INVALID_LS_ID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LocationUtil {

    private static final Logger logger                           = TableClientLoggerFactory
                                                                     .getLogger(LocationUtil.class);
    static {
        ParserConfig.getGlobalInstance().setSafeMode(true);
    }

    private static final String OB_VERSION_SQL                   = "SELECT /*+READ_CONSISTENCY(WEAK)*/ OB_VERSION() AS CLUSTER_VERSION;";

    private static final String PROXY_INDEX_INFO_SQL             = "SELECT /*+READ_CONSISTENCY(WEAK)*/ data_table_id, table_id, index_type FROM oceanbase.__all_virtual_table "
                                                                   + "where table_name = ?";

    private static final String PROXY_TABLE_ID_SQL               = "SELECT /*+READ_CONSISTENCY(WEAK)*/ table_id from oceanbase.__all_virtual_proxy_schema "
                                                                   + "where tenant_name = ? and database_name = ? and table_name = ? limit 1";

    private static final String OB_TENANT_EXIST_SQL              = "SELECT /*+READ_CONSISTENCY(WEAK)*/ tenant_id from __all_tenant where tenant_name = ?;";

    @Deprecated
    @SuppressWarnings("unused")
    private static final String PROXY_PLAIN_SCHEMA_SQL_FORMAT    = "SELECT /*+READ_CONSISTENCY(WEAK)*/ partition_id, svr_ip, sql_port, table_id, role, part_num, replica_num, schema_version, spare1 "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema "
                                                                   + "WHERE tenant_name = ? AND database_name = ?  AND table_name = ? AND partition_id in ({0}) AND sql_port > 0 "
                                                                   + "ORDER BY role ASC LIMIT ?";

    private static final String PROXY_PART_INFO_SQL              = "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_level, part_num, part_type, part_space, part_expr, "
                                                                   + "part_range_type, part_interval_bin, interval_start_bin, "
                                                                   + "sub_part_num, sub_part_type, sub_part_space, "
                                                                   + "sub_part_range_type, def_sub_part_interval_bin, def_sub_interval_start_bin, sub_part_expr, "
                                                                   + "part_key_name, part_key_type, part_key_idx, part_key_extra, spare1 "
                                                                   + "FROM oceanbase.__all_virtual_proxy_partition_info "
                                                                   + "WHERE table_id = ? group by part_key_name order by part_key_name LIMIT ?;";
    @Deprecated
    @SuppressWarnings("unused")
    private static final String PROXY_TENANT_SCHEMA_SQL          = "SELECT /*+READ_CONSISTENCY(WEAK)*/ svr_ip, sql_port, table_id, role, part_num, replica_num, spare1 "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema "
                                                                   + "WHERE tenant_name = ? AND database_name = ?  AND table_name = ? AND sql_port > 0 "
                                                                   + "ORDER BY partition_id ASC, role ASC LIMIT ?";

    private static final String PROXY_DUMMY_LOCATION_SQL         = "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port, "
                                                                   + "A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port, B.status as status, B.stop_time as stop_time "
                                                                   + ", A.spare1 as replica_type "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
                                                                   + "WHERE tenant_name = ? and database_name=? and table_name = ?";

    private static final String PROXY_LOCATION_SQL               = "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port, "
                                                                   + "A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port, B.status as status, B.stop_time as stop_time "
                                                                   + ", A.spare1 as replica_type "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
                                                                   + "WHERE tenant_name = ? and database_name=? and table_name = ? and partition_id = 0";

    private static final String PROXY_LOCATION_SQL_PARTITION     = "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.partition_id as partition_id, A.svr_ip as svr_ip, A.sql_port as sql_port, "
                                                                   + "A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port, B.status as status, B.stop_time as stop_time "
                                                                   + ", A.spare1 as replica_type "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
                                                                   + "WHERE tenant_name = ? and database_name=? and table_name = ? and partition_id in ({0})";

    private static final String PROXY_FIRST_PARTITION_SQL        = "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, high_bound_val "
                                                                   + "FROM oceanbase.__all_virtual_proxy_partition "
                                                                   + "WHERE table_id = ? LIMIT ?;";

    private static final String PROXY_SUB_PARTITION_SQL          = "SELECT /*+READ_CONSISTENCY(WEAK)*/ sub_part_id, part_name, high_bound_val "
                                                                   + "FROM oceanbase.__all_virtual_proxy_sub_partition "
                                                                   + "WHERE table_id = ? LIMIT ?;";

    private static final String PROXY_SERVER_STATUS_INFO         = "SELECT ss.svr_ip, ss.zone, zs.region, zs.spare4 as idc "
                                                                   + "FROM oceanbase.__all_virtual_proxy_server_stat ss, oceanbase.__all_virtual_zone_stat zs "
                                                                   + "WHERE zs.zone = ss.zone ;";

    @Deprecated
    @SuppressWarnings("unused")
    private static final String PROXY_PLAIN_SCHEMA_SQL_FORMAT_V4 = "SELECT /*+READ_CONSISTENCY(WEAK)*/ tablet_id, svr_ip, sql_port, table_id, role, part_num, replica_num, schema_version, spare1 "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema "
                                                                   + "WHERE tenant_name = ? AND database_name = ?  AND table_name = ? AND tablet_id in ({0}) AND sql_port > 0 "
                                                                   + "ORDER BY role ASC LIMIT ?";

    private static final String PROXY_PART_INFO_SQL_V4           = "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_level, part_num, part_type, part_space, part_expr, "
                                                                   + "part_range_type, sub_part_num, sub_part_type, sub_part_space, sub_part_range_type, sub_part_expr, "
                                                                   + "part_key_name, part_key_type, part_key_idx, part_key_extra, part_key_collation_type "
                                                                   + "FROM oceanbase.__all_virtual_proxy_partition_info "
                                                                   + "WHERE tenant_name = ? and table_id = ? group by part_key_name order by part_key_name LIMIT ?;";
    @Deprecated
    @SuppressWarnings("unused")
    private static final String PROXY_TENANT_SCHEMA_SQL_V4       = "SELECT /*+READ_CONSISTENCY(WEAK)*/ svr_ip, sql_port, table_id, role, part_num, replica_num, spare1 "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema "
                                                                   + "WHERE tenant_name = ? AND database_name = ?  AND table_name = ? AND sql_port > 0 "
                                                                   + "ORDER BY tablet_id ASC, role ASC LIMIT ?";

    private static final String PROXY_DUMMY_LOCATION_SQL_V4      = "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.tablet_id as tablet_id, A.svr_ip as svr_ip, A.sql_port as sql_port, "
                                                                   + "A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port, B.status as status, B.stop_time as stop_time "
                                                                   + ", A.spare1 as replica_type "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
                                                                   + "WHERE tenant_name = ? and database_name=? and table_name = ?";

    private static final String PROXY_LOCATION_SQL_V4            = "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.tablet_id as tablet_id, A.svr_ip as svr_ip, A.sql_port as sql_port, "
                                                                   + "A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port, B.status as status, B.stop_time as stop_time "
                                                                   + ", A.spare1 as replica_type "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
                                                                   + "WHERE tenant_name = ? and database_name=? and table_name = ? and tablet_id = 0";

    private static final String PROXY_LOCATION_SQL_PARTITION_V4  = "SELECT /*+READ_CONSISTENCY(WEAK)*/ A.tablet_id as tablet_id, A.svr_ip as svr_ip, A.sql_port as sql_port, "
                                                                   + "A.table_id as table_id, A.role as role, A.replica_num as replica_num, A.part_num as part_num, B.svr_port as svr_port, B.status as status, B.stop_time as stop_time "
                                                                   + ", A.spare1 as replica_type, D.ls_id as ls_id "
                                                                   + "FROM oceanbase.__all_virtual_proxy_schema A inner join oceanbase.__all_server B on A.svr_ip = B.svr_ip and A.sql_port = B.inner_port "
                                                                   + "inner join oceanbase.DBA_OB_TENANTS C on C.tenant_name = A.tenant_name "
                                                                   + "left join oceanbase.CDB_OB_TABLET_TO_LS D on D.tenant_id = C.tenant_id and D.tablet_id = A.tablet_id "
                                                                   + "WHERE C.tenant_name = ? and database_name= ? and table_name = ? and A.tablet_id in ({0}) ";

    private static final String PROXY_FIRST_PARTITION_SQL_V4     = "SELECT /*+READ_CONSISTENCY(WEAK)*/ part_id, part_name, tablet_id, high_bound_val, sub_part_num "
                                                                   + "FROM oceanbase.__all_virtual_proxy_partition "
                                                                   + "WHERE tenant_name = ? and table_id = ? LIMIT ?;";

    private static final String PROXY_SUB_PARTITION_SQL_V4       = "SELECT /*+READ_CONSISTENCY(WEAK)*/ sub_part_id, part_name, tablet_id, high_bound_val "
                                                                   + "FROM oceanbase.__all_virtual_proxy_sub_partition "
                                                                   + "WHERE tenant_name = ? and table_id = ? LIMIT ?;";

    private static final String PROXY_SERVER_STATUS_INFO_V4      = "SELECT ss.svr_ip, ss.zone, zs.region, zs.idc as idc "
                                                                   + "FROM DBA_OB_SERVERS ss, DBA_OB_ZONES zs "
                                                                   + "WHERE zs.zone = ss.zone ;";

    private static final String home                             = System.getProperty("user.home",
                                                                     "/home/admin");

    private static final int    TEMPLATE_PART_ID                 = -1;

    private abstract static class TableEntryRefreshWithPriorityCallback<T> {
        abstract T execute(ObServerAddr obServerAddr) throws ObTableEntryRefreshException;
    }

    private abstract static class TableEntryRefreshCallback<T> {
        abstract T execute(Connection connection) throws ObTableEntryRefreshException;
    }

    private static ObServerAddr randomObServers(List<ObServerAddr> obServerAddrs) {
        return obServerAddrs.get(getRandomNum(0, obServerAddrs.size()));
    }

    private static TableEntry callTableEntryRefreshWithPriority(ServerRoster serverRoster,
                                                                long priorityTimeout,
                                                                long cachingTimeout,
                                                                TableEntryRefreshWithPriorityCallback<TableEntry> callable)
                                                                                                                           throws ObTableEntryRefreshException {
        ObServerAddr addr = serverRoster.getServer(priorityTimeout, cachingTimeout);
        try {
            TableEntry tableEntry = callable.execute(addr);
            serverRoster.resetPriority(addr);
            return tableEntry;
        } catch (ObTableEntryRefreshException e) {
            RUNTIME.error("callTableEntryRefreshWithPriority meet exception", e);
            serverRoster.downgradePriority(addr);
            throw e;
        } catch (Throwable t) {
            RUNTIME.error("callTableEntryRefreshWithPriority meet exception", t);
            throw t;
        }
    }

    /*
     * Get Server LDC info from OB system table.
     */
    public static List<ObServerLdcItem> getServerLdc(ServerRoster serverRoster,
                                                     final long connectTimeout,
                                                     final long socketTimeout,
                                                     final long priorityTimeout,
                                                     final long cachingTimeout,
                                                     final ObUserAuth sysUA)
                                                                            throws ObTableEntryRefreshException {
        ObServerAddr addr = serverRoster.getServer(priorityTimeout, cachingTimeout);
        try {
            List<ObServerLdcItem> ss = callServerLdcRefresh(addr, connectTimeout, socketTimeout,
                sysUA);
            serverRoster.resetPriority(addr);
            return ss;
        } catch (ObTableEntryRefreshException e) {
            RUNTIME.error("getServerLdc meet exception", e);
            serverRoster.downgradePriority(addr);
            throw e;
        } catch (Throwable t) {
            RUNTIME.error("callTableEntryRefreshWithPriority meet exception", t);
            throw t;
        }
    }

    /*
     * Format ob server url with the tenant server.
     *
     * @param obServerAddr
     * @param connectTimeout
     * @param socketTimeout
     * @return
     */
    private static String formatObServerUrl(ObServerAddr obServerAddr, long connectTimeout,
                                            long socketTimeout) {
        return format(
            "jdbc:mysql://%s/oceanbase?useUnicode=true&characterEncoding=utf-8&connectTimeout=%d&socketTimeout=%d",
            obServerAddr.getIp() + ":" + obServerAddr.getSqlPort(), connectTimeout, socketTimeout);
    }

    /*
     * Establish db connection to the given database URL, with proxy user/password.
     *
     * @param url
     * @return
     * @throws ObTableEntryRefreshException
     */
    private static Connection getMetaRefreshConnection(String url, ObUserAuth sysUA)
                                                                                    throws ObTableEntryRefreshException {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            RUNTIME.error(LCD.convert("01-00006"), e.getMessage(), e);
            throw new ObTableEntryRefreshException(format(
                "fail to find com.mysql.cj.jdbc.Driver, errMsg=%s", e.getMessage()), e);
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00005"), e.getMessage(), e);
            throw new ObTableEntryRefreshException("fail to decode proxyro password", e);
        }

        try {
            return DriverManager.getConnection(url, sysUA.getUserName(), sysUA.getPassword());
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00005"), e.getMessage(), e);
            throw new ObTableEntryRefreshException("fail to connect meta server", e);
        }
    }

    /*
     * Refresh server LDC info.
     *
     * @param obServerAddr
     * @param connectTimeout
     * @param socketTimeout
     * @return List<ObServerLdcItem>
     * @throws ObTableEntryRefreshException
     */
    private static List<ObServerLdcItem> callServerLdcRefresh(ObServerAddr obServerAddr,
                                                              long connectTimeout,
                                                              long socketTimeout, ObUserAuth sysUA)
                                                                                                   throws ObTableEntryRefreshException {
        String url = formatObServerUrl(obServerAddr, connectTimeout, socketTimeout);
        List<ObServerLdcItem> ss = new ArrayList<ObServerLdcItem>();
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            connection = getMetaRefreshConnection(url, sysUA);
            if (ObGlobal.obVsnMajor() >= 4) {
                ps = connection.prepareStatement(PROXY_SERVER_STATUS_INFO_V4);
            } else {
                ps = connection.prepareStatement(PROXY_SERVER_STATUS_INFO);
            }
            rs = ps.executeQuery();
            while (rs.next()) {
                String ip = rs.getString("svr_ip");
                String zone = rs.getString("zone");
                String idc = rs.getString("idc");
                String region = rs.getString("region");
                ss.add(new ObServerLdcItem(ip, zone, idc, region));
            }
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00027"), url, e);
            throw new ObTableEntryRefreshException(format(
                "fail to refresh server LDC from remote url=%s", url), e);
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
                if (null != ps) {
                    ps.close();
                }
                if (null != connection) {
                    connection.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }
        return ss;

    }

    private static TableEntry callTableEntryRefresh(ObServerAddr obServerAddr, TableEntryKey key,
                                                    long connectTimeout, long socketTimeout,
                                                    ObUserAuth sysUA, boolean initialized,
                                                    TableEntryRefreshCallback<TableEntry> callback)
                                                                                                   throws ObTableEntryRefreshException {
        String url = formatObServerUrl(obServerAddr, connectTimeout, socketTimeout);
        Connection connection = null;
        TableEntry entry;
        try {
            connection = getMetaRefreshConnection(url, sysUA);
            entry = callback.execute(connection);
        } catch (ObTableNotExistException e) {
            // avoid to refresh meta for ObTableNotExistException
            RUNTIME.error("callTableEntryRefresh meet exception", e);
            throw e;
        } catch (Exception e) {
            if (!initialized) {
                BOOT.error(LCD.convert("01-00007"), url, key, e);
            } else {
                RUNTIME.error(LCD.convert("01-00007"), url, key, e);
            }
            throw new ObTableEntryRefreshException(format(
                "fail to refresh table entry from remote url=%s, key=%s", url, key), e);
        } finally {
            try {
                if (null != connection) {
                    connection.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }

        if (entry != null && entry.isValid()) {
            entry.setRefreshTimeMills(System.currentTimeMillis());
            return entry;
        } else {
            if (!initialized) {
                BOOT.error(LCD.convert("01-00008"), obServerAddr, key, entry);
            } else {
                RUNTIME.error(LCD.convert("01-00008"), obServerAddr, key, entry);
                RUNTIME.error("table entry is invalid");
            }
            throw new ObTableEntryRefreshException("table entry is invalid, addr = " + obServerAddr
                                                   + " key =" + key + " entry =" + entry);
        }

    }

    /*
     * Load table entry with priority.
     */
    public static TableEntry loadTableEntryWithPriority(final ServerRoster serverRoster,
                                                        final TableEntryKey key,
                                                        final long connectTimeout,
                                                        final long socketTimeout,
                                                        final long priorityTimeout,
                                                        final long cachingTimeout,
                                                        final ObUserAuth sysUA)
                                                                               throws ObTableEntryRefreshException {
        return callTableEntryRefreshWithPriority(serverRoster, priorityTimeout, cachingTimeout,
            new TableEntryRefreshWithPriorityCallback<TableEntry>() {
                @Override
                TableEntry execute(ObServerAddr obServerAddr) throws ObTableEntryRefreshException {
                    return callTableEntryRefresh(obServerAddr, key, connectTimeout, socketTimeout,
                        sysUA, true, new TableEntryRefreshCallback<TableEntry>() {
                            @Override
                            TableEntry execute(Connection connection)
                                                                     throws ObTableEntryRefreshException {
                                return getTableEntryFromRemote(connection, key, true);
                            }
                        });
                }
            });
    }

    /*
     * Load table entry location with priority.
     */
    public static TableEntry loadTableEntryLocationWithPriority(final ServerRoster serverRoster,
                                                                final TableEntryKey key,
                                                                final TableEntry tableEntry,
                                                                final long connectTimeout,
                                                                final long socketTimeout,
                                                                final long priorityTimeout,
                                                                final long cachingTimeout,
                                                                final ObUserAuth sysUA)
                                                                                       throws ObTableEntryRefreshException {

        return callTableEntryRefreshWithPriority(serverRoster, priorityTimeout, cachingTimeout,
            new TableEntryRefreshWithPriorityCallback<TableEntry>() {
                @Override
                TableEntry execute(ObServerAddr obServerAddr) throws ObTableEntryRefreshException {
                    return callTableEntryRefresh(obServerAddr, key, connectTimeout, socketTimeout,
                        sysUA, true, new TableEntryRefreshCallback<TableEntry>() {
                            @Override
                            TableEntry execute(Connection connection)
                                                                     throws ObTablePartitionLocationRefreshException {
                                return getTableEntryLocationFromRemote(connection, key, tableEntry);
                            }
                        });
                }
            });
    }

    /*
     * Load table entry randomly.
     */
    public static TableEntry loadTableEntryRandomly(final List<ObServerAddr> rsList,//
                                                    final TableEntryKey key, //
                                                    final long connectTimeout,//
                                                    final long socketTimeout,
                                                    final ObUserAuth sysUA,
                                                    final boolean initialized)
                                                                              throws ObTableEntryRefreshException {
        return callTableEntryRefresh(randomObServers(rsList), key, connectTimeout, socketTimeout,
            sysUA, initialized, new TableEntryRefreshCallback<TableEntry>() {
                @Override
                TableEntry execute(Connection connection) throws ObTableEntryRefreshException {
                    return getTableEntryFromRemote(connection, key, initialized);
                }
            });
    }

    private static void getObVersionFromRemote(Connection connection)
                                                                     throws ObTableEntryRefreshException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            ps = connection.prepareStatement(OB_VERSION_SQL);
            rs = ps.executeQuery();
            if (rs.next()) {
                String versionString = rs.getString("CLUSTER_VERSION");
                parseObVersionFromSQL(versionString);
            } else {
                throw new ObTableEntryRefreshException("fail to get ob version from remote");
            }
        } catch (Exception e) {
            throw new ObTableEntryRefreshException("fail to get ob version from remote", e);
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

    // check tenant exist or not
    private static void checkTenantExistFromRemote(Connection connection, TableEntryKey key)
                                                                     throws ObTableEntryRefreshException {
        try (PreparedStatement ps = connection.prepareStatement(OB_TENANT_EXIST_SQL)) {
            ps.setString(1, key.getTenantName());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    throw new ObTableEntryRefreshException("fail to get tenant id from remote");
                }
            } catch (Exception e) {
                throw new ObTableEntryRefreshException("fail to get tenant id from remote", e);
            }
        } catch (Exception e) {
            throw new ObTableEntryRefreshException("fail to get tenant id from remote", e);
        }
    }

    private static TableEntry getTableEntryFromRemote(Connection connection, TableEntryKey key,
                                                      boolean initialized)
                                                                          throws ObTableEntryRefreshException {
        PreparedStatement ps = null;
        ResultSet rs = null;
        TableEntry tableEntry;
        try {
            if (ObGlobal.obVsnMajor() == 0) {
                getObVersionFromRemote(connection);
            }
            checkTenantExistFromRemote(connection, key);
            if (ObGlobal.obVsnMajor() >= 4) {
                if (key.getTableName().equals(Constants.ALL_DUMMY_TABLE)) {
                    ps = connection.prepareStatement(PROXY_DUMMY_LOCATION_SQL_V4);
                    ps.setString(1, key.getTenantName());
                    ps.setString(2, key.getDatabaseName());
                    ps.setString(3, key.getTableName());
                } else {
                    ps = connection.prepareStatement(PROXY_LOCATION_SQL_V4);
                    ps.setString(1, key.getTenantName());
                    ps.setString(2, key.getDatabaseName());
                    ps.setString(3, key.getTableName());
                }
            } else {
                if (key.getTableName().equals(Constants.ALL_DUMMY_TABLE)) {
                    ps = connection.prepareStatement(PROXY_DUMMY_LOCATION_SQL);
                    ps.setString(1, key.getTenantName());
                    ps.setString(2, key.getDatabaseName());
                    ps.setString(3, key.getTableName());
                } else {
                    ps = connection.prepareStatement(PROXY_LOCATION_SQL);
                    ps.setString(1, key.getTenantName());
                    ps.setString(2, key.getDatabaseName());
                    ps.setString(3, key.getTableName());
                }
            }
            rs = ps.executeQuery();
            tableEntry = getTableEntryFromResultSet(key, rs);
            if (null != tableEntry) {
                tableEntry.setTableEntryKey(key);
                // TODO: check capacity flag later
                // fetch tablet ids when table is partition table
                if (tableEntry.isPartitionTable()) {
                    // fetch partition info
                    fetchPartitionInfo(connection, tableEntry);
                    if (null != tableEntry.getPartitionInfo()) {
                        // fetch first range part
                        if (null != tableEntry.getPartitionInfo().getFirstPartDesc()) {
                            ObPartFuncType obPartFuncType = tableEntry.getPartitionInfo()
                                .getFirstPartDesc().getPartFuncType();
                            fetchFirstPart(connection, tableEntry, obPartFuncType);
                        }
                        // fetch sub range part
                        if (null != tableEntry.getPartitionInfo().getSubPartDesc()) {
                            ObPartFuncType subPartFuncType = tableEntry.getPartitionInfo()
                                .getSubPartDesc().getPartFuncType();
                            fetchSubPart(connection, tableEntry, subPartFuncType);
                        }
                    }
                }

                // get location info
                getTableEntryLocationFromRemote(connection, key, tableEntry);

                if (!initialized) {
                    if (BOOT.isInfoEnabled()) {
                        BOOT.info("get table entry from remote, entry={}", JSON.toJSON(tableEntry));
                    }
                } else {
                    if (logger.isInfoEnabled()) {
                        logger.info("get table entry from remote");
                    }
                }
            }
        } catch (ObTableNotExistException e) {
            // avoid to refresh meta for ObTableNotExistException
            RUNTIME.error("getTableEntryFromRemote meet exception", e);
            throw e;
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00009"), key, e);
            throw new ObTableEntryRefreshException(format(
                "fail to get table entry from remote, key=%s", key), e);
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
        return tableEntry;
    }

    /*
     * Get table entry location from remote.
     */
    public static TableEntry getTableEntryLocationFromRemote(Connection connection,
                                                             TableEntryKey key,
                                                             TableEntry tableEntry)
                                                                                   throws ObTablePartitionLocationRefreshException {

        PreparedStatement ps = null;
        ResultSet rs = null;
        long partitionNum = tableEntry.getPartitionNum();
        StringBuilder sb = new StringBuilder();
        ObPartitionEntry partitionEntry;
        String sql = null;
        if (ObGlobal.obVsnMajor() >= 4) {
            if (tableEntry.isPartitionTable()) {
                Map<Long, Long> partTabletIdMap = tableEntry.getPartitionInfo()
                    .getPartTabletIdMap();
                int i = 0;
                for (Long tabletId : partTabletIdMap.values()) {
                    if (i++ > 0) {
                        sb.append(", ");
                    }
                    sb.append(tabletId);
                }
            } else {
                for (int i = 0; i < partitionNum; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(i);
                }
            }
            sql = MessageFormat.format(PROXY_LOCATION_SQL_PARTITION_V4, sb.toString());
        } else {
            if (tableEntry.isPartitionTable()
                && null != tableEntry.getPartitionInfo().getSubPartDesc()) {
                long firstPartNum = tableEntry.getPartitionInfo().getFirstPartDesc().getPartNum();
                long subPartNum = tableEntry.getPartitionInfo().getSubPartDesc().getPartNum();
                for (long i = 0; i < firstPartNum; ++i) {
                    for (long j = 0; j < subPartNum; ++j) {
                        if (i > 0 || j > 0) {
                            sb.append(", ");
                        }
                        sb.append(ObPartIdCalculator.generatePartId(i, j));
                    }
                }
            } else {
                for (int i = 0; i < partitionNum; i++) {
                    if (i > 0) {
                        sb.append(", ");
                    }
                    sb.append(i);
                }
            }
            sql = MessageFormat.format(PROXY_LOCATION_SQL_PARTITION, sb.toString());
        }
        try {
            ps = connection.prepareStatement(sql);
            ps.setString(1, key.getTenantName());
            ps.setString(2, key.getDatabaseName());
            ps.setString(3, key.getTableName());

            rs = ps.executeQuery();
            partitionEntry = getPartitionLocationFromResultSet(tableEntry, rs);
            tableEntry.setPartitionEntry(partitionEntry);
            tableEntry.setRefreshTimeMills(System.currentTimeMillis());
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00010"), key, partitionNum, tableEntry, e);
            throw new ObTablePartitionLocationRefreshException(
                format(
                    "fail to get partition location entry from remote entryKey = %s partNum = %d tableEntry =%s",
                    key, partitionNum, tableEntry), e);
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
        return tableEntry;
    }

    public static Long getTableIdFromRemote(ObServerAddr obServerAddr, ObUserAuth sysUA,
                                            long connectTimeout, long socketTimeout,
                                            String tenantName, String databaseName, String tableName)
                                                                                                     throws ObTableEntryRefreshException {
        Long tableId = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String url = formatObServerUrl(obServerAddr, connectTimeout, socketTimeout);
            connection = getMetaRefreshConnection(url, sysUA);
            ps = connection.prepareStatement(PROXY_TABLE_ID_SQL);
            ps.setString(1, tenantName);
            ps.setString(2, databaseName);
            ps.setString(3, tableName);
            rs = ps.executeQuery();
            if (rs.next()) {
                tableId = rs.getLong("table_id");
            } else {
                throw new ObTableEntryRefreshException("fail to get " + tableName
                                                       + " table_id from remote");
            }
        } catch (Exception e) {
            throw new ObTableEntryRefreshException("fail to get " + tableName
                                                   + " table_id from remote", e);
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
        return tableId;
    }

    public static ObIndexInfo getIndexInfoFromRemote(ObServerAddr obServerAddr, ObUserAuth sysUA,
                                                     long connectTimeout, long socketTimeout,
                                                     String indexTableName)
                                                                           throws ObTableEntryRefreshException {
        ObIndexInfo indexInfo = null;
        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            String url = formatObServerUrl(obServerAddr, connectTimeout, socketTimeout);
            connection = getMetaRefreshConnection(url, sysUA);
            ps = connection.prepareStatement(PROXY_INDEX_INFO_SQL);
            ps.setString(1, indexTableName);
            rs = ps.executeQuery();
            if (rs.next()) {
                indexInfo = new ObIndexInfo();
                indexInfo.setDataTableId(rs.getLong("data_table_id"));
                indexInfo.setIndexTableId(rs.getLong("table_id"));
                indexInfo.setIndexType(ObIndexType.valueOf(rs.getInt("index_type")));
            } else {
                throw new ObTableEntryRefreshException(
                    "fail to get index info from remote, result set is empty");
            }
        } catch (Exception e) {
            throw new ObTableEntryRefreshException(format(
                "fail to get index info from remote, indexTableName: %s", indexTableName), e);
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
        return indexInfo;
    }

    private static void fetchFirstPart(Connection connection, TableEntry tableEntry,
                                       ObPartFuncType obPartFuncType)
                                                                     throws ObTablePartitionInfoRefreshException {
        String tableName = "";
        TableEntryKey key = tableEntry.getTableEntryKey();
        if (key != null) {
            tableName = key.getDatabaseName() + "." + key.getTableName();
        }
        String uuid = UUID.randomUUID().toString();

        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            if (ObGlobal.obVsnMajor() >= 4) {
                ps = connection.prepareStatement(PROXY_FIRST_PARTITION_SQL_V4);
                ps.setString(1, key.getTenantName());
                ps.setLong(2, tableEntry.getTableId());
                ps.setInt(3, Integer.MAX_VALUE);
            } else {
                ps = connection.prepareStatement(PROXY_FIRST_PARTITION_SQL);
                ps.setLong(1, tableEntry.getTableId());
                ps.setInt(2, Integer.MAX_VALUE);
            }
            rs = ps.executeQuery();
            if (obPartFuncType.isRangePart()) {
                List<ObComparableKV<ObPartitionKey, Long>> bounds = parseFirstPartRange(rs,
                    tableEntry);
                ((ObRangePartDesc) tableEntry.getPartitionInfo().getFirstPartDesc())
                    .setBounds(bounds);

                if (logger.isInfoEnabled()) {
                    logger.info(format("uuid:%s, get first ranges from remote for %s, bounds=%s",
                        uuid, tableName, JSON.toJSON(bounds)));
                }

            } else if (obPartFuncType.isListPart()) {
                Map<ObPartitionKey, Long> sets = parseFirstPartSets(rs, tableEntry);
                ((ObListPartDesc) tableEntry.getPartitionInfo().getFirstPartDesc()).setSets(sets);
                if (logger.isInfoEnabled()) {
                    logger.info(format("uuid:%s, get first list sets from remote for %s, sets=%s",
                        uuid, tableName, JSON.toJSON(sets)));
                }
            } else if (ObGlobal.obVsnMajor() >= 4
                       && (obPartFuncType.isKeyPart() || obPartFuncType.isHashPart())) {
                tableEntry.getPartitionInfo().setPartTabletIdMap(
                    parseFirstPartKeyHash(rs, tableEntry));
            }
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00011"), tableEntry, obPartFuncType, e);

            throw new ObTablePartitionInfoRefreshException(format(
                "fail to get first part from remote for %s, tableEntry=%s partFuncType=%s",
                tableName, tableEntry, obPartFuncType), e);
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

    private static void fetchSubPart(Connection connection, TableEntry tableEntry,
                                     ObPartFuncType subPartFuncType)
                                                                    throws ObTablePartitionInfoRefreshException {
        String tableName = "";
        TableEntryKey key = tableEntry.getTableEntryKey();
        if (key != null) {
            tableName = key.getDatabaseName() + "." + key.getTableName();
        }
        String uuid = UUID.randomUUID().toString();

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            if (ObGlobal.obVsnMajor() >= 4) {
                pstmt = connection.prepareStatement(PROXY_SUB_PARTITION_SQL_V4);
                pstmt.setString(1, key.getTenantName());
                pstmt.setLong(2, tableEntry.getTableId());
                pstmt.setInt(3, Integer.MAX_VALUE);
            } else {
                pstmt = connection.prepareStatement(PROXY_SUB_PARTITION_SQL);
                pstmt.setLong(1, tableEntry.getTableId());
                pstmt.setInt(2, Integer.MAX_VALUE);
            }

            rs = pstmt.executeQuery();
            if (subPartFuncType.isRangePart()) {
                List<ObComparableKV<ObPartitionKey, Long>> bounds = parseSubPartRange(rs,
                    tableEntry);
                ((ObRangePartDesc) tableEntry.getPartitionInfo().getSubPartDesc())
                    .setBounds(bounds);
                if (logger.isInfoEnabled()) {
                    logger.info(format("uuid:%s, get sub ranges from remote for %s, bounds=%s",
                        uuid, tableName, JSON.toJSON(bounds)));
                }
            } else if (subPartFuncType.isListPart()) {
                Map<ObPartitionKey, Long> sets = parseSubPartSets(rs, tableEntry);
                ((ObListPartDesc) tableEntry.getPartitionInfo().getSubPartDesc()).setSets(sets);
                if (logger.isInfoEnabled()) {
                    logger.info(format("uuid:%s, get sub list sets from remote, sets=%s", uuid,
                        JSON.toJSON(sets)));
                }
            } else if (ObGlobal.obVsnMajor() >= 4
                       && (subPartFuncType.isKeyPart() || subPartFuncType.isHashPart())) {
                tableEntry.getPartitionInfo().setPartTabletIdMap(
                    parseSubPartKeyHash(rs, tableEntry));
            }
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00012"), tableEntry, subPartFuncType, e);
            throw new ObTablePartitionInfoRefreshException(format(
                "fail to get sub part from remote, tableEntry=%s partFuncType=%s", tableEntry,
                subPartFuncType), e);
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
                if (null != pstmt) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }
    }

    private static TableEntry getTableEntryFromResultSet(TableEntryKey key, ResultSet rs)
                                                                                         throws SQLException,
                                                                                         ObTableEntryRefreshException {
        TableEntry entry = new TableEntry();
        Long replicaNum = null;
        Long partitionNum = null;
        Long tableId = null;
        List<ReplicaLocation> replicaLocations = new ArrayList<ReplicaLocation>(3);
        while (rs.next()) {
            ReplicaLocation replica = buildReplicaLocation(rs);
            tableId = rs.getLong("table_id");
            replicaNum = rs.getLong("replica_num");
            partitionNum = rs.getLong("part_num");
            if (!replica.isValid()) {
                logger
                    .warn(format("replica is invalid, continue, replica=%s, key=%s", replica, key));
            } else if (replicaLocations.contains(replica)) {
                logger.warn(format(
                    "replica is repeated, continue, replica=%s, key=%s, replicas=%s", replica, key,
                    replicaLocations));
            } else {
                replicaLocations.add(replica);
            }
        }
        TableLocation tableLocation = new TableLocation();
        tableLocation.setReplicaLocations(replicaLocations);
        if (!replicaLocations.isEmpty()) {
            entry.setTableId(tableId);
            entry.setTableLocation(tableLocation);
            entry.setPartitionNum(partitionNum);
            entry.setReplicaNum(replicaNum);
        } else {
            RUNTIME.error("table not exist");
            throw new ObTableNotExistException("table not exist: " + key.getTableName(),
                ResultCodes.OB_ERR_UNKNOWN_TABLE.errorCode);
        }

        return entry;
    }

    private static ObPartitionEntry getPartitionLocationFromResultSet(TableEntry tableEntry,
                                                                      ResultSet rs)
                                                                                   throws SQLException,
                                                                                   ObTablePartitionLocationRefreshException {
        Map<Long, ObPartitionLocation> partitionLocation = new HashMap<Long, ObPartitionLocation>();
        Map<Long, Long> tabletLsIdMap = new HashMap<>();
        while (rs.next()) {
            ReplicaLocation replica = buildReplicaLocation(rs);
            long partitionId;
            if (ObGlobal.obVsnMajor() >= 4) {
                partitionId = rs.getLong("tablet_id");
                long lsId = rs.getLong("ls_id");
                if (!rs.wasNull()) {
                    tabletLsIdMap.put(partitionId, lsId);
                } else {
                    tabletLsIdMap.put(partitionId, INVALID_LS_ID); // non-partitioned table
                }
            } else {
                partitionId = rs.getLong("partition_id");
                if (tableEntry.isPartitionTable()
                    && null != tableEntry.getPartitionInfo().getSubPartDesc()) {
                    partitionId = ObPartIdCalculator.getPartIdx(partitionId, tableEntry
                        .getPartitionInfo().getSubPartDesc().getPartNum());
                }
            }
            if (!replica.isValid()) {
                RUNTIME
                    .warn(format(
                        "replica is invalid, continue, replica=%s, partitionId/tabletId=%d, tableId=%d",
                        replica, partitionId, tableEntry.getTableId()));
                continue;
            }
            ObPartitionLocation location = partitionLocation.get(partitionId);

            if (location == null) {
                location = new ObPartitionLocation();
                partitionLocation.put(partitionId, location);
            }
            location.addReplicaLocation(replica);
        }
        ObPartitionEntry partitionEntry = new ObPartitionEntry();
        partitionEntry.setPartitionLocation(partitionLocation);
        partitionEntry.setTabletLsIdMap(tabletLsIdMap);

        if (ObGlobal.obVsnMajor() < 4) {
            for (long i = 0; i < tableEntry.getPartitionNum(); i++) {
                ObPartitionLocation location = partitionEntry.getPartitionLocationWithPartId(i);
                if (location == null) {
                    RUNTIME.error(LCD.convert("01-00013"), i, partitionEntry, tableEntry);
                    RUNTIME.error(format(
                        "partition num=%d is not exist partitionEntry=%s original tableEntry=%s",
                        i, partitionEntry, tableEntry));
                    throw new ObTablePartitionNotExistException(format(
                        "partition num=%d is not exist partitionEntry=%s original tableEntry=%s",
                        i, partitionEntry, tableEntry));
                }
                if (location.getLeader() == null) {
                    RUNTIME.error(LCD.convert("01-00028"), i, partitionEntry, tableEntry);
                    RUNTIME.error(format(
                        "partition num=%d has no leader partitionEntry=%s original tableEntry=%s",
                        i, partitionEntry, tableEntry));
                    throw new ObTablePartitionNoMasterException(format(
                        "partition num=%d has no leader partitionEntry=%s original tableEntry=%s",
                        i, partitionEntry, tableEntry));
                }
            }
        }

        return partitionEntry;
    }

    /*
     * Get ReplicaLocation from the result row.
     *
     * @param rs
     * @return
     * @throws SQLException
     */
    private static ReplicaLocation buildReplicaLocation(ResultSet rs) throws SQLException {
        String ip = rs.getString("svr_ip");
        int port = rs.getInt("sql_port");
        int svrPort = rs.getInt("svr_port");
        ObServerRole role = ObServerRole.getRole(rs.getInt("role"));
        String status = rs.getString("status");
        long stopTime = rs.getLong("stop_time");
        ObReplicaType replicaType = ObReplicaType.getReplicaType(rs.getInt("replica_type"));

        ReplicaLocation replica = new ReplicaLocation();
        ObServerAddr obServerAddr = new ObServerAddr();
        obServerAddr.setAddress(ip);
        obServerAddr.setSqlPort(port);
        obServerAddr.setSvrPort(svrPort);

        ObServerInfo obServerInfo = new ObServerInfo();
        obServerInfo.setStatus(status);
        obServerInfo.setStopTime(stopTime);

        replica.setAddr(obServerAddr);
        replica.setInfo(obServerInfo);
        replica.setRole(role);
        replica.setReplicaType(replicaType);
        return replica;
    }

    private static void fetchPartitionInfo(Connection connection, TableEntry tableEntry)
                                                                                        throws ObTablePartitionInfoRefreshException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        ObPartitionInfo info = null;
        try {
            if (ObGlobal.obVsnMajor() >= 4) {
                pstmt = connection.prepareStatement(PROXY_PART_INFO_SQL_V4);
                pstmt.setString(1, tableEntry.getTableEntryKey().getTenantName());
                pstmt.setLong(2, tableEntry.getTableId());
                pstmt.setLong(3, Long.MAX_VALUE);
            } else {
                pstmt = connection.prepareStatement(PROXY_PART_INFO_SQL);
                pstmt.setLong(1, tableEntry.getTableId());
                pstmt.setLong(2, Long.MAX_VALUE);
            }

            rs = pstmt.executeQuery();
            info = parsePartitionInfo(rs);

            if (logger.isInfoEnabled()) {
                logger.info("get part info from remote info:{}", JSON.toJSON(info));
            }
            tableEntry.setPartitionInfo(info);
        } catch (Exception e) {
            RUNTIME.error(LCD.convert("01-00014"), tableEntry);
            RUNTIME.error("fail to get part info from remote");
            throw new ObTablePartitionInfoRefreshException(format(
                "fail to get part info from remote, tableEntry=%s", tableEntry), e);
        } finally {
            try {
                if (null != rs) {
                    rs.close();
                }
                if (null != pstmt) {
                    pstmt.close();
                }
            } catch (SQLException e) {
                // ignore
            }
        }

    }

    private static ObPartitionInfo parsePartitionInfo(ResultSet rs)
                                                                   throws IllegalArgumentException,
                                                                   GenerateColumnParseException,
                                                                   SQLException {
        ObPartitionInfo info = new ObPartitionInfo();
        boolean isFirstRow = true;
        while (rs.next()) {
            // get part info for the first loop
            if (isFirstRow) {
                isFirstRow = false;
                // get part level
                info.setLevel(ObPartitionLevel.valueOf(rs.getLong("part_level")));

                // get first part
                if (info.getLevel().getIndex() >= ObPartitionLevel.LEVEL_ONE.getIndex()) {
                    ObPartDesc partDesc = buildPartDesc(ObPartitionLevel.LEVEL_ONE, rs);
                    if (partDesc == null) {
                        logger.warn("fail to build first part");
                    } else {
                        info.setFirstPartDesc(partDesc);
                    }
                }

                // get sub part
                if (info.getLevel().getIndex() == ObPartitionLevel.LEVEL_TWO.getIndex()) {
                    ObPartDesc partDesc = buildPartDesc(ObPartitionLevel.LEVEL_TWO, rs);
                    if (partDesc == null) {
                        logger.warn("fail to build sub part");
                    } else {
                        info.setSubPartDesc(partDesc);
                    }
                }
            }

            // get part key for each loop
            String partKeyExtra = rs.getString("part_key_extra");
            partKeyExtra = partKeyExtra.replace("`", ""); // '`' is not supported by druid
            partKeyExtra = partKeyExtra.replace(" ", ""); // ' ' should be removed
            ObColumn column;
            String collationTypeLabel = null;
            if (ObGlobal.obVsnMajor() >= 4) {
                collationTypeLabel = "part_key_collation_type";
            } else {
                collationTypeLabel = "spare1";
            }
            if (!partKeyExtra.isEmpty()) {
                column = new ObGeneratedColumn(
                    rs.getString("part_key_name"),//
                    rs.getInt("part_key_idx"),//
                    ObObjType.valueOf(rs.getInt("part_key_type")),//
                    ObCollationType.valueOf(rs.getInt(collationTypeLabel)),
                    new ObGeneratedColumnExpressParser(getPlainString(partKeyExtra)).parse());
            } else {
                column = new ObSimpleColumn(rs.getString("part_key_name"),//
                    rs.getInt("part_key_idx"),//
                    ObObjType.valueOf(rs.getInt("part_key_type")),//
                    ObCollationType.valueOf(rs.getInt(collationTypeLabel)));
            }

            info.addColumn(column);
        }

        // get list partition column types here
        List<ObColumn> orderedPartedColumns1 = null;
        if (null != info.getFirstPartDesc()) {
            if (info.getFirstPartDesc().getPartFuncType().isListPart()
                || info.getFirstPartDesc().getPartFuncType().isRangePart()) {
                orderedPartedColumns1 = getOrderedPartColumns(info.getPartColumns(),
                    info.getFirstPartDesc());
            }
        }

        List<ObColumn> orderedPartedColumns2 = null;
        if (null != info.getSubPartDesc()) {
            if (info.getSubPartDesc().getPartFuncType().isListPart()
                || info.getSubPartDesc().getPartFuncType().isRangePart()) {
                orderedPartedColumns2 = getOrderedPartColumns(info.getPartColumns(),
                    info.getSubPartDesc());
            }
        }

        // set the property of first part and sub part
        setPartDescProperty(info.getFirstPartDesc(), info.getPartColumns(), orderedPartedColumns1);
        setPartDescProperty(info.getSubPartDesc(), info.getPartColumns(), orderedPartedColumns2);

        return info;
    }

    private static ObPartDesc buildPartDesc(ObPartitionLevel level, ResultSet rs)
                                                                                 throws SQLException {
        ObPartDesc partDesc = null;
        String partLevelPrefix = (level == ObPartitionLevel.LEVEL_TWO ? "sub_" : "");
        ObPartFuncType partType = ObPartFuncType.getObPartFuncType(rs.getLong(partLevelPrefix
                                                                              + "part_type"));
        String partExpr = rs.getString(partLevelPrefix + "part_expr");
        partExpr = partExpr.replace("`", ""); // '`' is not supported by druid

        if (partType.isRangePart()) {
            ObRangePartDesc rangeDesc = new ObRangePartDesc();
            rangeDesc.setPartFuncType(partType);
            rangeDesc.setPartExpr(partExpr);
            rangeDesc.setPartNum(rs.getInt(partLevelPrefix + "part_num"));
            rangeDesc.setPartSpace(rs.getInt(partLevelPrefix + "part_space"));
            ArrayList<ObObjType> types = new ArrayList<ObObjType>(1);
            String objTypesStr = rs.getString(partLevelPrefix + "part_range_type");
            for (String typeStr : objTypesStr.split(",")) {
                types.add(ObObjType.valueOf(Integer.valueOf(typeStr)));
            }
            rangeDesc.setOrderedCompareColumnTypes(types);
            partDesc = rangeDesc;
        } else if (partType.isHashPart()) {
            ObHashPartDesc hashDesc = new ObHashPartDesc();
            hashDesc.setPartExpr(partExpr);
            hashDesc.setPartFuncType(partType);
            hashDesc.setPartNum(rs.getInt(partLevelPrefix + "part_num"));
            hashDesc.setPartSpace(rs.getInt(partLevelPrefix + "part_space"));
            if (ObGlobal.obVsnMajor() < 4) {
                Map<String, Long> partNameIdMap = buildDefaultPartNameIdMap(hashDesc.getPartNum());
                hashDesc.setPartNameIdMap(partNameIdMap);
            }
            partDesc = hashDesc;
        } else if (partType.isKeyPart()) {
            ObKeyPartDesc keyPartDesc = new ObKeyPartDesc();
            keyPartDesc.setPartFuncType(partType);
            keyPartDesc.setPartExpr(partExpr);
            keyPartDesc.setPartNum(rs.getInt(partLevelPrefix + "part_num"));
            keyPartDesc.setPartSpace(rs.getInt(partLevelPrefix + "part_space"));
            if (ObGlobal.obVsnMajor() < 4) {
                Map<String, Long> partNameIdMap = buildDefaultPartNameIdMap(keyPartDesc
                    .getPartNum());
                keyPartDesc.setPartNameIdMap(partNameIdMap);
            }
            partDesc = keyPartDesc;
        } else {
            RUNTIME.error(LCD.convert("01-00015"), partType);
            throw new IllegalArgumentException(format("not supported part type, type = %s",
                partType));
        }
        return partDesc;
    }

    private static List<ObColumn> getOrderedPartColumns(List<ObColumn> partitionKeyColumns,
                                                        ObPartDesc partDesc) {
        List<ObColumn> columns = new ArrayList<ObColumn>();
        for (String partColName : partDesc.getOrderedPartColumnNames()) {
            for (ObColumn keyColumn : partitionKeyColumns) {
                if (partColName.equalsIgnoreCase(keyColumn.getColumnName())) {
                    columns.add(keyColumn);
                }
            }
        }
        return columns;
    }

    private static void setPartDescProperty(ObPartDesc partDesc, List<ObColumn> partColumns,
                                            List<ObColumn> listPartColumns)
                                                                           throws ObTablePartitionInfoRefreshException {
        ObPartFuncType obPartFuncType = null;
        if (null != partDesc) {
            partDesc.setPartColumns(partColumns);
            obPartFuncType = partDesc.getPartFuncType();
            if (obPartFuncType.isKeyPart()) {
                if (partColumns == null || partColumns.size() == 0) {
                    RUNTIME.error("key part desc need part ref columns but found " + partColumns);
                    throw new ObTablePartitionInfoRefreshException(
                        "key part desc need part ref columns but found " + partColumns);
                }
            } else if (obPartFuncType.isListPart()) {
                ((ObListPartDesc) partDesc).setOrderCompareColumns(listPartColumns);
            } else if (obPartFuncType.isRangePart()) {
                ((ObRangePartDesc) partDesc).setOrderedCompareColumns(listPartColumns);
            }
        }
    }

    public static Map<String, Long> buildDefaultPartNameIdMap(int partNum) {
        // the default partition name is 'p0,p1...'
        Map<String, Long> partNameIdMap = new HashMap<String, Long>();
        for (int i = 0; i < partNum; i++) {
            partNameIdMap.put("p" + i, (long) i);
        }
        return partNameIdMap;
    }

    private static Map<String, Long> buildPartNameIdMap(ObPartitionInfo partitionInfo) {
        Map<String, Long> partNameIdMap1 = partitionInfo.getFirstPartDesc().getPartNameIdMap();
        Map<String, Long> partNameIdMap2 = Collections.EMPTY_MAP;
        Map<String, Long> partNameIdMap = new HashMap<String, Long>();
        for (String partName1 : partNameIdMap1.keySet()) {
            Long partId1 = partNameIdMap1.get(partName1);
            if (null != partitionInfo.getSubPartDesc()) {
                partNameIdMap2 = partitionInfo.getSubPartDesc().getPartNameIdMap();
                for (String partName2 : partNameIdMap2.keySet()) {
                    String comPartName = partName1 + "s" + partName2;
                    Long partId2 = partNameIdMap2.get(partName2);
                    Long partId = ObPartIdCalculator.generatePartId(partId1, partId2);
                    partNameIdMap.put(comPartName, partId);
                }
            } else {
                partNameIdMap.put(partName1, partId1);
            }
        }

        return partNameIdMap;
    }

    private static Map<Long, Long> parseFirstPartKeyHash(ResultSet rs, TableEntry tableEntry)
                                                                                             throws SQLException,
                                                                                             IllegalArgumentException,
                                                                                             FeatureNotSupportedException {
        return parseKeyHashPart(rs, tableEntry, false);
    }

    private static List<ObComparableKV<ObPartitionKey, Long>> parseFirstPartRange(ResultSet rs,
                                                                                  TableEntry tableEntry)
                                                                                                        throws SQLException,
                                                                                                        IllegalArgumentException,
                                                                                                        FeatureNotSupportedException {
        return parseRangePart(rs, tableEntry, false);
    }

    private static Map<ObPartitionKey, Long> parseFirstPartSets(ResultSet rs, TableEntry tableEntry)
                                                                                                    throws SQLException,
                                                                                                    IllegalArgumentException,
                                                                                                    FeatureNotSupportedException {
        return parseListPartSets(rs, tableEntry, false);
    }

    private static List<ObComparableKV<ObPartitionKey, Long>> parseSubPartRange(ResultSet rs,
                                                                                TableEntry tableEntry)
                                                                                                      throws SQLException,
                                                                                                      IllegalArgumentException,
                                                                                                      FeatureNotSupportedException {
        return parseRangePart(rs, tableEntry, true);
    }

    private static Map<ObPartitionKey, Long> parseSubPartSets(ResultSet rs, TableEntry tableEntry)
                                                                                                  throws SQLException,
                                                                                                  IllegalArgumentException,
                                                                                                  FeatureNotSupportedException {
        return parseListPartSets(rs, tableEntry, true);
    }

    private static Map<Long, Long> parseSubPartKeyHash(ResultSet rs, TableEntry tableEntry)
                                                                                           throws SQLException,
                                                                                           IllegalArgumentException,
                                                                                           FeatureNotSupportedException {
        return parseKeyHashPart(rs, tableEntry, true);
    }

    private static Map<Long, Long> parseKeyHashPart(ResultSet rs, TableEntry tableEntry,
                                                    boolean isSubPart) throws SQLException,
                                                                      IllegalArgumentException,
                                                                      FeatureNotSupportedException {
        long idx = 0L;
        Map<Long, Long> partTabletIdMap = new HashMap<Long, Long>();
        while (rs.next()) {
            ObPartDesc subPartDesc = tableEntry.getPartitionInfo().getSubPartDesc();
            if (null != subPartDesc) {
                // client only support template partition table
                // so the sub_part_num is a constant and will store in subPartDesc which is different from proxy
                if (subPartDesc instanceof ObKeyPartDesc) {
                    ObKeyPartDesc subKeyPartDesc = (ObKeyPartDesc) subPartDesc;
                    if (!isSubPart && subKeyPartDesc.getPartNum() == 0) {
                        long subPartNum = rs.getLong("sub_part_num");
                        subKeyPartDesc.setPartNum((int) subPartNum);
                    }
                } else if (subPartDesc instanceof ObHashPartDesc) {
                    ObHashPartDesc subHashPartDesc = (ObHashPartDesc) subPartDesc;
                    if (!isSubPart && subHashPartDesc.getPartNum() == 0) {
                        long subPartNum = rs.getLong("sub_part_num");
                        subHashPartDesc.setPartNum((int) subPartNum);
                    }
                } else {
                    throw new IllegalArgumentException("sub part desc is not key or hash part desc");
                }
            }
            Long tabletId = rs.getLong("tablet_id");
            partTabletIdMap.put(idx++, tabletId);
        }
        return partTabletIdMap;
    }

    private static List<ObComparableKV<ObPartitionKey, Long>> parseRangePart(ResultSet rs,
                                                                             TableEntry tableEntry,
                                                                             boolean isSubPart)
                                                                                               throws SQLException,
                                                                                               IllegalArgumentException,
                                                                                               FeatureNotSupportedException {
        String partIdColumnName = "part_id";
        ObPartDesc partDesc = tableEntry.getPartitionInfo().getFirstPartDesc();
        if (isSubPart) {
            partIdColumnName = "sub_part_id";
            partDesc = tableEntry.getPartitionInfo().getSubPartDesc();
        }

        List<ObColumn> orderPartColumns = ((ObRangePartDesc) partDesc).getOrderedCompareColumns();
        List<ObComparableKV<ObPartitionKey, Long>> bounds = new ArrayList<ObComparableKV<ObPartitionKey, Long>>();
        Map<String, Long> partNameIdMap = new HashMap<String, Long>();
        Map<Long, Long> partTabletIdMap = new HashMap<Long, Long>();
        ObPartDesc subRangePartDesc = tableEntry.getPartitionInfo().getSubPartDesc();
        long idx = 0L;
        while (rs.next()) {
            if (null != subRangePartDesc && !isSubPart && subRangePartDesc.getPartNum() == 0) {
                // client only support template partition table
                // so the sub_part_num is a constant and will store in subPartDesc which is different from proxy
                long subPartNum = rs.getLong("sub_part_num");
                subRangePartDesc.setPartNum((int) subPartNum);
            }

            String highBoundVal = rs.getString("high_bound_val");
            String[] splits = highBoundVal.split(",");
            List<Comparable> partElements = new ArrayList<Comparable>();

            for (int i = 0; i < splits.length; i++) {
                String elementStr = getPlainString(splits[i]);
                if (elementStr.equalsIgnoreCase("MAXVALUE")) {
                    partElements.add(MAX_PARTITION_ELEMENT);
                } else if (elementStr.equalsIgnoreCase("MINVALUE")) {
                    partElements.add(MIN_PARTITION_ELEMENT);
                } else {
                    partElements
                        .add(orderPartColumns
                            .get(i)
                            .getObObjType()
                            .parseToComparable(elementStr,
                                orderPartColumns.get(i).getObCollationType()));
                }
            }
            ObPartitionKey partitionKey = new ObPartitionKey(orderPartColumns, partElements);
            if (ObGlobal.obVsnMajor() >= 4) {
                long tabletId = rs.getLong("tablet_id");
                bounds.add(new ObComparableKV<ObPartitionKey, Long>(partitionKey, idx));
                partTabletIdMap.put(idx, tabletId);
                idx++;
            } else {
                long partId = rs.getLong(partIdColumnName);
                String partName = rs.getString("part_name");
                bounds.add(new ObComparableKV<ObPartitionKey, Long>(partitionKey, partId));
                partNameIdMap.put(partName.toLowerCase(), partId);
            }
        }
        if (ObGlobal.obVsnMajor() >= 4) {
            //set single level partition tablet-id mapping
            tableEntry.getPartitionInfo().setPartTabletIdMap(partTabletIdMap);
        } else {
            //set single level partition name-id mapping
            partDesc.setPartNameIdMap(partNameIdMap);
        }
        Collections.sort(bounds);
        return bounds;
    }

    private static String[] parseListPartSetsCommon(ResultSet rs, TableEntry tableEntry)
                                                                                        throws SQLException,
                                                                                        IllegalArgumentException,
                                                                                        FeatureNotSupportedException {
        // multi-columns: '(1,2),(1,3),(1,4),(default)'
        // single-columns: '(1),(2),(3)' or '1,2,3'
        String setsStr = rs.getString("high_bound_val");
        setsStr = (null == setsStr) ? "" : setsStr.trim();
        if (setsStr.length() < 2) {
            RUNTIME.error(LCD.convert("01-00016"), setsStr, tableEntry.toString());
            RUNTIME.error(format("high_bound_val value is error, high_bound_val=%s", setsStr));
            // if partition value format is wrong, directly throw exception
            throw new IllegalArgumentException(format(
                "high_bound_val value is error, high_bound_val=%s, tableEntry=%s", setsStr,
                tableEntry.toString()));
        }
        // skip the first character '(' and the last ')' if exist
        if (setsStr.startsWith("(") && setsStr.endsWith(")")) {
            setsStr = setsStr.substring(1, setsStr.length() - 1);
        }

        String[] setArray = null;
        if (setsStr.contains("),(")) { // multi-column format
            setArray = setsStr.split("\\),\\(");
        } else { // single-column format
            setArray = setsStr.split(",");
        }
        return setArray;
    }

    private static Map<ObPartitionKey, Long> parseListPartSets(ResultSet rs, TableEntry tableEntry,
                                                               boolean isSubPart)
                                                                                 throws SQLException,
                                                                                 IllegalArgumentException,
                                                                                 FeatureNotSupportedException {

        String partIdColumnName = "part_id";
        // tableEntry.getPartInfo() will not be null
        ObPartDesc partDesc = tableEntry.getPartitionInfo().getFirstPartDesc();
        if (isSubPart) {
            partIdColumnName = "sub_part_id";
            partDesc = tableEntry.getPartitionInfo().getSubPartDesc();
        }

        List<ObColumn> columns = ((ObListPartDesc) partDesc).getOrderCompareColumns();
        Map<ObPartitionKey, Long> sets = new HashMap<ObPartitionKey, Long>();
        if (ObGlobal.obVsnMajor() >= 4) {
            Map<Long, Long> partTabletIdMap = new HashMap<Long, Long>();
            long idx = 0L;
            while (rs.next()) {
                String[] setArray = parseListPartSetsCommon(rs, tableEntry);
                ObPartitionKey key = null;
                // setArray can not be null
                for (String set : setArray) {
                    if ("default".equalsIgnoreCase(set)) {
                        key = ObPartDesc.DEFAULT_PART_KEY;
                    } else {
                        String[] splits = set.split(",");

                        List<Comparable> partElements = new ArrayList<Comparable>();
                        for (int i = 0; i < splits.length; i++) {
                            partElements.add(columns.get(i).getObObjType()
                                .parseToComparable(splits[i], columns.get(i).getObCollationType()));
                        }
                        key = new ObPartitionKey(columns, partElements);
                    }
                    sets.put(key, idx);
                }
                long tabletId = rs.getLong("tablet_id");
                partTabletIdMap.put(idx++, tabletId);
            }
            //set single level partition tablet-id mapping
            tableEntry.getPartitionInfo().setPartTabletIdMap(partTabletIdMap);

        } else {
            Map<String, Long> partNameIdMap = new HashMap<String, Long>();
            while (rs.next()) {
                String[] setArray = parseListPartSetsCommon(rs, tableEntry);
                ObPartitionKey key = null;
                Long partId = null;
                String partName = null;
                // setArray can not be null
                for (String set : setArray) {
                    if ("default".equalsIgnoreCase(set)) {
                        key = ObPartDesc.DEFAULT_PART_KEY;
                    } else {
                        String[] splits = set.split(",");

                        List<Comparable> partElements = new ArrayList<Comparable>();
                        for (int i = 0; i < splits.length; i++) {
                            partElements.add(columns.get(i).getObObjType()
                                .parseToComparable(splits[i], columns.get(i).getObCollationType()));
                        }
                        key = new ObPartitionKey(columns, partElements);
                    }
                    partId = rs.getLong(partIdColumnName);
                    partName = rs.getString("part_name");
                    sets.put(key, partId);
                    partNameIdMap.put(partName.toLowerCase(), partId);
                }
            }
            //set single level partition name-id mapping
            partDesc.setPartNameIdMap(partNameIdMap);
        }

        return sets;
    }

    /*
     * Load ocp model.
     */
    public static OcpModel loadOcpModel(String paramURL, String dataSourceName, int connectTimeout,
                                        int readTimeout, int retryTimes, long retryInternal)
                                                                                            throws Exception {

        OcpModel ocpModel = new OcpModel();
        List<ObServerAddr> obServerAddrs = new ArrayList<ObServerAddr>();
        ocpModel.setObServerAddrs(obServerAddrs);

        OcpResponse ocpResponse = getRemoteOcpResponseOrNull(paramURL, dataSourceName,
            connectTimeout, readTimeout, retryTimes, retryInternal);

        if (ocpResponse == null && (dataSourceName != null && !dataSourceName.isEmpty())) { // get config from local file
            ocpResponse = getLocalOcpResponseOrNull(dataSourceName);
        }

        if (ocpResponse != null) {
            OcpResponseData ocpResponseData = ocpResponse.getData();
            ocpModel.setClusterId(ocpResponseData.getObRegionId());
            for (OcpResponseDataRs responseRs : ocpResponseData.getRsList()) {
                ObServerAddr obServerAddr = new ObServerAddr();
                obServerAddr.setAddress(responseRs.getAddress());
                obServerAddr.setSqlPort(responseRs.getSql_port());
                obServerAddrs.add(obServerAddr);
            }
        }

        if (obServerAddrs.isEmpty()) {
            RUNTIME.error("load rs list failed dataSource: " + dataSourceName + " paramURL:"
                          + paramURL + " response:" + ocpResponse);
            throw new RuntimeException("load rs list failed dataSource: " + dataSourceName
                                       + " paramURL:" + paramURL + " response:" + ocpResponse);
        }

        // Get IDC -> Region map if any.
        String obIdcRegionURL = paramURL.replace(Constants.OCP_ROOT_SERVICE_ACTION,
            Constants.OCP_IDC_REGION_ACTION);

        ocpResponse = getRemoteOcpIdcRegionOrNull(obIdcRegionURL, connectTimeout, readTimeout,
            retryTimes, retryInternal);

        if (ocpResponse != null) {
            OcpResponseData ocpResponseData = ocpResponse.getData();
            if (ocpResponseData != null && ocpResponseData.getIDCList() != null) {
                for (OcpResponseDataIDC idcRegion : ocpResponseData.getIDCList()) {
                    ocpModel.addIdc2Region(idcRegion.getIdc(), idcRegion.getRegion());
                }
            }
        }
        return ocpModel;
    }

    private static OcpResponse getRemoteOcpResponseOrNull(String paramURL, String dataSourceName,
                                                          int connectTimeout, int readTimeout,
                                                          int tryTimes, long retryInternal)
                                                                                           throws InterruptedException {

        OcpResponse ocpResponse = null;
        String content = null;
        int tries = 0;
        Exception cause = null;
        for (; tries < tryTimes; tries++) {
            try {
                content = loadStringFromUrl(paramURL, connectTimeout, readTimeout);
                ocpResponse = JSONObject.parseObject(content, OcpResponse.class);
                if (ocpResponse != null && ocpResponse.validate()) {
                    if (dataSourceName != null && !dataSourceName.isEmpty()) {
                        saveLocalContent(dataSourceName, content);
                    }
                    return ocpResponse;
                }
            } catch (Exception e) {
                cause = e;
                RUNTIME.error(LCD.convert("01-00017"), e);
                Thread.sleep(retryInternal);
            }
        }

        if (tries >= tryTimes) {
            RUNTIME
                .error("Fail to get OCP response after " + tryTimes + " tries from [" + paramURL);
            throw new ObTableRetryExhaustedException("Fail to get OCP response after " + tryTimes
                                                     + " tries from [" + paramURL
                                                     + "], the content is [" + content + "]", cause);
        }
        return null;
    }

    /*
     * Get IdcRegion info from OCP.
     * Return null instead of throwing exception if get nothing, because the info is optional.
     *
     * @param paramURL
     * @param connectTimeout
     * @param readTimeout
     * @param tryTimes
     * @param retryInternal
     * @return
     * @throws InterruptedException
     */
    private static OcpResponse getRemoteOcpIdcRegionOrNull(String paramURL, int connectTimeout,
                                                           int readTimeout, int tryTimes,
                                                           long retryInternal)
                                                                              throws InterruptedException {

        OcpResponse ocpResponse = null;
        String content = null;
        int tries = 0;
        for (; tries < tryTimes; tries++) {
            try {
                content = loadStringFromUrl(paramURL, connectTimeout, readTimeout);
                ocpResponse = JSONObject.parseObject(content, OcpResponse.class);
                if (ocpResponse != null) {
                    return ocpResponse;
                }
            } catch (Exception e) {
                RUNTIME.error(LCD.convert("01-00017"), e);
                Thread.sleep(retryInternal);
            }
        }

        if (tries >= tryTimes) {
            RUNTIME.error(LCD.convert("01-00017"), "OCP IdcRegion after" + tryTimes
                                                   + " tries from [" + paramURL
                                                   + "], the content is [" + content + "]");
        }
        return null;
    }

    private static OcpResponse parseOcpResponse(String content) throws JSONException {
        return JSONObject.parseObject(content, OcpResponse.class);
    }

    private static OcpResponse getLocalOcpResponseOrNull(String fileName) {
        File file = new File(format("%s/conf/obtable//%s", home, fileName));
        try {
            BufferedInputStream inputStream = null;
            try {
                InputStream fileInputStream;
                if (file.exists()) {
                    fileInputStream = new FileInputStream(file);
                } else {
                    return null;
                }
                inputStream = new BufferedInputStream(fileInputStream);
                byte[] bytes = new byte[inputStream.available()];
                int read = inputStream.read(bytes);
                if (read != bytes.length) {
                    throw new IOException("File bytes invalid: " + fileName);
                }
                String content = new String(bytes);
                return parseOcpResponse(content);
            } finally {
                if (inputStream != null) {
                    inputStream.close();
                }
            }
        } catch (IOException e) {
            RUNTIME.warn("load obtable file meet exception: " + file.getAbsolutePath(), e);
            return null;
        }

    }

    private static void saveLocalContent(String fileName, String content) {
        File file = new File(format("%s/conf/obtable/%s", home, fileName));

        // Format content
        try {
            if (!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }

            BufferedOutputStream outputStream = null;
            try {
                outputStream = new BufferedOutputStream(new FileOutputStream(file));
                outputStream.write(content.getBytes());
            } finally {
                if (outputStream != null) {
                    outputStream.close();
                }
            }
        } catch (IOException e) {
            // 写配置失败需要删除文件，避免写入脏数据
            file.delete();
            RUNTIME.warn("Save obtable file meet exception: " + file.getAbsolutePath(), e);
        }
    }

    private static String loadStringFromUrl(String url, int connectTimeout, int readTimeout)
                                                                                            throws Exception {
        HttpURLConnection con = null;
        String content;
        try {
            URL obj = new URL(url);
            con = (HttpURLConnection) obj.openConnection();
            // optional default is GET
            con.setRequestMethod("GET");
            con.setConnectTimeout(connectTimeout);
            con.setReadTimeout(readTimeout);
            con.connect();

            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            content = response.toString();
        } finally {
            if (con != null) {
                con.disconnect();
            }
        }

        return content;
    }

    // trim single '
    private static String getPlainString(String str) {
        int start = str.length() > 0 && str.charAt(0) == '\'' ? 1 : 0;
        int end = str.length() > 0 && str.charAt(str.length() - 1) == '\'' ? str.length() - 1 : str
            .length();
        return str.substring(start, end);
    }

    private static void parseObVersionFromSQL(String serverVersion) {
        // serverVersion is like "4.2.1.0"
        Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)");
        Matcher matcher = pattern.matcher(serverVersion);
        if (matcher.find() && ObGlobal.OB_VERSION == 0) {
            ObGlobal.OB_VERSION = ObGlobal.calcVersion(Integer.parseInt(matcher.group(1)),
                (short) Integer.parseInt(matcher.group(2)),
                (byte) Integer.parseInt(matcher.group(3)),
                (byte) Integer.parseInt(matcher.group(4)));
        }
    }

    public static void parseObVerionFromLogin(String serverVersion) {
        Pattern pattern;
        if (serverVersion.startsWith("OceanBase_CE")) {
            // serverVersion in CE is like "OceanBase_CE 4.0.0.0"
            pattern = Pattern.compile("OceanBase_CE\\s+(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)");
        } else {
            // serverVersion is like "OceanBase 4.0.0.0"
            pattern = Pattern.compile("OceanBase\\s+(\\d+)\\.(\\d+)\\.(\\d+)\\.(\\d+)");
        }
        Matcher matcher = pattern.matcher(serverVersion);
        if (matcher.find() && ObGlobal.OB_VERSION == 0) {
            ObGlobal.OB_VERSION = ObGlobal.calcVersion(Integer.parseInt(matcher.group(1)),
                (short) Integer.parseInt(matcher.group(2)),
                (byte) Integer.parseInt(matcher.group(3)),
                (byte) Integer.parseInt(matcher.group(4)));
        }
    }
}

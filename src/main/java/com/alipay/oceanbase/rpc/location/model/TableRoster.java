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

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableCloseException;
import com.alipay.oceanbase.rpc.table.*;
import org.slf4j.Logger;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;

public class TableRoster {
    private static final Logger logger = getLogger(TableRoster.class);
    private String tenantName;
    private String userName;
    private String password;
    private String database;
    private Properties properties = new Properties();
    private Map<String, Object> tableConfigs = new HashMap<>();
    private ObTableClientType clientType;
    /*
     * ServerAddr(all) -> ObTableConnection
     */
    private volatile ConcurrentHashMap<ObServerAddr, ObTable> tables = new ConcurrentHashMap<ObServerAddr, ObTable>();

    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }
    public void setUserName(String userName) {
        this.userName = userName;
    }
    public void setPassword(String password) {
        this.password = password;
    }
    public void setDatabase(String database) {
        this.database = database;
    }
    public void setTables(ConcurrentHashMap<ObServerAddr, ObTable> tables) {
        this.tables = tables;
    }
    public void setClientType(ObTableClientType clientType) {
        this.clientType = clientType;
    }
    public void setProperties(Properties properties) {
        this.properties = properties;
    }
    public void setTableConfigs(Map<String, Object> tableConfigs) {
        this.tableConfigs = tableConfigs;
    }
    public ObTable getTable(ObServerAddr addr) {
        return tables.get(addr);
    }
    public ConcurrentHashMap<ObServerAddr, ObTable> getTables() { return tables; }
    public boolean containsKey(ObServerAddr addr) {
        return tables.containsKey(addr);
    }
    public ObTable putIfAbsent(ObServerAddr addr, ObTable obTable) {
        return tables.putIfAbsent(addr, obTable);
    }

    public List<ObServerAddr> refreshTablesAndGetNewServers(List<ReplicaLocation> newLocations) throws Exception {
        List<ObServerAddr> newServers = new ArrayList<>();
        for (ReplicaLocation replicaLocation : newLocations) {
            ObServerAddr addr = replicaLocation.getAddr();
            ObServerInfo info = replicaLocation.getInfo();
            if (!info.isActive()) {
                logger.warn("will not refresh location {} because status is {} stop time {}",
                        addr.toString(), info.getStatus(), info.getStopTime());
                continue;
            }

            newServers.add(addr);

            if (tables.containsKey(addr)) { // has ob table addr, continue
                continue;
            }

            ObTable obTable = new ObTable.Builder(addr.getIp(), addr.getSvrPort()) //
                    .setLoginInfo(tenantName, userName, password, database, clientType) //
                    .setProperties(properties).setConfigs(tableConfigs).build();
            ObTable oldObTable = tables.putIfAbsent(addr, obTable);
            logger.warn("add new table addr, {}", addr.toString());
            if (oldObTable != null) { // maybe create two ob table concurrently, close current ob table
                obTable.close();
            }
        }
        // clean useless obTable connection
        for (ObServerAddr addr : tables.keySet()) {
            if (newServers.contains(addr)) {
                continue;
            }
            ObTable table = this.tables.remove(addr);
            logger.warn("remove useless table addr, {}", addr.toString());
            if (table != null) {
                table.close();
            }
        }
        return newServers;
    }

    public void closeRoster() throws ObTableCloseException {
        Exception throwException = null;
        List<ObServerAddr> exceptionObServers = new ArrayList<ObServerAddr>();
        for (Map.Entry<ObServerAddr, ObTable> entry : tables.entrySet()) {
            try {
                entry.getValue().close();
            } catch (Exception e) {
                // do not throw exception immediately
                BOOT.error(LCD.convert("01-00004"), entry.getKey(), e);
                RUNTIME.error(LCD.convert("01-00004"), entry.getKey(), e);
                throwException = e;
                exceptionObServers.add(entry.getKey());
            }
        }
        if (!exceptionObServers.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append("following ob servers [");
            for (int i = 0; i < exceptionObServers.size(); i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(exceptionObServers.get(i));
            }
            sb.append("] close error.");
            throw new ObTableCloseException(sb.toString(), throwException);
        }
    }

    public static TableRoster getInstanceOf(String tenantName, String userName, String password, String database,
                                            ObTableClientType clientType, Properties properties, Map<String, Object> tableConfigs) {
        TableRoster tableRoster = new TableRoster();
        tableRoster.setTenantName(tenantName);
        tableRoster.setUserName(userName);
        tableRoster.setPassword(password);
        tableRoster.setDatabase(database);
        tableRoster.setClientType(clientType);
        tableRoster.setProperties(properties);
        tableRoster.setTableConfigs(tableConfigs);
        return tableRoster;
    }
}

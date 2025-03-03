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
                              String database, Properties properties,
                              Map<String, Object> tableConfigs) throws Exception {
        this.obTable = new ObTable.Builder(addr, port)
            .setLoginInfo(tenantName, fullUserName, password, database).setProperties(properties)
            .setConfigs(tableConfigs).build();
    }

    public ObTable getObTable() {
        return this.obTable;
    }
}

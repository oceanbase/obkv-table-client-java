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

package com.alipay.oceanbase.rpc.location.model;

/**
 * ObServerLdcItem defines the LDC information for server, including idc, zone and region.
 *
 */
public class ObServerLdcItem {
    private String ip;
    private int    svrPort;
    private String zone;   // concept of OceanBase
    private String idc;    // physical idc
    private String region; // city

    /*
     * Constructor.
     *
     * @param ip
     * @param svrPort
     * @param zone
     * @param idc
     * @param region
     */
    public ObServerLdcItem(String ip, int svrPort, String zone, String idc, String region) {
        this.ip = ip;
        this.svrPort = svrPort;
        this.zone = zone;
        this.idc = idc;
        this.region = region;
    }

    /*
     * Get IP of the server.
     */
    public String getIp() {
        return ip;
    }

    /*
     * Get server port.
     */
    public int getSvrPort() {
        return svrPort;
    }

    /*
     * Get Zone of the server.
     */
    public String getZone() {
        return zone;
    }

    /*
     * Get IDC of the server.
     */
    public String getIdc() {
        return idc;
    }

    /*
     * Get region of the server.
     */
    public String getRegion() {
        return region;
    }

    /*
     * To String.
     */
    @Override
    public String toString() {
        return "ObServerLdcItem{" + "ip='" + ip + '\'' + ", svrPort=" + svrPort + ", zone='" + zone
               + '\'' + ", idc='" + idc + '\'' + ", region='" + region + '\'' + '}';
    }
}

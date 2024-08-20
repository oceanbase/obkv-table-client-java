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

import java.util.HashMap;
import java.util.List;

public class OcpModel {

    private List<ObServerAddr>      obServerAddrs;
    private long                    clusterId  = -1;
    private HashMap<String, String> idc2Region = new HashMap<String, String>();

    /*
     * Get ob server addrs.
     */
    public List<ObServerAddr> getObServerAddrs() {
        return obServerAddrs;
    }

    /*
     * Set ob server addrs.
     */
    public void setObServerAddrs(List<ObServerAddr> obServerAddrs) {
        this.obServerAddrs = obServerAddrs;
    }

    /*
     * Get cluster id.
     */
    public long getClusterId() {
        return clusterId;
    }

    /*
     * Set cluster id.
     */
    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    /*
     * Get Region by IDC.
     */
    public String getIdc2Region(String idc) {
        return idc2Region.get(idc);
    }

    public HashMap<String, String> getIdc2Region() {
        return idc2Region;
    }

    /*
     * Add Idc-Region pair.
     */
    public void addIdc2Region(String idc, String region) {
        idc2Region.put(idc, region);
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "OcpModel{" + "obServerAddrs=" + obServerAddrs + ", idc2Region=" + idc2Region + '}';
    }
}

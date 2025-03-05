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

import java.util.HashMap;
import java.util.List;

public class ConfigServerInfo {
    private String                  paramURL;
    private String                  localFile;                                 // read from local file
    private List<ObServerAddr>      rsList;
    private long                    clusterId  = -1;
    private HashMap<String, String> idc2Region = new HashMap<String, String>();

    public void setRsList(List<ObServerAddr> rsList) {
        this.rsList = rsList;
    }

    public void setClusterId(long clusterId) {
        this.clusterId = clusterId;
    }

    public void setParamURL(String paramURL) {
        this.paramURL = paramURL;
    }

    public void setLocalFile(String localFile) {
        this.localFile = localFile;
    }

    public String getParamURL() {
        return this.paramURL;
    }

    public String getLocalFile() {
        return this.localFile;
    }

    public long getClusterId() {
        return this.clusterId;
    }

    public List<ObServerAddr> getRsList() {
        return this.rsList;
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
        return "OcpModel{" + "obServerAddrs=" + rsList + ", idc2Region=" + idc2Region + '}';
    }
}

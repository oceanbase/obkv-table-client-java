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

import java.util.List;

public class OcpResponseData {
    private String                   ObRegion;
    private long                     ObRegionId = -1;
    private List<OcpResponseDataRs>  RsList;
    private List<OcpResponseDataIDC> IDCList;
    public OcpResponseData() {}

    /*
     * Get ob region.
     */
    public String getObRegion() {
        return ObRegion;
    }

    /*
     * Set ob region.
     */
    public void setObRegion(String obRegion) {
        ObRegion = obRegion;
    }

    /*
     * Get ob region id.
     */
    public long getObRegionId() {
        return ObRegionId;
    }

    /*
     * Set ob region id.
     */
    public void setObRegionId(long obRegionId) {
        ObRegionId = obRegionId;
    }

    /*
     * Get rs list.
     */
    public List<OcpResponseDataRs> getRsList() {
        return RsList;
    }

    /*
     * Set rs list.
     */
    public void setRsList(List<OcpResponseDataRs> rsList) {
        RsList = rsList;
    }

    /*
     * Get IDC list.
     */
    public List<OcpResponseDataIDC> getIDCList() {
        return IDCList;
    }

    /*
     * Set IDC list.
     */
    public void setIDCList(List<OcpResponseDataIDC> IDCList) {
        this.IDCList = IDCList;
    }

    /*
     * Validate.
     */
    public boolean validate() {
        return RsList != null && RsList.size() > 0;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "OcpResponseData{" + "ObRegion='" + ObRegion + '\'' + ", ObRegionId=" + ObRegionId
               + ", RsList=" + RsList + ", IDCList=" + IDCList + '}';
    }
}

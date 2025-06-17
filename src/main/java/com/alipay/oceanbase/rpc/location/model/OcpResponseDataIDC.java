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

/*
 * Idc->Region mapping defined in OCP.
 *
 */

public class OcpResponseDataIDC {
    private String idc;
    private String region;

    /*
     * Get idc.
     */
    public String getIdc() {
        return idc;
    }

    /*
     * Set idc.
     */
    public void setIdc(String idc) {
        this.idc = idc;
    }

    /*
     * Get region.
     */
    public String getRegion() {
        return region;
    }

    /*
     * Set region.
     */
    public void setRegion(String region) {
        this.region = region;
    }

    @Override
    public String toString() {
        return "OcpResponseDataIDC{" + "idc='" + idc + '\'' + ", region='" + region + '\'' + '}';
    }
}

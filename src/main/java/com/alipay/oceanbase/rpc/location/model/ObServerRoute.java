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

import java.util.HashSet;
import java.util.Set;

/**
 * ObServerRoute defines the route policy and server LDC for table operation request.
 * It also keeps the black list of servers which failed to access.
 *
 */
public class ObServerRoute {
    // default route for strong read.
    public static ObServerRoute STRONG_READ = new ObServerRoute(ObReadConsistency.STRONG);
    private ObReadConsistency   readConsistency;
    private ObRoutePolicy       readRoutePolicy;
    private boolean             isLdcUsed;
    // black ip list
    private Set<String>         blackIpList = new HashSet<String>();

    /*
     * Construct OB Server Route.
     */
    private ObServerRoute(ObReadConsistency readConsistency) {
        this(readConsistency, ObRoutePolicy.IDC_ORDER, false);
    }

    /*
     * Construct OB Server Route.
     */
    public ObServerRoute(ObReadConsistency readConsistency, ObRoutePolicy readRoutePolicy,
                         boolean isLdcUsed) {

        this.readConsistency = readConsistency;
        this.readRoutePolicy = readRoutePolicy;
        this.isLdcUsed = isLdcUsed;
    }

    /*
     * Get Consistency Level.
     */
    public ObReadConsistency getReadConsistency() {
        return readConsistency;
    }

    /*
     * Get read route policy.
     */
    public ObRoutePolicy getReadRoutePolicy() {
        return readRoutePolicy;
    }

    /*
     * Check whether LDC enabled.
     */
    public boolean isLdcEnabled() {
        return isLdcUsed;
    }

    /*
     * Reset route, which will clean the black ip list.
     */
    public void reset() {
        blackIpList.clear();
    }

    /*
     * Check whether ip is in black list.
     */
    public boolean isInBlackList(String ip) {
        return blackIpList.contains(ip);
    }

    /*
     * Add ip into black list.
     */
    public void addToBlackList(String ip) {
        blackIpList.add(ip);
    }

    /*
     * set blackIpList.
     */
    public void setBlackList(Set<String> blackIpList) {
        if (blackIpList != null) {
            this.blackIpList = blackIpList;
        }
    }
}

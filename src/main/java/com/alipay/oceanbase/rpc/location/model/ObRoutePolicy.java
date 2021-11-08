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
import java.util.Map;

/**
 * There are PRIMARY(P), FOLLOWER(F) and READONLY(R) follower types.
 * ObRoutePolicy defines the weak read routing policy.
 *
 */
public enum ObRoutePolicy {
    // read order: same IDC -> same region -> other region
    IDC_ORDER(0)
    // read order: F+R ->  P
    , FOLLOWER_FIRST(1)
    // read order: R -> F -> P
    // , READONLY_FIRST(2)
    // read from: R
    // , ONLY_READONLY(3)
    // read from: P + F
    // , ONLY_READWRITE(4)
    ;

    private int                                value;

    private static Map<Integer, ObRoutePolicy> map = new HashMap<Integer, ObRoutePolicy>();

    /**
     * Constructor.
     *
     * @param value
     */
    ObRoutePolicy(int value) {
        this.value = value;
    }

    static {
        for (ObRoutePolicy type : ObRoutePolicy.values()) {
            map.put(type.value, type);
        }
    }

    /**
     * Get ObRoutePolicy name, return "IDC_ORDER" by default.
     */
    static public ObRoutePolicy getByName(String routePolicy) {
        if (routePolicy != null) {
            if (routePolicy.equalsIgnoreCase("idc_order")) {
                return ObRoutePolicy.IDC_ORDER;
            } else if (routePolicy.equalsIgnoreCase("follower_first")) {
                return ObRoutePolicy.FOLLOWER_FIRST;
            } /*else if (routePolicy.equalsIgnoreCase("readonly_first")) {
                return ObRoutePolicy.READONLY_FIRST;
              } else if (routePolicy.equalsIgnoreCase("only_readonly")) {
                return ObRoutePolicy.ONLY_READONLY;
              } else if (routePolicy.equalsIgnoreCase("only_readwrite")) {
                return ObRoutePolicy.ONLY_READWRITE;
              }*/
        }
        return ObRoutePolicy.IDC_ORDER;
    }
}

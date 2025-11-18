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

public enum ObRoutePolicy {
    FOLLOWER_FIRST(0),
    FOLLOWER_ONLY(1);

    private int                                value;

    private static Map<Integer, ObRoutePolicy> map = new HashMap<Integer, ObRoutePolicy>();

    /*
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

    /*
     * Get ObRoutePolicy name.
     */
    static public ObRoutePolicy getByName(String routePolicy) throws IllegalArgumentException {
        if (routePolicy.equalsIgnoreCase("follower_first")) {
            return ObRoutePolicy.FOLLOWER_FIRST;
        } else if (routePolicy.equalsIgnoreCase("follower_only")) {
            return ObRoutePolicy.FOLLOWER_ONLY;
        } else {
            throw new IllegalArgumentException("routePolicy is invalid: " + routePolicy);
        }
    }
}

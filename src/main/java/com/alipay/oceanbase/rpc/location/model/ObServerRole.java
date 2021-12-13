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

public enum ObServerRole {
    INVALID_ROLE("INVALID_ROLE", 0), LEADER("LEADER", 1), FOLLOWER("FOLLOWER", 2);

    private String name;
    private int    index;

    private ObServerRole(String name, int index) {
        this.name = name;
        this.index = index;
    }

    static public ObServerRole getRole(int idx) {
        if (LEADER.index == idx) {
            return LEADER;
        } else if (FOLLOWER.index == idx) {
            return FOLLOWER;
        } else {
            return INVALID_ROLE;
        }
    }

    static public ObServerRole getRole(String role) {
        if (role.equalsIgnoreCase(LEADER.name)) {
            return LEADER;
        } else if (role.equalsIgnoreCase(FOLLOWER.name)) {
            return FOLLOWER;
        } else {
            return INVALID_ROLE;
        }
    }

    /*
     * Get name.
     */
    public String getName() {
        return name;
    }

    /*
     * Get index.
     */
    public int getIndex() {
        return index;
    }
}

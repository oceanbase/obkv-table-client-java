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

public class TableLocation {
    private List<ReplicaLocation> replicaLocations;

    /*
     * Get replica locations.
     */
    public List<ReplicaLocation> getReplicaLocations() {
        return replicaLocations;
    }

    /*
     * Set replica locations.
     */
    public void setReplicaLocations(List<ReplicaLocation> replicaLocations) {
        this.replicaLocations = replicaLocations;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "TableLocation{" + "replicaLocations=" + replicaLocations + '}';
    }
}

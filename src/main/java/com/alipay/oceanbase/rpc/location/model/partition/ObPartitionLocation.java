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

package com.alipay.oceanbase.rpc.location.model.partition;

import com.alipay.oceanbase.rpc.exception.ObTablePartitionNoMasterException;
import com.alipay.oceanbase.rpc.location.model.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ObPartitionLocation {
    private ReplicaLocation       leader;
    // all replicas including leader
    private List<ReplicaLocation> replicas    = new ArrayList<ReplicaLocation>(3);
    // LDC classification
    private List<ReplicaLocation> sameIdc     = new ArrayList<ReplicaLocation>();
    private List<ReplicaLocation> sameRegion  = new ArrayList<ReplicaLocation>();
    private List<ReplicaLocation> otherRegion = new ArrayList<ReplicaLocation>();

    /**
     * Get leader.
     */
    public ReplicaLocation getLeader() {
        if (leader == null) {
            // Previously, exception is thrown when get table meta with any no leader partition,
            // thus might prevent us from updating other partitions.
            // Now, put off the exception until we need to access the leader server instead.
            throw new ObTablePartitionNoMasterException("partition has no leader.");
        }
        return leader;
    }

    /**
     * Add replication
     *
     * @param replica
     */
    public void addReplicaLocation(ReplicaLocation replica) {
        if (replica.isLeader()) {
            this.leader = replica;
        }
        this.replicas.add(replica);
    }

    /**
     * Get replica according to route strategy.
     *
     * @param route
     * @return
     */
    public ReplicaLocation getReplica(ObServerRoute route) {
        // strong read : read leader
        if (route.getReadConsistency() == ObReadConsistency.STRONG) {
            return leader;
        }

        // weak read by LDC
        if (route.isLdcEnabled()) {
            return getReadReplicaByLDC(route);
        } else {
            return getReadReplicaNoLdc(route);
        }
    }

    /**
     * Get read replica according to LDC route strategy.
     *
     * @param route
     * @return
     */
    public ReplicaLocation getReadReplicaByLDC(ObServerRoute route) {
        if (route.getReadRoutePolicy() == ObRoutePolicy.FOLLOWER_FIRST) {
            if (!route.isInBlackList(leader.getIp())) {
                route.addToBlackList(leader.getIp());
            }
        }
        for (ReplicaLocation r : sameIdc) {
            if (!route.isInBlackList(r.getAddr().getIp())) {
                return r;
            }
        }
        for (ReplicaLocation r : sameRegion) {
            if (!route.isInBlackList(r.getAddr().getIp())) {
                return r;
            }
        }
        for (ReplicaLocation r : otherRegion) {
            if (!route.isInBlackList(r.getAddr().getIp())) {
                return r;
            }
        }
        return leader;
    }

    /**
     * Get read replica according to LDC route strategy.
     *
     * @param route
     * @return
     */
    public ReplicaLocation getReadReplicaNoLdc(ObServerRoute route) {
        for (ReplicaLocation r : replicas) {
            if (!route.isInBlackList(r.getIp())
                && !(r.isLeader() && route.getReadRoutePolicy() == ObRoutePolicy.FOLLOWER_FIRST)) {
                return r;
            }
        }
        return leader;
    }

    /**
     * Classify Replica for weak read, according to Server LDC location.
     * Synchronized to avoid duplicate initialization.
     *
     * @param ldcLocation
     */
    public void prepareForWeakRead(ObServerLdcLocation ldcLocation) {
        Collections.shuffle(replicas);
        if (ldcLocation != null && ldcLocation.isLdcUsed()) {
            for (ReplicaLocation replica : replicas) {
                if (ldcLocation.inSameIDC(replica.getIp())) {
                    sameIdc.add(replica);
                } else if (ldcLocation.inSameRegion(replica.getIp())) {
                    sameRegion.add(replica);
                } else {
                    otherRegion.add(replica);
                }
            }
        }
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return "ObPartitionLocation{" + "leader=" + leader + ", replicas=" + replicas
               + ", sameIdc=" + sameIdc + ", sameRegion=" + sameRegion + ", otherRegion="
               + otherRegion + '}';
    }
}

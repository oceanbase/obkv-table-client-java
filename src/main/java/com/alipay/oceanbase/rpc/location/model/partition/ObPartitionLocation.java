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

import com.alipay.oceanbase.rpc.location.model.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObReadConsistency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class ObPartitionLocation {
    private ReplicaLocation       leader;
    // all replicas including leader
    private List<ReplicaLocation> replicas    = new ArrayList<ReplicaLocation>(3);
    // LDC classification
    private List<ReplicaLocation> sameIdc     = new ArrayList<ReplicaLocation>();
    private List<ReplicaLocation> sameRegion  = new ArrayList<ReplicaLocation>();
    private List<ReplicaLocation> otherRegion = new ArrayList<ReplicaLocation>();

    /*
     * Get leader.
     */
    public ReplicaLocation getLeader() {
        return leader;
    }

    public List<ReplicaLocation> getReplicas() {
        return replicas;
    }

    public List<ReplicaLocation> getSameIdc() {
        return sameIdc;
    }

    public List<ReplicaLocation> getSameRegion() {
        return sameRegion;
    }

    public List<ReplicaLocation> getOtherRegion() {
        return otherRegion;
    }

    /*
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
     * Select a replica without excluding endpoints from previous attempts.
     *
     * @param consistencyLevel read consistency; null means strong read
     * @param routePolicy replica preference for weak reads
     * @return selected replica, or null if no replica can be selected
     * @throws IllegalArgumentException if FOLLOWER_ONLY has no valid follower
     */
    public ReplicaLocation getReplica(ObReadConsistency consistencyLevel,
                                      ObRoutePolicy routePolicy) throws IllegalArgumentException {
        return getReplica(consistencyLevel, routePolicy, null);
    }

    /**
     * Strong reads select the leader. Weak reads prefer valid, unfailed followers,
     * then allow an unfailed leader unless the policy is FOLLOWER_ONLY.
     * If no candidate remains, retry once without the failure filter under the same policy.
     *
     * @param consistencyLevel read consistency; null means strong read
     * @param routePolicy replica preference for weak reads
     * @param failedServers endpoints in IP:RPC-port form; null or empty disables filtering;
     *                      the set is not modified
     * @return selected replica, or null if no replica can be selected
     * @throws IllegalArgumentException if FOLLOWER_ONLY has no valid follower
     */
    public ReplicaLocation getReplica(ObReadConsistency consistencyLevel,
                                      ObRoutePolicy routePolicy, Set<String> failedServers) {
        // Strong reads must stay on the leader, even if it previously failed.
        if (consistencyLevel == null || consistencyLevel == ObReadConsistency.STRONG) {
            return leader;
        }
        ReplicaLocation replica = sameIdc.isEmpty() && sameRegion.isEmpty() && otherRegion.isEmpty()
            ? getRandomFollowerFromList(replicas, failedServers)
            : getReadReplicaByRoutePolicy(failedServers);
        if (replica != null) {
            return replica;
        }
        if (routePolicy != ObRoutePolicy.FOLLOWER_ONLY && leader != null
            && !isFailed(leader, failedServers)) {
            return leader;
        }
        // No unfailed candidate remains: ignore the filter once to allow recovery.
        if (failedServers != null && !failedServers.isEmpty()) {
            return getReplica(consistencyLevel, routePolicy, null);
        }
        if (routePolicy == ObRoutePolicy.FOLLOWER_ONLY) {
            throw new IllegalArgumentException("No follower replica found for route policy: "
                                               + routePolicy);
        }
        return leader;
    }

    /**
     * Select an unfailed follower by locality: same IDC, same region, then other regions.
     * Route-policy checks and leader fallback are handled by getReplica.
     *
     * @param failedServers endpoints in IP:RPC-port form; null or empty disables filtering
     * @return selected follower, or null if no valid, unfailed follower is available
     */
    private ReplicaLocation getReadReplicaByRoutePolicy(Set<String> failedServers) {
        ReplicaLocation follower = getRandomFollowerFromList(sameIdc, failedServers);
        if (follower == null) {
            follower = getRandomFollowerFromList(sameRegion, failedServers);
        }
        if (follower == null) {
            follower = getRandomFollowerFromList(otherRegion, failedServers);
        }
        return follower;
    }

    private boolean isFailed(ReplicaLocation replica, Set<String> failedServers) {
        return replica != null && failedServers != null
               && failedServers.contains(replica.getIp() + ":" + replica.getSvrPort());
    }

    /**
     * 从列表中随机选择一个有效且未被失败集合排除的 follower，避免热点问题
     *
     * @param locations replica 列表
     * @param failedServers IP:RPC端口形式的失败端点；null 或空集合表示不排除
     * @return 随机选择的 follower；没有满足条件的 follower 时返回 null
     */
    private ReplicaLocation getRandomFollowerFromList(List<ReplicaLocation> locations,
                                                        Set<String> failedServers) {
        if (locations == null || locations.isEmpty()) {
            return null;
        }
        
        // 收集所有有效且未被失败集合排除的 follower
        List<ReplicaLocation> validFollowers = new ArrayList<>();
        for (ReplicaLocation r : locations) {
            if (r.isValid() && !r.isLeader() && !isFailed(r, failedServers)) {
                validFollowers.add(r);
            }
        }
        
        if (validFollowers.isEmpty()) {
            return null;
        }
        
        // 随机选择一个 follower
        return validFollowers.get(ThreadLocalRandom.current().nextInt(validFollowers.size()));
    }

    /*
     * Classify Replica for weak read, according to Server LDC location.
     * Synchronized to avoid duplicate initialization.
     *
     * @param ldcLocation
     */
    public void prepareForWeakRead(ObServerLdcLocation ldcLocation) {
        Collections.shuffle(replicas);
        if (ldcLocation != null && ldcLocation.isLdcUsed()) {
            for (ReplicaLocation replica : replicas) {
                if (ldcLocation.inSameIDC(replica.getIp() + replica.getSvrPort())) {
                    sameIdc.add(replica);
                } else if (ldcLocation.inSameRegion(replica.getIp() + replica.getSvrPort())) {
                    sameRegion.add(replica);
                } else {
                    otherRegion.add(replica);
                }
            }
        }
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "ObPartitionLocation{" + "leader=" + leader + ", replicas=" + replicas
               + ", sameIdc=" + sameIdc + ", sameRegion=" + sameRegion + ", otherRegion="
               + otherRegion + '}';
    }
}

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

    /*
     * Get replica according to route strategy.
     *
     * @param route
     * @return
     */
    public ReplicaLocation getReplica(ObReadConsistency consistencyLevel,
                                      ObRoutePolicy routePolicy) throws IllegalArgumentException {
        // strong read : read leader
        if (consistencyLevel == null || consistencyLevel == ObReadConsistency.STRONG) {
            return leader;
        }

        // empty means not idc route
        if (sameIdc.isEmpty() && sameRegion.isEmpty() && otherRegion.isEmpty()) {
            return getReadReplicaNoLdc(routePolicy);
        }

        return getReadReplicaByRoutePolicy(routePolicy);
    }

    private ReplicaLocation getReadReplicaNoLdc(ObRoutePolicy routePolicy) {
        ReplicaLocation follower = getRandomFollowerFromList(replicas);
        if (follower != null) {
            return follower;
        }
        if (routePolicy == ObRoutePolicy.FOLLOWER_ONLY) {
            throw new IllegalArgumentException("No follower replica found for route policy: "
                                               + routePolicy);
        }
        return leader;
    }

    private ReplicaLocation getReadReplicaByRoutePolicy(ObRoutePolicy routePolicy)
                                                                                  throws IllegalArgumentException {
        // 路由策略优先：FOLLOWER_FIRST 优先选择 follower，FOLLOWER_ONLY 只能选择 follower
        // 在满足路由策略的前提下，按就近原则选择（同机房 -> 同 region -> 其他 region）

        // 优先在同机房找 follower（随机选择以避免热点）
        ReplicaLocation follower = getRandomFollowerFromList(sameIdc);
        if (follower != null) {
            return follower;
        }
        
        // 如果同机房没有 follower，在同 region 找 follower（随机选择以避免热点）
        follower = getRandomFollowerFromList(sameRegion);
        if (follower != null) {
            return follower;
        }
        
        // 如果同 region 没有 follower，在其他 region 找 follower（随机选择以避免热点）
        follower = getRandomFollowerFromList(otherRegion);
        if (follower != null) {
            return follower;
        }
        
        // 如果都没有找到 follower
        if (routePolicy == ObRoutePolicy.FOLLOWER_ONLY) {
            // FOLLOWER_ONLY 必须选择 follower，没有就抛出异常
            throw new IllegalArgumentException("No follower replica found for route policy: "
                                                + routePolicy);
        }

        // 如果都没有找到，返回 leader（兜底）
        return leader;
    }

    /**
     * 从列表中随机选择一个有效的 follower，避免热点问题
     *
     * @param locations replica 列表
     * @return 随机选择的 follower，如果没有有效的 follower 则返回 null
     */
    private ReplicaLocation getRandomFollowerFromList(List<ReplicaLocation> locations) {
        if (locations == null || locations.isEmpty()) {
            return null;
        }
        
        // 收集所有有效的 follower
        List<ReplicaLocation> validFollowers = new ArrayList<>();
        for (ReplicaLocation r : locations) {
            if (r.isValid() && !r.isLeader()) {
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

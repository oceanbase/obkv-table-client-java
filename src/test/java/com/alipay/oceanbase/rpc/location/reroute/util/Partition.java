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

package com.alipay.oceanbase.rpc.location.reroute.util;

import java.util.ArrayList;
import java.util.List;
public class Partition {
    private long tableId;
    private long partId;
    private long replicaNum;
    private Replica leader;
    private List<Replica> follower;

    public Partition(long partId) {
        tableId = 0;
        this.partId = partId;
        replicaNum = 0;
        leader = null;
        follower = new ArrayList<>(2);
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setPartId(long partId) {
        this.partId = partId;
    }

    public long getPartId() {
        return partId;
    }

    public void addReplicaNum(long num) {
        this.replicaNum += num;
    }

    public long getReplicaNum() {
        return replicaNum;
    }

    public void setLeader(Replica leader) {
        this.leader = leader;
    }

    public Replica getLeader() {
        return leader;
    }

    public void appendFollower(Replica follower) {
        this.follower.add(follower);
    }

    public List<Replica> getFollower() {
        return follower;
    }
}

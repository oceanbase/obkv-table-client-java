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

/**
 * ObReplicaType(副本类型)
 *
 */
public enum ObReplicaType {
    // 全能型副本：是paxos成员；有ssstore；有memstore
    REPLICA_TYPE_FULL("FULL", 0),

    // 备份型副本：是paxos成员；有ssstore；没有memstore
    // REPLICA_TYPE_BACKUP("BACKUP", 1),

    // 日志型副本: 是paxos成员；没有ssstore；没有memstore
    REPLICA_TYPE_LOGONLY("LOGONLY", 5),

    // 只读型副本：不是paxos成员；有ssstore；有memstore
    REPLICA_TYPE_READONLY("READONLY", 16),

    // 增量型副本：不是paxos成员；没有ssstore；有memstore
    // REPLICA_TYPE_MEMONLY("MEMONLY", 20),

    // invalid value
    REPLICA_TYPE_INVALID("INVALID", Integer.MAX_VALUE);

    private String name;
    private int    index;

    /*
     * Constructor.
     *
     * @param name
     * @param index
     */
    ObReplicaType(String name, int index) {
        this.name = name;
        this.index = index;
    }

    /*
     * Get ReplicaType by idx.
     *
     * @param idx
     * @return
     */
    static public ObReplicaType getReplicaType(int idx) {
        if (REPLICA_TYPE_FULL.index == idx) {
            return REPLICA_TYPE_FULL;
        } else if (REPLICA_TYPE_LOGONLY.index == idx) {
            return REPLICA_TYPE_LOGONLY;
        } else if (REPLICA_TYPE_READONLY.index == idx) {
            return REPLICA_TYPE_READONLY;
        } else {
            return REPLICA_TYPE_INVALID;
        }
    }

    /*
     * whether the replica is readable.
     */
    public boolean isReadable() {
        return this.index == REPLICA_TYPE_FULL.index || this.index == REPLICA_TYPE_READONLY.index;
    }

    /*
     * whether the replica is readonly.
     */
    public boolean isReadonly() {
        return this.index == REPLICA_TYPE_READONLY.index;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return this.name;
    }
}

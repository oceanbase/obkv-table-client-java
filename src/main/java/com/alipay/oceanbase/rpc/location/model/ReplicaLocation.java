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

public class ReplicaLocation {

    private ObServerAddr  addr;
    private ObServerInfo  info;
    private ObServerRole  role;
    private ObReplicaType replicaType;

    /*
     * Is valid.
     */
    public boolean isValid() {
        return (null != addr) && (ObServerRole.INVALID_ROLE != this.role) && (null != info)
               && (info.isActive());
    }

    /*
     * Get addr.
     */
    public ObServerAddr getAddr() {
        return addr;
    }

    /*
     * Set addr.
     */
    public void setAddr(ObServerAddr addr) {
        this.addr = addr;
    }

    /*
     * Get role.
     */
    public ObServerRole getRole() {
        return role;
    }

    /*
     * Set role.
     */
    public void setRole(ObServerRole role) {
        this.role = role;
    }

    /*
     * Is leader.
     */
    public boolean isLeader() {
        return ObServerRole.LEADER.equals(role);
    }

    /*
     * Get info.
     */
    public ObServerInfo getInfo() {
        return info;
    }

    /*
     * Set info.
     */
    public void setInfo(ObServerInfo info) {
        this.info = info;
    }

    /*
     * Get replicaType.
     */
    public ObReplicaType getReplicaType() {
        return replicaType;
    }

    /*
     * Set replicaType.
     */
    public void setReplicaType(ObReplicaType replicaType) {
        this.replicaType = replicaType;
    }

    /*
     * The replica is readable.
     */
    public boolean isReadable() {
        return replicaType.isReadable();
    }

    /*
     * The replica is readonly.
     */
    public boolean isReadonly() {
        return replicaType.isReadonly();
    }

    /*
     * Get Ip of the replica.
     */
    public String getIp() {
        return addr.getIp();
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "ReplicaLocation{" + "addr=" + addr + ", info=" + info + ", role=" + role
               + ", replicaType=" + replicaType + '}';
    }
}

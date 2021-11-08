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

import java.util.concurrent.atomic.AtomicInteger;

public class ObServerAddr implements Comparable<ObServerAddr> {

    private String              ip;
    private int                 sqlPort;
    private int                 svrPort;
    private final AtomicInteger priority          = new AtomicInteger(0);
    private volatile long       grantPriorityTime = 0;
    private volatile long       lastAccessTime    = System.currentTimeMillis();

    /**
     * Whether the addr is expired given the timeout.
     */
    public boolean isExpired(long cachingTimeout) {
        return System.currentTimeMillis() - lastAccessTime > cachingTimeout;
    }

    /**
     * Record the access time.
     */
    public void recordAccess() {
        lastAccessTime = System.currentTimeMillis();
    }

    /**
     * Get ip.
     */
    public String getIp() {
        return ip;
    }

    /**
     * Set ip.
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    /**
     * Set address.
     */
    public void setAddress(String address) {
        if (address.contains(":")) {
            this.ip = address.split(":")[0];
            this.svrPort = Integer.parseInt(address.split(":")[1]);
        } else {
            this.ip = address;
        }
    }

    /**
     * Get sql port.
     */
    public int getSqlPort() {
        return sqlPort;
    }

    /**
     * Set sql port.
     */
    public void setSqlPort(int sqlPort) {
        this.sqlPort = sqlPort;
    }

    /**
     * Get svr port.
     */
    public int getSvrPort() {
        return svrPort;
    }

    /**
     * Set svr port.
     */
    public void setSvrPort(int svrPort) {
        this.svrPort = svrPort;
    }

    /**
     * Get priority.
     */
    public AtomicInteger getPriority() {
        return priority;
    }

    /**
     * Get grant priority time.
     */
    public long getGrantPriorityTime() {
        return grantPriorityTime;
    }

    /**
     * Set grant priority time.
     */
    public void setGrantPriorityTime(long grantPriorityTime) {
        this.grantPriorityTime = grantPriorityTime;
    }

    public long getLastAccessTime() {
        return lastAccessTime;
    }

    /**
     * Equals.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ObServerAddr that = (ObServerAddr) o;
        return sqlPort == that.sqlPort && svrPort == that.svrPort && ip.equals(that.ip);
    }

    /**
     * Hash code.
     */
    @Override
    public int hashCode() {
        return ip.hashCode() + sqlPort + svrPort;
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return "ObServerAddr{" + "ip='" + ip + '\'' + ", sqlPort=" + sqlPort + ", svrPort="
               + svrPort + '}';
    }

    /**
     * Compare to.
     */
    @Override
    public int compareTo(ObServerAddr that) {
        int thisValue = this.priority.get();
        int thatValue = that.priority.get();
        return thisValue < thatValue ? 1 : (thisValue == thatValue) ? 0 : -1;
    }
}

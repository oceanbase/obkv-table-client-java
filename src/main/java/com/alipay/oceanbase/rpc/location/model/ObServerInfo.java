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

public class ObServerInfo {

    private long   stopTime;
    private String status;  // ACTIVE / INACTIVE / DELETING

    /*
     * Get status.
     */
    public String getStatus() {
        return status;
    }

    /*
     * Set status.
     */
    public void setStatus(String status) {
        this.status = status;
    }

    /*
     * Get stop time.
     */
    public long getStopTime() {
        return stopTime;
    }

    /*
     * Set stop time.
     */
    public void setStopTime(long stopTime) {
        this.stopTime = stopTime;
    }

    /*
     * Is active.
     */
    public boolean isActive() {
        return stopTime == 0 && "active".equalsIgnoreCase(status);
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "ObServerInfo{" + "stopTime=" + stopTime + ", status='" + status + '\'' + '}';
    }
}

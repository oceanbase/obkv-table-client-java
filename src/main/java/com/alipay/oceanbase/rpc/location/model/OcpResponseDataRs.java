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

public class OcpResponseDataRs {
    private String address;
    private String role;
    private int    sql_port;

    /*
     * Get address.
     */
    public String getAddress() {
        return address;
    }

    /*
     * Set address.
     */
    public void setAddress(String address) {
        this.address = address;
    }

    /*
     * Get role.
     */
    public String getRole() {
        return role;
    }

    /*
     * Set role.
     */
    public void setRole(String role) {
        this.role = role;
    }

    /*
     * Get sql_port.
     */
    public int getSql_port() {
        return sql_port;
    }

    /*
     * Set sql_port.
     */
    public void setSql_port(int sql_port) {
        this.sql_port = sql_port;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "OcpResponseDataRs{" + "address='" + address + '\'' + ", role='" + role + '\''
               + ", sql_port=" + sql_port + '}';
    }
}

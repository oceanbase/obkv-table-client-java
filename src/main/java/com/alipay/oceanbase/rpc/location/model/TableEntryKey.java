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

import com.alipay.oceanbase.rpc.constant.Constants;
import com.alipay.oceanbase.rpc.util.StringUtil;

public class TableEntryKey {

    private String clusterName  = Constants.EMPTY_STRING;
    private String tenantName   = Constants.EMPTY_STRING;
    private String databaseName = Constants.EMPTY_STRING;
    private String tableName    = Constants.EMPTY_STRING;

    /*
     * Table entry key.
     */
    public TableEntryKey() {
    }

    /*
     * Table entry key.
     */
    public TableEntryKey(String clusterName, String tenantName, String databaseName,
                         String tableName) {
        super();
        this.clusterName = clusterName;
        this.tenantName = tenantName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    /*
     * Get dummy entry key.
     */
    public static TableEntryKey getDummyEntryKey(String clusterName, String tenantName) {
        TableEntryKey dummyKey = new TableEntryKey(clusterName, tenantName,
            Constants.OCEANBASE_DATABASE, Constants.ALL_DUMMY_TABLE);
        return dummyKey;
    }

    /*
     * Get sys dummy entry key.
     */
    public static TableEntryKey getSysDummyEntryKey(String clusterName) {
        return getDummyEntryKey(clusterName, Constants.SYS_TENANT);
    }

    /*
     * Is sys all dummy.
     */
    public boolean isSysAllDummy() {
        return this.tenantName.equals(Constants.SYS_TENANT) && this.isAllDummy();
    }

    /*
     * Is all dummy.
     */
    public boolean isAllDummy() {
        return this.databaseName.equals(Constants.OCEANBASE_DATABASE)
               && this.tableName.equals(Constants.ALL_DUMMY_TABLE);
    }

    /*
     * Is valid.
     */
    public boolean isValid() {
        return StringUtil.isNotEmpty(this.clusterName) && StringUtil.isNotEmpty(this.tenantName)
               && StringUtil.isNotEmpty(this.databaseName) && StringUtil.isNotEmpty(this.tableName);
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "TableEntryKey [clusterName=" + clusterName + ", tenantName=" + tenantName
               + ", databaseName=" + databaseName + ", tableName=" + tableName + "]";
    }

    /*
     * Hash code.
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((clusterName == null) ? 0 : clusterName.hashCode());
        result = prime * result + ((databaseName == null) ? 0 : databaseName.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        result = prime * result + ((tenantName == null) ? 0 : tenantName.hashCode());
        return result;
    }

    /*
     * Equals.
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TableEntryKey other = (TableEntryKey) obj;
        if (clusterName == null) {
            if (other.clusterName != null)
                return false;
        } else if (!clusterName.equals(other.clusterName))
            return false;
        if (databaseName == null) {
            if (other.databaseName != null)
                return false;
        } else if (!databaseName.equals(other.databaseName))
            return false;
        if (tableName == null) {
            if (other.tableName != null)
                return false;
        } else if (!tableName.equals(other.tableName))
            return false;
        if (tenantName == null) {
            if (other.tenantName != null)
                return false;
        } else if (!tenantName.equals(other.tenantName))
            return false;

        return true;
    }

    /*
     * Get cluster name.
     */
    public String getClusterName() {
        return clusterName;
    }

    /*
     * Set cluster name.
     */
    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    /*
     * Get tenant name.
     */
    public String getTenantName() {
        return tenantName;
    }

    /*
     * Set tenant name.
     */
    public void setTenantName(String tenantName) {
        this.tenantName = tenantName;
    }

    /*
     * Get database name.
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /*
     * Set database name.
     */
    public void setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
    }

    /*
     * Get table name.
     */
    public String getTableName() {
        return tableName;
    }

    /*
     * Set table name.
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}

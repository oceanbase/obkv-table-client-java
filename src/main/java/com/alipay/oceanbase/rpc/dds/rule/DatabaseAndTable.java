/*-
* #%L
 * * OceanBase Table Client Framework
 * *
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
 * *
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

package com.alipay.oceanbase.rpc.dds.rule;

import java.util.Arrays;

/**
* @author zhiqi.zzq
* @since 2021/7/9 下午3:57
*/
public class DatabaseAndTable {

    private Integer  tableShardValue;
    private Integer  databaseShardValue;
    private Integer  elasticIndexValue;
    private String   tableName;

    private boolean  isReadOnly;
    private String[] rowKeyColumns;

    /**
    *
    * @return
    */
    public Integer getTableShardValue() {
        return tableShardValue;
    }

    /**
    *
    * @param tableShardValue
    */
    public void setTableShardValue(Integer tableShardValue) {
        this.tableShardValue = tableShardValue;
    }

    /**
    *
    * @return
    */
    public Integer getDatabaseShardValue() {
        return databaseShardValue;
    }

    /**
    *
    * @param databaseShardValue
    */
    public void setDatabaseShardValue(Integer databaseShardValue) {
        this.databaseShardValue = databaseShardValue;
    }

    /**
    *
    * @return
    */
    public Integer getElasticIndexValue() {
        return elasticIndexValue;
    }

    /**
    *
    * @param elasticIndexValue
    */
    public void setElasticIndexValue(Integer elasticIndexValue) {
        this.elasticIndexValue = elasticIndexValue;
    }

    /**
    *
    * @return
    */
    public String getTableName() {
        return tableName;
    }

    /**
    *
    * @param tableName
    */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /**
    *
    * @param that
    * @return
    */
    public boolean isInSameShard(DatabaseAndTable that) {
        if (getTableShardValue() != null ? !getTableShardValue().equals(that.getTableShardValue())
            : that.getTableShardValue() != null)
            return false;
        if (getDatabaseShardValue() != null ? !getDatabaseShardValue().equals(
            that.getDatabaseShardValue()) : that.getDatabaseShardValue() != null)
            return false;
        if (getElasticIndexValue() != null ? !getElasticIndexValue().equals(
            that.getElasticIndexValue()) : that.getElasticIndexValue() != null)
            return false;
        return getTableName() != null ? getTableName().equals(that.getTableName()) : that
            .getTableName() == null;
    }

    /**
    *
    * @param o
    * @return
    */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DatabaseAndTable))
            return false;

        DatabaseAndTable that = (DatabaseAndTable) o;

        if (isReadOnly() != that.isReadOnly())
            return false;
        if (getTableShardValue() != null ? !getTableShardValue().equals(that.getTableShardValue())
            : that.getTableShardValue() != null)
            return false;
        if (getDatabaseShardValue() != null ? !getDatabaseShardValue().equals(
            that.getDatabaseShardValue()) : that.getDatabaseShardValue() != null)
            return false;
        if (getElasticIndexValue() != null ? !getElasticIndexValue().equals(
            that.getElasticIndexValue()) : that.getElasticIndexValue() != null)
            return false;
        if (getTableName() != null ? !getTableName().equals(that.getTableName()) : that
            .getTableName() != null)
            return false;
        // Probably incorrect - comparing Object[] arrays with Arrays.equals
        return Arrays.equals(getRowKeyColumns(), that.getRowKeyColumns());
    }

    /**
    *
    * @return
    */
    public boolean isReadOnly() {
        return isReadOnly;
    }

    /**
    *
    * @param readOnly
    */
    public void setReadOnly(boolean readOnly) {
        isReadOnly = readOnly;
    }

    /**
    *
    * @return
    */
    public String[] getRowKeyColumns() {
        return rowKeyColumns;
    }

    /**
    *
    * @param rowKeyColumns
    */
    public void setRowKeyColumns(String[] rowKeyColumns) {
        this.rowKeyColumns = rowKeyColumns;
    }

    /**
    *
    * @return
    */
    @Override
    public int hashCode() {
        int result = getTableShardValue() != null ? getTableShardValue().hashCode() : 0;
        result = 31 * result
                 + (getDatabaseShardValue() != null ? getDatabaseShardValue().hashCode() : 0);
        result = 31 * result
                 + (getElasticIndexValue() != null ? getElasticIndexValue().hashCode() : 0);
        result = 31 * result + (getTableName() != null ? getTableName().hashCode() : 0);
        result = 31 * result + (isReadOnly() ? 1 : 0);
        result = 31 * result + Arrays.hashCode(getRowKeyColumns());
        return result;
    }

    /**
    *
    * @return
    */
    @Override
    public String toString() {
        return "DatabaseAndTable{" + "tableShardValue=" + tableShardValue + ", databaseShardValue="
               + databaseShardValue + ", elasticIndexValue=" + elasticIndexValue + ", tableName='"
               + tableName + '\'' + ", isReadOnly=" + isReadOnly + ", rowKeyColumns="
               + Arrays.toString(rowKeyColumns) + '}';
    }
}

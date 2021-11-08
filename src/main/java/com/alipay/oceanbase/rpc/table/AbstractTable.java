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

package com.alipay.oceanbase.rpc.table;

import com.alipay.oceanbase.rpc.property.AbstractPropertyAware;
import com.alipay.oceanbase.rpc.table.api.Table;

import java.util.Map;

public abstract class AbstractTable extends AbstractPropertyAware implements Table {

    public Map<String, Object> get(String tableName, Object rowkey, String[] columns)
                                                                                     throws Exception {
        return get(tableName, new Object[] { rowkey }, columns);
    }

    /**
     * Update.
     */
    public long update(String tableName, Object rowkey, String[] columns, Object[] values)
                                                                                          throws Exception {
        return update(tableName, new Object[] { rowkey }, columns, values);
    }

    /**
     * Delete.
     */
    public long delete(String tableName, Object rowkey) throws Exception {
        return delete(tableName, new Object[] { rowkey });
    }

    /**
     * Insert.
     */
    public long insert(String tableName, Object rowkey, String[] columns, Object[] values)
                                                                                          throws Exception {
        return insert(tableName, new Object[] { rowkey }, columns, values);
    }

    /**
     * Replace.
     */
    public long replace(String tableName, Object rowkey, String[] columns, Object[] values)
                                                                                           throws Exception {
        return replace(tableName, new Object[] { rowkey }, columns, values);
    }

    /**
     * Insert or update.
     */
    public long insertOrUpdate(String tableName, Object rowkey, String[] columns, Object[] values)
                                                                                                  throws Exception {
        return insertOrUpdate(tableName, new Object[] { rowkey }, columns, values);
    }

    public Map<String, Object> increment(String tableName, Object rowkey, String[] columns,
                                         Object[] values, boolean withResult) throws Exception {
        return increment(tableName, new Object[] { rowkey }, columns, values, withResult);
    }

    public Map<String, Object> append(String tableName, Object rowkey, String[] columns,
                                      Object[] values, boolean withResult) throws Exception {
        return append(tableName, new Object[] { rowkey }, columns, values, withResult);
    }

}

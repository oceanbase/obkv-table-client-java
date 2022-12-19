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

package com.alipay.oceanbase.rpc.table.api;

import com.alipay.oceanbase.rpc.mutation.*;

import java.util.Map;

public interface Table {

    void init() throws Exception;

    TableQuery query(String tableName) throws Exception;

    TableQuery queryByBatchV2(String tableName) throws Exception;

    TableQuery queryByBatch(String tableName) throws Exception;

    TableBatchOps batch(String tableName) throws Exception;

    Map<String, Object> get(String tableName, Object rowkey, String[] columns) throws Exception;

    Map<String, Object> get(String tableName, Object[] rowkeys, String[] columns) throws Exception;

    Update update(String tableName);

    long update(String tableName, Object rowkey, String[] columns, Object[] values)
                                                                                   throws Exception;

    long update(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                      throws Exception;

    Delete delete(String tableName);

    long delete(String tableName, Object rowkey) throws Exception;

    long delete(String tableName, Object[] rowkeys) throws Exception;

    public Insert insert(String tableName);

    long insert(String tableName, Object rowkey, String[] columns, Object[] values)
                                                                                   throws Exception;

    long insert(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                      throws Exception;

    Replace replace(String tableName);

    long replace(String tableName, Object rowkey, String[] columns, Object[] values)
                                                                                    throws Exception;

    long replace(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                       throws Exception;

    InsertOrUpdate insertOrUpdate(String tableName);

    long insertOrUpdate(String tableName, Object rowkey, String[] columns, Object[] values)
                                                                                           throws Exception;

    long insertOrUpdate(String tableName, Object[] rowkeys, String[] columns, Object[] values)
                                                                                              throws Exception;

    Increment increment(String tableName);

    Map<String, Object> increment(String tableName, Object rowkey, String[] columns,
                                  Object[] values, boolean withResult) throws Exception;

    Map<String, Object> increment(String tableName, Object[] rowkeys, String[] columns,
                                  Object[] values, boolean withResult) throws Exception;

    Append append(String tableName);

    Map<String, Object> append(String tableName, Object rowkey, String[] columns, Object[] values,
                               boolean withResult) throws Exception;

    Map<String, Object> append(String tableName, Object[] rowkeys, String[] columns,
                               Object[] values, boolean withResult) throws Exception;

    BatchOperation batchOperation(String tableName);

    void addProperty(String property, String value);

}

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

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;

import java.util.List;

public interface TableBatchOps {

    ObTableBatchOperation getObTableBatchOperation();

    String getTableName();

    void setAtomicOperation(boolean atomicOperation);

    boolean isAtomicOperation();

    void setEntityType(ObTableEntityType entityType);

    ObTableEntityType getEntityType();

    void get(Object rowkey, String[] columns);

    void get(Object[] rowkeys, String[] columns);

    void update(Object rowkey, String[] columns, Object[] values);

    void update(Object[] rowkeys, String[] columns, Object[] values);

    void delete(Object rowkey);

    void delete(Object[] rowkeys);

    void insert(Object rowkey, String[] columns, Object[] values);

    void insert(Object[] rowkeys, String[] columns, Object[] values);

    void replace(Object rowkey, String[] columns, Object[] values);

    void replace(Object[] rowkeys, String[] columns, Object[] values);

    void insertOrUpdate(Object rowkey, String[] columns, Object[] values);

    void insertOrUpdate(Object[] rowkeys, String[] columns, Object[] values);

    /**
     * increment the value
     * @param rowkey the primary key
     * @param columns the columns will be increment
     * @param values the value incremented
     * @param withResult whether the server will return entity. be careful that the param
     *                   will affect all the operation in the batch
     */
    void increment(Object rowkey, String[] columns, Object[] values, boolean withResult);

    /**
     * increment the value
     * @param rowkeys the primary key
     * @param columns the columns will be increment
     * @param values the value incremented
     * @param withResult whether the server will return entity. be careful that the param
     *                   will affect all the operation in the batch
     */
    void increment(Object[] rowkeys, String[] columns, Object[] values, boolean withResult);

    /**
     * append the value
     * @param rowkey the primary key
     * @param columns the columns will be increment
     * @param values the value incremented
     * @param withResult whether the server will return entity. be careful that the param
     *                   will affect all the operation in the batch
     */
    void append(Object rowkey, String[] columns, Object[] values, boolean withResult);

    /**
     * append the value
     * @param rowkeys the primary key
     * @param columns the columns will be increment
     * @param values the value incremented
     * @param withResult whether the server will return entity. be careful that the param
     *                   will affect all the operation in the batch
     */
    void append(Object[] rowkeys, String[] columns, Object[] values, boolean withResult);

    List<Object> execute() throws Exception;

    void clear();

}

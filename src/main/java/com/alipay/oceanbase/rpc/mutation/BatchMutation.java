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

package com.alipay.oceanbase.rpc.mutation;

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.result.AppendResult;
import com.alipay.oceanbase.rpc.mutation.result.BatchMutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchMutation {
    private String tableName;
    private Table client;
    boolean withResult;
    private List<Mutation> mutations;

    /*
     * default constructor
     */
    public BatchMutation() {
        tableName = null;
        client = null;
        withResult = false;
        mutations = new ArrayList<>();
    }

    /*
     * construct with client and table name
     */
    public BatchMutation(Table client, String tableName) {
        this.tableName = tableName;
        this.client = client;
        withResult = false;
        mutations = new ArrayList<>();
    }

    /*
     * set client
     */
    public BatchMutation setClient(Table client) {
        this.client = client;
        return this;
    }

    /*
     * set table
     */
    public BatchMutation setTable(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /*
     * add mutations
     */
    public BatchMutation addMutation(Mutation... mutations) {
        this.mutations.addAll(Arrays.asList(mutations));
        return this;
    }


    public BatchMutationResult execute() throws Exception {
        TableBatchOps batchOps = client.batch(tableName);

        for (Mutation mutation : mutations) {
            ObTableOperationType type = mutation.getOperationType();
            switch (type) {
                case GET:
                    throw new IllegalArgumentException("Invalid type in batch operation, " + type);
                case INSERT:
                    batchOps.insert(mutation.getRowKeys(),
                            ((Insert) mutation).getColumns(),
                            ((Insert) mutation).getValues());
                    break;
                case DEL:
                    batchOps.delete(mutation.getRowKeys());
                    break;
                case UPDATE:
                    batchOps.update(mutation.getRowKeys(),
                            ((Update) mutation).getColumns(),
                            ((Update) mutation).getValues());
                    break;
                case INSERT_OR_UPDATE:
                    batchOps.insertOrUpdate(mutation.getRowKeys(),
                            ((InsertOrUpdate) mutation).getColumns(),
                            ((InsertOrUpdate) mutation).getValues());
                    break;
                case REPLACE:
                    batchOps.replace(mutation.getRowKeys(),
                            ((Replace) mutation).getColumns(),
                            ((Replace) mutation).getValues());
                    break;
                case INCREMENT:
                    batchOps.increment(mutation.getRowKeys(),
                            ((Increment) mutation).getColumns(),
                            ((Increment) mutation).getValues(),
                            withResult);
                    break;
                case APPEND:
                    batchOps.append(mutation.getRowKeys(),
                            ((Append) mutation).getColumns(),
                            ((Append) mutation).getValues(),
                            withResult);
                    break;
                default:
                    throw new ObTableException("unknown operation type " + type);
            }
        }

        return new BatchMutationResult(batchOps.executeWithResult());
    }
}

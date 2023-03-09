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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchOperation {
    private String       tableName;
    private Table        client;
    boolean              withResult;
    private List<Object> operations;
    boolean              isAtomic = true;

    /*
     * default constructor
     */
    public BatchOperation() {
        tableName = null;
        client = null;
        withResult = false;
        operations = new ArrayList<>();
    }

    /*
     * construct with client and table name
     */
    public BatchOperation(Table client, String tableName) {
        this.tableName = tableName;
        this.client = client;
        withResult = false;
        operations = new ArrayList<>();
    }

    /*
     * set client
     */
    public BatchOperation setClient(Table client) {
        this.client = client;
        return this;
    }

    /*
     * set table
     */
    public BatchOperation setTable(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /*
     * add queries
     */
    public BatchOperation addOperation(TableQuery... queries) {
        this.operations.addAll(Arrays.asList(queries));
        return this;
    }

    /*
     * add mutations
     */
    public BatchOperation addOperation(Mutation... mutations) {
        this.operations.addAll(Arrays.asList(mutations));
        return this;
    }

    /*
     * add mutations
     */
    public BatchOperation addOperation(List<Mutation> mutations) {
        this.operations.addAll(mutations);
        return this;
    }

    public BatchOperation setIsAtomic(boolean isAtomic) {
        this.isAtomic = isAtomic;
        return this;
    }

    public BatchOperationResult execute() throws Exception {
        // add rowkeyElement
        boolean hasSetRowkeyElement = false;
        TableBatchOps batchOps = client.batch(tableName);

        for (Object operation : operations) {
            if (operation instanceof Mutation) {
                Mutation mutation = (Mutation) operation;
                if (!hasSetRowkeyElement && mutation.getRowKeyNames() != null) {
                    ((ObTableClient) client).addRowKeyElement(tableName, (String[]) mutation
                        .getRowKeyNames().toArray(new String[0]));
                    hasSetRowkeyElement = true;
                }
                ObTableOperationType type = mutation.getOperationType();
                switch (type) {
                    case GET:
                        throw new IllegalArgumentException("Invalid type in batch operation, "
                                                           + type);
                    case INSERT:
                        ((Insert) mutation).removeRowkeyFromMutateColval();
                        batchOps.insert(mutation.getRowKey(), ((Insert) mutation).getColumns(),
                            ((Insert) mutation).getValues());
                        break;
                    case DEL:
                        batchOps.delete(mutation.getRowKey());
                        break;
                    case UPDATE:
                        ((Update) mutation).removeRowkeyFromMutateColval();
                        batchOps.update(mutation.getRowKey(), ((Update) mutation).getColumns(),
                            ((Update) mutation).getValues());
                        break;
                    case INSERT_OR_UPDATE:
                        ((InsertOrUpdate) mutation).removeRowkeyFromMutateColval();
                        batchOps.insertOrUpdate(mutation.getRowKey(),
                            ((InsertOrUpdate) mutation).getColumns(),
                            ((InsertOrUpdate) mutation).getValues());
                        break;
                    case REPLACE:
                        ((Replace) mutation).removeRowkeyFromMutateColval();
                        batchOps.replace(mutation.getRowKey(), ((Replace) mutation).getColumns(),
                            ((Replace) mutation).getValues());
                        break;
                    case INCREMENT:
                        ((Increment) mutation).removeRowkeyFromMutateColval();
                        batchOps.increment(mutation.getRowKey(),
                            ((Increment) mutation).getColumns(),
                            ((Increment) mutation).getValues(), withResult);
                        break;
                    case APPEND:
                        ((Append) mutation).removeRowkeyFromMutateColval();
                        batchOps.append(mutation.getRowKey(), ((Append) mutation).getColumns(),
                            ((Append) mutation).getValues(), withResult);
                        break;
                    default:
                        throw new ObTableException("unknown operation type " + type);
                }
            } else if (operation instanceof TableQuery) {
                TableQuery query = (TableQuery) operation;
                batchOps.get(query.getRowKey().getValues(),
                    query.getSelectColumns().toArray((new String[0])));
            } else {
                throw new ObTableException("unknown operation " + operation);
            }
        }
        batchOps.setAtomicOperation(isAtomic);
        return new BatchOperationResult(batchOps.executeWithResult());
    }
}

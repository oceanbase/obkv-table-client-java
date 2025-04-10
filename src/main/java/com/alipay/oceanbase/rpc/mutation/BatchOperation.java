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
import com.alipay.oceanbase.rpc.checkandmutate.CheckAndInsUp;
import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.get.Get;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutate;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.queryandmutate.QueryAndMutate;
import com.alipay.oceanbase.rpc.table.ObTableClientLSBatchOpsImpl;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.ObGlobal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BatchOperation {
    private String              tableName;
    private Table               client;
    boolean                     withResult;
    private List<Object>        operations;
    boolean                     isAtomic         = false;
    boolean                     returnOneResult  = false;
    boolean                     hasCheckAndInsUp = false;
    boolean                     hasGet           = false;
    ObTableOperationType        lastType         = ObTableOperationType.INVALID;
    boolean                     isSameType       = true;
    protected ObTableEntityType entityType       = ObTableEntityType.KV;

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
        boolean isHBaseQuery = false;
        if (queries != null && queries.length > 0) {
            isHBaseQuery = queries[0].getObTableQuery().isHbaseQuery();
        }
        if (isHBaseQuery) {
            if (isSameType && lastType != ObTableOperationType.INVALID
                && lastType != ObTableOperationType.SCAN) {
                isSameType = false;
            }
            lastType = ObTableOperationType.SCAN;
        } else {
            if (isSameType && lastType != ObTableOperationType.INVALID
                && lastType != ObTableOperationType.GET) {
                isSameType = false;
            }
            lastType = ObTableOperationType.GET;
        }
        this.operations.addAll(Arrays.asList(queries));
        return this;
    }

    public BatchOperation addOperation(QueryAndMutate qm) {
        lastType = ObTableOperationType.QUERY_AND_MUTATE;
        this.operations.add(qm);
        return this;
    }

    /*
     * add get
     */
    public BatchOperation addOperation(Get... gets) {
        if (isSameType && lastType != ObTableOperationType.INVALID
            && lastType != ObTableOperationType.GET) {
            isSameType = false;
        }

        lastType = ObTableOperationType.GET;
        this.operations.addAll(Arrays.asList(gets));
        return this;
    }

    /*
     * add mutations
     */
    public BatchOperation addOperation(Mutation... mutations) {
        for (int i = 0; i < mutations.length; i++) {
            if (isSameType && lastType != ObTableOperationType.INVALID
                && lastType != mutations[i].getOperationType()) {
                isSameType = false;
            }
            lastType = mutations[i].getOperationType();
        }
        this.operations.addAll(Arrays.asList(mutations));
        return this;
    }

    /*
     * add mutations
     */
    public BatchOperation addOperation(List<Mutation> mutations) {
        for (int i = 0; i < mutations.size(); i++) {
            if (isSameType && lastType != ObTableOperationType.INVALID
                && lastType != mutations.get(i).getOperationType()) {
                isSameType = false;
            }
            lastType = mutations.get(i).getOperationType();
        }
        this.operations.addAll(mutations);
        return this;
    }

    /*
     * add CheckAndInsUp
     */
    public BatchOperation addOperation(CheckAndInsUp... insUps) {
        if (isSameType && lastType != ObTableOperationType.INVALID
            && lastType != ObTableOperationType.CHECK_AND_INSERT_UP) {
            isSameType = false;
        }
        lastType = ObTableOperationType.CHECK_AND_INSERT_UP;
        this.operations.addAll(Arrays.asList(insUps));
        this.hasCheckAndInsUp = true;
        return this;
    }

    public void setEntityType(ObTableEntityType entityType) {
        this.entityType = entityType;
    }

    public BatchOperation setIsAtomic(boolean isAtomic) {
        this.isAtomic = isAtomic;
        return this;
    }

    public BatchOperation setReturnOneResult(boolean returnOneResult) {
        this.returnOneResult = returnOneResult;
        return this;
    }

    @SuppressWarnings("unchecked")
    public BatchOperationResult execute() throws Exception {
        if (returnOneResult && !ObGlobal.isReturnOneResultSupport()) {
            throw new FeatureNotSupportedException(
                "returnOneResult is not supported in this Observer version ["
                        + ObGlobal.obVsnString() + "]");
        } else if (returnOneResult
                   && !(isSameType && (lastType == ObTableOperationType.INSERT
                                       || lastType == ObTableOperationType.PUT
                                       || lastType == ObTableOperationType.REPLACE || lastType == ObTableOperationType.DEL))) {
            throw new IllegalArgumentException(
                "returnOneResult only support multi-insert/put/replace/del");
        }

        if (hasCheckAndInsUp || ObGlobal.isLsOpSupport()) {
            return executeWithLSBatchOp();
        } else {
            return executeWithNormalBatchOp();
        }
    }

    private BatchOperationResult executeWithNormalBatchOp() throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        TableBatchOps batchOps = client.batch(tableName);
        batchOps.setEntityType(entityType);
        boolean hasSetRowkeyElement = false;

        for (Object operation : operations) {
            if (operation instanceof Mutation) {
                Mutation mutation = (Mutation) operation;
                if (!hasSetRowkeyElement && mutation.getRowKeyNames() != null) {
                    List<String> rowKeyNames = mutation.getRowKeyNames();
                    ((ObTableClient) client).addRowKeyElement(tableName,
                        rowKeyNames.toArray(new String[0]));
                    hasSetRowkeyElement = true;
                }
                ObTableOperationType type = mutation.getOperationType();
                switch (type) {
                    case GET:
                        throw new IllegalArgumentException("Invalid type in batch operation, "
                                                           + type);
                    case INSERT:
                        ((Insert) mutation).removeRowkeyFromMutateColval();
                        batchOps.insert(((Insert) mutation).getRowKeyValues()
                            .toArray(new Object[0]), ((Insert) mutation).getColumns(),
                            ((Insert) mutation).getValues());
                        break;
                    case DEL:
                        batchOps.delete(((Delete) mutation).getRowKeyValues()
                            .toArray(new Object[0]));
                        break;
                    case UPDATE:
                        ((Update) mutation).removeRowkeyFromMutateColval();
                        batchOps.update(((Update) mutation).getRowKeyValues()
                            .toArray(new Object[0]), ((Update) mutation).getColumns(),
                            ((Update) mutation).getValues());
                        break;
                    case INSERT_OR_UPDATE:
                        ((InsertOrUpdate) mutation).removeRowkeyFromMutateColval();
                        batchOps.insertOrUpdate(((InsertOrUpdate) mutation).getRowKeyValues()
                            .toArray(new Object[0]), ((InsertOrUpdate) mutation).getColumns(),
                            ((InsertOrUpdate) mutation).getValues());
                        break;
                    case REPLACE:
                        ((Replace) mutation).removeRowkeyFromMutateColval();
                        batchOps.replace(
                            ((Replace) mutation).getRowKeyValues().toArray(new Object[0]),
                            ((Replace) mutation).getColumns(), ((Replace) mutation).getValues());
                        break;
                    case INCREMENT:
                        ((Increment) mutation).removeRowkeyFromMutateColval();
                        batchOps.increment(
                            ((Increment) mutation).getRowKeyValues().toArray(new Object[0]),
                            ((Increment) mutation).getColumns(),
                            ((Increment) mutation).getValues(), withResult);
                        break;
                    case APPEND:
                        ((Append) mutation).removeRowkeyFromMutateColval();
                        batchOps.append(((Append) mutation).getRowKeyValues()
                            .toArray(new Object[0]), ((Append) mutation).getColumns(),
                            ((Append) mutation).getValues(), withResult);
                        break;
                    case PUT:
                        ((Put) mutation).removeRowkeyFromMutateColval();
                        batchOps.put(((Put) mutation).getRowKeyValues().toArray(new Object[0]),
                            ((Put) mutation).getColumns(), ((Put) mutation).getValues());
                        break;
                    default:
                        throw new ObTableException("unknown operation type " + type);
                }
            } else if (operation instanceof TableQuery) {
                TableQuery query = (TableQuery) operation;
                batchOps.get(query.getRowKey().getValues(),
                    query.getSelectColumns().toArray((new String[0])));
            } else if (operation instanceof Get) {
                Get get = (Get) operation;
                if (get.getRowKey() == null) {
                    throw new IllegalArgumentException("RowKey is null in Get operation");
                }
                batchOps.get(get.getRowKey().getValues(), get.getSelectColumns());
            } else {
                throw new ObTableException("unknown operation " + operation);
            }
        }
        batchOps.setAtomicOperation(isAtomic);
        batchOps.setReturnOneResult(returnOneResult);
        return new BatchOperationResult(batchOps.executeWithResult());
    }

    private BatchOperationResult executeWithLSBatchOp() throws Exception {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        ObTableClientLSBatchOpsImpl batchOps;
        boolean hasSetRowkeyElement = false;
        int checkAndInsUpCnt = 0;

        if (client instanceof ObTableClient) {
            batchOps = new ObTableClientLSBatchOpsImpl(tableName, (ObTableClient) client);
            batchOps.setEntityType(entityType);
            for (Object operation : operations) {
                if (operation instanceof CheckAndInsUp) {
                    checkAndInsUpCnt++;
                    CheckAndInsUp checkAndInsUp = (CheckAndInsUp) operation;
                    batchOps.addOperation(checkAndInsUp);
                    List<String> rowKeyNames = checkAndInsUp.getInsUp().getRowKeyNames();
                    if (!hasSetRowkeyElement && rowKeyNames != null) {
                        ((ObTableClient) client).addRowKeyElement(tableName,
                            rowKeyNames.toArray(new String[0]));
                        hasSetRowkeyElement = true;
                    }
                } else if (operation instanceof Mutation) {
                    Mutation mutation = (Mutation) operation;
                    if (((ObTableClient) client).getRunningMode() == ObTableClient.RunningMode.HBASE) {
                        negateHbaseTimestamp(mutation);
                    }
                    batchOps.addOperation(mutation);
                    if (!hasSetRowkeyElement && mutation.getRowKeyNames() != null) {
                        List<String> rowKeyNames = mutation.getRowKeyNames();
                        ((ObTableClient) client).addRowKeyElement(tableName,
                            rowKeyNames.toArray(new String[0]));
                        hasSetRowkeyElement = true;
                    }
                } else if (operation instanceof Get) {
                    Get get = (Get) operation;
                    if (get.getRowKey() == null) {
                        throw new IllegalArgumentException("RowKey is null in Get operation");
                    }
                    batchOps.addOperation(get);
                } else if (operation instanceof TableQuery) {
                    TableQuery query = (TableQuery) operation;
                    batchOps.addOperation(query);
                } else if (operation instanceof QueryAndMutate) {
                    QueryAndMutate qm = (QueryAndMutate) operation;
                    batchOps.addOperation(qm);
                } else {
                    throw new IllegalArgumentException(
                        "The operations in batch must be all checkAndInsUp or all non-checkAndInsUp");
                }
            }
        } else {
            throw new IllegalArgumentException(
                "execute batch using ObTable directly is not supported");
        }

        if (checkAndInsUpCnt > 0 && checkAndInsUpCnt != operations.size()) {
            throw new IllegalArgumentException(
                "Can not mix checkAndInsUP and other types operation in batch");
        }

        batchOps.setReturningAffectedEntity(withResult);
        batchOps.setReturnOneResult(returnOneResult);
        batchOps.setAtomicOperation(isAtomic);
        return new BatchOperationResult(batchOps.executeWithResult());
    }

    private void negateHbaseTimestamp(Mutation mutation) {
        Row rowKey = mutation.getRowKey();
        if (rowKey == null || rowKey.size() != 3) {
            throw new IllegalArgumentException("hbase rowkey length must be 3");
        } else {
            long ts = (long) ObTableClient.getRowKeyValue(mutation, 2);
            ObTableClient.setRowKeyValue(mutation, 2, -ts);
        }
    }
}

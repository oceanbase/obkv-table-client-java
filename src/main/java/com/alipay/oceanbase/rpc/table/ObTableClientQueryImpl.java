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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.MonitorUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObTableClientQueryImpl extends AbstractTableQueryImpl {

    private String                                tableName;
    private final ObTableClient                   obTableClient;
    private Map<Long, ObPair<Long, ObTableParam>> partitionObTables;

    private Row                                   rowKey;           // only used by BatchOperation

    /*
     * Add aggregation.
     */
    public void addAggregation(ObTableAggregationType aggType, String aggColumn) {
        this.tableQuery.addAggregation(aggType, aggColumn);
    }

    /*
     * Ob table client query impl construct only with tableName
     */
    public ObTableClientQueryImpl() {
        this.tableName = null;
        this.indexTableName = null;
        this.obTableClient = null;
        this.tableQuery = new ObTableQuery();
        this.rowKey = null;
    }

    /*
     * Ob table client query impl.
     */
    public ObTableClientQueryImpl(String tableName, ObTableClient client) {
        this.tableName = tableName;
        this.indexTableName = tableName;
        this.obTableClient = client;
        this.tableQuery = new ObTableQuery();
        this.rowKey = null;
    }

    /*
     * Ob table client query impl.
     */
    public ObTableClientQueryImpl(String tableName, ObTableQuery tableQuery, ObTableClient client) {
        this.tableName = tableName;
        this.indexTableName = tableName;
        this.obTableClient = client;
        this.tableQuery = tableQuery;
        this.rowKey = null;
    }

    /*
     * Execute.
     */
    @Override
    public QueryResultSet execute() throws Exception {
        return new QueryResultSet(executeInternal());
    }

    /*
     * Execute.
     */
    @Override
    public QueryResultSet asyncExecute() throws Exception {
        return new QueryResultSet(asyncExecuteInternal());
    }

    /**
     * 只有 limit query 需要，其他不需要
     * @param keys keys want to set
     * @return table query
     */
    @Override
    public TableQuery setKeys(String... keys) {
        throw new IllegalArgumentException("Not needed");
    }

    /*
     * set row key of query (only used by BatchOperation)
     */
    public TableQuery setRowKey(Row rowKey) throws Exception {
        this.rowKey = rowKey;
        return this;
    }

    /*
     * check argument before execution
     */
    public void checkArgumentBeforeExec() throws Exception {
        if (null == obTableClient) {
            throw new ObTableException("table client is null");
        } else if (tableQuery.getLimit() < 0 && tableQuery.getOffset() > 0) {
            throw new ObTableException("offset can not be use without limit");
        } else if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
    }

    /*
     * Set parameter into request
     */
    private void setCommonParams2Result(AbstractQueryStreamResult result) throws Exception {
        result.setTableQuery(tableQuery);
        result.setEntityType(entityType);
        result.setTableName(tableName);
        result.setIndexTableName(indexTableName);
        result.setExpectant(partitionObTables);
        result.setOperationTimeout(operationTimeout);
        result.setReadConsistency(obTableClient.getReadConsistency());
    }

    private abstract static class InitQueryResultCallback<T> {
        abstract T execute() throws Exception;
    }

    private AbstractQueryStreamResult commonExecute(InitQueryResultCallback<AbstractQueryStreamResult> callable)
                                                                                                                throws Exception {
        checkArgumentBeforeExec();

        final long startTime = System.currentTimeMillis();
        this.partitionObTables = new HashMap<Long, ObPair<Long, ObTableParam>>(); // partitionObTables -> Map<logicId, Pair<logicId, param>>

        // fill a whole range if no range is added explicitly.
        if (tableQuery.getKeyRanges().isEmpty()) {
            tableQuery.addKeyRange(ObNewRange.getWholeRange());
        }

        // init partitionObTables
        if (obTableClient.isOdpMode()) {
            if (tableQuery.getScanRangeColumns().isEmpty()) {
                if (tableQuery.getIndexName() != null
                    && !tableQuery.getIndexName().equalsIgnoreCase("primary")) {
                    throw new ObTableException("key range columns must be specified when use index");
                }
            }
            this.partitionObTables.put(0L, new ObPair<Long, ObTableParam>(0L, new ObTableParam(
                obTableClient.getOdpTable())));
        } else {
            initPartitions();
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<Long, ObPair<Long, ObTableParam>> entry : this.partitionObTables.entrySet()) {
            stringBuilder.append("#").append(entry.getValue().getRight().getObTable().getIp())
                .append(":").append(entry.getValue().getRight().getObTable().getPort());
        }
        String endpoint = stringBuilder.toString();
        long getTableTime = System.currentTimeMillis();

        // defend aggregation of multiple partitions.
        if (tableQuery.isAggregation()) {
            if (this.partitionObTables.size() > 1) {
                throw new ObTableException(
                    "Not supported aggregate of multiple partitions, the partition size is: "
                            + this.partitionObTables.size(), ResultCodes.OB_NOT_SUPPORTED.errorCode);
            }
        }

        // set correct table group name for hbase
        if (tableQuery.isHbaseQuery()
            && obTableClient.getTableGroupInverted().containsKey(tableName)
            && tableName.equalsIgnoreCase(obTableClient.getTableGroupCache().get(
                obTableClient.getTableGroupInverted().get(tableName)))) {
            tableName = obTableClient.getTableGroupInverted().get(tableName);
        }

        // init query stream result
        AbstractQueryStreamResult streamResult = callable.execute();

        MonitorUtil
            .info((ObPayload) streamResult, obTableClient.getDatabase(), tableName, "QUERY",
                endpoint, tableQuery, streamResult, getTableTime - startTime,
                System.currentTimeMillis() - getTableTime,
                obTableClient.getslowQueryMonitorThreshold());

        return streamResult;
    }

    /*
     * Execute internal.
     */
    public ObTableClientQueryStreamResult executeInternal() throws Exception {
        return (ObTableClientQueryStreamResult) commonExecute(new InitQueryResultCallback<AbstractQueryStreamResult>() {
            @Override
            ObTableClientQueryStreamResult execute() throws Exception {
                ObTableClientQueryStreamResult obTableClientQueryStreamResult = new ObTableClientQueryStreamResult();
                setCommonParams2Result(obTableClientQueryStreamResult);
                obTableClientQueryStreamResult.setClient(obTableClient);
                obTableClientQueryStreamResult.init();
                return obTableClientQueryStreamResult;
            }
        });
    }

    public ObTableClientQueryAsyncStreamResult asyncExecuteInternal() throws Exception {
        return (ObTableClientQueryAsyncStreamResult) commonExecute(new InitQueryResultCallback<AbstractQueryStreamResult>() {
            @Override
            ObTableClientQueryAsyncStreamResult execute() throws Exception {
                ObTableClientQueryAsyncStreamResult obTableClientQueryAsyncStreamResult = new ObTableClientQueryAsyncStreamResult();
                setCommonParams2Result(obTableClientQueryAsyncStreamResult);
                obTableClientQueryAsyncStreamResult.setClient(obTableClient);
                obTableClientQueryAsyncStreamResult.init();
                return obTableClientQueryAsyncStreamResult;
            }
        });
    }

    /*
     * Init partition tables involved in this query
     */
    public void initPartitions() throws Exception {
        String indexName = tableQuery.getIndexName();
        if (!this.obTableClient.isOdpMode()) {
            indexTableName = obTableClient.getIndexTableName(tableName, indexName,
                tableQuery.getScanRangeColumns(), false);
        }

        for (ObNewRange rang : this.tableQuery.getKeyRanges()) {
            ObRowKey startKey = rang.getStartKey();
            int startKeySize = startKey.getObjs().size();
            ObRowKey endKey = rang.getEndKey();
            int endKeySize = endKey.getObjs().size();
            Object[] start = new Object[startKeySize];
            Object[] end = new Object[endKeySize];
            for (int i = 0; i < startKeySize; i++) {
                if (startKey.getObj(i).isMinObj() || startKey.getObj(i).isMaxObj()) {
                    start[i] = startKey.getObj(i);
                } else {
                    start[i] = startKey.getObj(i).getValue();
                }
            }
            for (int i = 0; i < endKeySize; i++) {
                if (endKey.getObj(i).isMinObj() || endKey.getObj(i).isMaxObj()) {
                    end[i] = endKey.getObj(i);
                } else {
                    end[i] = endKey.getObj(i).getValue();
                }
            }
            ObBorderFlag borderFlag = rang.getBorderFlag();
            // pairs -> List<Pair<logicId, param>>
            List<ObPair<Long, ObTableParam>> pairs = this.obTableClient.getTables(indexTableName,
                    tableQuery, start, borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd(),
                    false, false);
            for (ObPair<Long, ObTableParam> pair : pairs) {
                this.partitionObTables.put(pair.getLeft(), pair);
            }
        }
    }

    /*
     * Clear.
     */
    @Override
    public void clear() {
        this.tableQuery = new ObTableQuery();
    }

    /*
     * Get ob table query.
     */
    @Override
    public ObTableQuery getObTableQuery() {
        return tableQuery;
    }

    /*
     * Get table name.
     */
    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tabName) {
        this.tableName = tabName;
    }

    /*
     * Get row key (used by BatchOperation)
     */
    public Row getRowKey() {
        return this.rowKey;
    }
}

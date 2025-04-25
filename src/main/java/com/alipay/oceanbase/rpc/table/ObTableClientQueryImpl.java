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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableNotExistException;
import com.alipay.oceanbase.rpc.location.model.ObServerRoute;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.MonitorUtil;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.rpc.protocol.payload.Constants.INVALID_TABLET_ID;
import static com.alipay.oceanbase.rpc.protocol.payload.Constants.OB_INVALID_ID;

public class ObTableClientQueryImpl extends AbstractTableQueryImpl {

    private String                                tableName;
    private final ObTableClient                   obTableClient;
    private Map<Long, ObPair<Long, ObTableParam>> partitionObTables;

    private Row                                   rowKey;                    // only used by BatchOperation

    private boolean                               allowDistributeScan = true;

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
        } else if (tableQuery.isFTSQuery()) {
            if (!ObGlobal.isFtsQuerySupport()) {
                throw new FeatureNotSupportedException("full text query is not supported in "
                                                       + ObGlobal.obVsnString());
            }
            if (tableQuery.getIndexName() == null || tableQuery.getIndexName().isEmpty()
                || tableQuery.getIndexName().equalsIgnoreCase("primary")) {
                throw new IllegalArgumentException(
                    "use fulltext search but specified index name is not fulltext index");
            }
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
        this.partitionObTables = new LinkedHashMap<Long, ObPair<Long, ObTableParam>>(); // partitionObTables -> Map<logicId, Pair<logicId, param>>

        // fill a whole range if no range is added explicitly.
        if (tableQuery.getKeyRanges().isEmpty()) {
            tableQuery.addKeyRange(ObNewRange.getWholeRange());
        }

        // init partitionObTables
        if (obTableClient.isOdpMode()) {
            if (tableQuery.getScanRangeColumns().isEmpty()) {
                if (tableQuery.getIndexName() != null
                    && !tableQuery.getIndexName().equalsIgnoreCase("primary")
                    && !tableQuery.isFTSQuery()) {
                    throw new ObTableException("key range columns must be specified when use index");
                }
            }
            if (entityType != ObTableEntityType.HKV &&
                    getPartId() != null && tableQuery.getIndexName() == null) {
                ObServerRoute route = obTableClient.getRoute(false);
                try {
                    ObTableParam odpTable = obTableClient.getTableParamWithPartId(
                            tableName, getPartId(), route);
                    partitionObTables.put(odpTable.getPartId(), new ObPair<>(odpTable.getPartId(), odpTable));
                } catch (Exception e) {
                    if (e instanceof ObTableException) {
                        if (((ObTableException) e).getErrorCode() == ResultCodes.OB_NOT_SUPPORTED.errorCode) {
                            // current ODP version does not support get partition meta information
                            throw new FeatureNotSupportedException(
                                "current ODP version does not support query with part id", e);
                        } else if (((ObTableException) e).getErrorCode() == ResultCodes.OB_ERR_KV_ROUTE_ENTRY_EXPIRE.errorCode) {
                            // retry table meta one time
                            obTableClient.getOrRefreshTableEntry(tableName, true);
                            ObTableParam odpTable = obTableClient.getTableParamWithPartId(
                                    tableName, getPartId(), route);
                            partitionObTables.put(odpTable.getPartId(), new ObPair<>(odpTable.getPartId(), odpTable));
                        } else {
                            throw e;
                        }
                    } else {
                        throw e;
                    }
                }
            } else {
                partitionObTables.put(0L, new ObPair<Long, ObTableParam>(0L, new ObTableParam(
                    obTableClient.getOdpTable())));
            }
        } else {
            if (getPartId() == null) {
                initPartitions();
            } else { // directly get table from table entry by logic partId
                if (this.entityType != ObTableEntityType.HKV) {
                    indexTableName = obTableClient.getIndexTableName(tableName,
                        tableQuery.getIndexName(), tableQuery.getScanRangeColumns(), false);
                } else if (obTableClient.isTableGroupName(tableName)) {
                    indexTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName,
                        false);
                }
                try {
                    ObTableParam table = obTableClient.getTableParamWithPartId(indexTableName,
                            getPartId(), obTableClient.getRoute(false));
                    partitionObTables.put(table.getPartId(), new ObPair<>(table.getPartId(), table));
                } catch (ObTableNotExistException e) {
                    if (this.entityType == ObTableEntityType.HKV
                            && obTableClient.isTableGroupName(tableName)
                            && obTableClient.getTableGroupInverted().get(indexTableName) != null) {
                        // if it is HKV and is tableGroup request, TableNotExist and tableGroup cache not empty mean that the table cached had been dropped
                        // not to refresh tableGroup cache
                        obTableClient.eraseTableGroupFromCache(tableName);
                        indexTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, true);
                        ObTableParam table = obTableClient.getTableParamWithPartId(indexTableName,
                                getPartId(), obTableClient.getRoute(false));
                        partitionObTables.put(table.getPartId(), new ObPair<>(table.getPartId(), table));
                    }
                }
            }
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
                obTableClientQueryAsyncStreamResult.setAllowDistributeScan(allowDistributeScan);
                setCommonParams2Result(obTableClientQueryAsyncStreamResult);
                obTableClientQueryAsyncStreamResult.setClient(obTableClient);
                obTableClientQueryAsyncStreamResult.init();
                return obTableClientQueryAsyncStreamResult;
            }
        });
    }

    public Map<Long, ObPair<Long, ObTableParam>> initFirstPartition(ObTableQuery tableQuery, String tableName) throws Exception {
        Map<Long, ObPair<Long, ObTableParam>> partitionObTables = new LinkedHashMap<>();
        String indexName = tableQuery.getIndexName();

        if (!this.obTableClient.isOdpMode()) {
            indexTableName = obTableClient.getIndexTableName(tableName, indexName, tableQuery.getScanRangeColumns(), false);
        }

        if (tableQuery.getKeyRanges().isEmpty()) {
            throw new IllegalArgumentException("query ranges is empty");
        } else {
            ObNewRange range = tableQuery.getKeyRanges().get(0);
            ObRowKey startKey = range.getStartKey();
            int startKeySize = startKey.getObjs().size();
            Object[] start = new Object[startKeySize];

            for (int i = 0; i < startKeySize; i++) {
                start[i] = startKey.getObj(i).isMinObj() || startKey.getObj(i).isMaxObj() ?
                        startKey.getObj(i) : startKey.getObj(i).getValue();
            }

            if (this.entityType == ObTableEntityType.HKV && obTableClient.isTableGroupName(tableName)) {
                indexTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, false);
            }
            ObBorderFlag borderFlag = range.getBorderFlag();
            List<ObTableParam> params = this.obTableClient.getTableParams(indexTableName, tableQuery, start,
                    borderFlag.isInclusiveStart(), start, borderFlag.isInclusiveEnd());

            partitionObTables.put(INVALID_TABLET_ID, new ObPair<>(params.get(0).getPartId(), params.get(0)));
        }

        return partitionObTables;
    }

    public Map<Long, ObPair<Long, ObTableParam>> initPartitions(ObTableQuery tableQuery, String tableName) throws Exception {
        // partitionObTables -> <logicId, <logicId, tableParam>>
        Map<Long, ObPair<Long, ObTableParam>> partitionObTables = new LinkedHashMap<>();
        String indexName = tableQuery.getIndexName();

        if (!this.obTableClient.isOdpMode()) {
            indexTableName = obTableClient.getIndexTableName(tableName, indexName, tableQuery.getScanRangeColumns(), false);
        }

        for (ObNewRange range : tableQuery.getKeyRanges()) {
            ObRowKey startKey = range.getStartKey();
            int startKeySize = startKey.getObjs().size();
            ObRowKey endKey = range.getEndKey();
            int endKeySize = endKey.getObjs().size();
            Object[] start = new Object[startKeySize];
            Object[] end = new Object[endKeySize];

            for (int i = 0; i < startKeySize; i++) {
                start[i] = startKey.getObj(i).isMinObj() || startKey.getObj(i).isMaxObj() ?
                        startKey.getObj(i) : startKey.getObj(i).getValue();
            }

            for (int i = 0; i < endKeySize; i++) {
                end[i] = endKey.getObj(i).isMinObj() || endKey.getObj(i).isMaxObj() ?
                        endKey.getObj(i) : endKey.getObj(i).getValue();
            }
            if (this.entityType == ObTableEntityType.HKV && obTableClient.isTableGroupName(tableName)) {
                indexTableName = obTableClient.tryGetTableNameFromTableGroupCache(tableName, false);
            }
            ObBorderFlag borderFlag = range.getBorderFlag();
            List<ObTableParam> params = this.obTableClient.getTableParams(indexTableName, tableQuery, start,
                borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd());
            if (tableQuery.getScanOrder() == ObScanOrder.Reverse) {
                for (int i = params.size() - 1; i >= 0; i--) {
                    ObTableParam param = params.get(i);
                    partitionObTables.put(param.getPartId(), new ObPair<>(param.getPartId(), param));
                }
            } else {
                for (ObTableParam param : params) {
                    partitionObTables.put(param.getPartId(), new ObPair<>(param.getPartId(), param));
                }
            }
        }

        return partitionObTables;
    }

    /*
     * Init partition tables involved in this query
     */
    public void initPartitions() throws Exception {
        if (obTableClient.getServerCapacity().isSupportDistributedExecute()) {
            this.partitionObTables = initFirstPartition(tableQuery, tableName);
        } else {
            this.partitionObTables = initPartitions(tableQuery, tableName);
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

    public void setPartId(Long partId) {
        getObTableQuery().setPartId(partId);
    }

    public Long getPartId() {
        return getObTableQuery().getPartId();
    }

    public void setAllowDistributeScan(boolean allowDistributeScan) {
        this.allowDistributeScan = allowDistributeScan;
    }
}

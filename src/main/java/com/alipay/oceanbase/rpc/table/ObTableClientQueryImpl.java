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
import com.alipay.oceanbase.rpc.location.model.ObIndexInfo;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.ObTableAggregationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.MonitorUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObTableClientQueryImpl extends AbstractTableQueryImpl {

    private final String        tableName;
    private final ObTableClient obTableClient;

    private Row                 rowKey;       // only used by BatchOperation

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
        this.obTableClient = null;
        this.tableQuery = new ObTableQuery();
        this.rowKey = null;
    }

    /*
     * Ob table client query impl.
     */
    public ObTableClientQueryImpl(String tableName, ObTableClient client) {
        this.tableName = tableName;
        this.obTableClient = client;
        this.tableQuery = new ObTableQuery();
        this.rowKey = null;
    }

    /*
     * Ob table client query impl.
     */
    public ObTableClientQueryImpl(String tableName, ObTableQuery tableQuery, ObTableClient client) {
        this.tableName = tableName;
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

    @Override
    public QueryResultSet executeInit(ObPair<Long, ObTableParam> entry) throws Exception {
        throw new IllegalArgumentException("not support executeInit");
    }

    @Override
    public QueryResultSet executeNext(ObPair<Long, ObTableParam> entry) throws Exception {
        throw new IllegalArgumentException("not support executeInit");
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
     * Execute internal.
     */
    public ObTableClientQueryStreamResult executeInternal() throws Exception {
        if (null == obTableClient) {
            throw new ObTableException("table client is null");
        } else if (tableQuery.getLimit() < 0 && tableQuery.getOffset() > 0) {
            throw new ObTableException("offset can not be use without limit");
        }
        final long startTime = System.currentTimeMillis();
        // partitionObTables -> Map<logicId, Pair<logicId, param>>
        Map<Long, ObPair<Long, ObTableParam>> partitionObTables = new HashMap<Long, ObPair<Long, ObTableParam>>();
        // fill a whole range if no range is added explicitly.
        if (tableQuery.getKeyRanges().isEmpty()) {
            tableQuery.addKeyRange(ObNewRange.getWholeRange());
        }
        if (obTableClient.isOdpMode()) {
            if (tableQuery.getScanRangeColumns().isEmpty()) {
                if (tableQuery.getIndexName() != null
                    && !tableQuery.getIndexName().equalsIgnoreCase("primary")) {
                    throw new ObTableException("key range columns must be specified when use index");
                }
            }
            partitionObTables.put(0L, new ObPair<Long, ObTableParam>(0L, new ObTableParam(
                obTableClient.getOdpTable())));
        } else {
            String indexName = tableQuery.getIndexName();
            String indexTableName = tableName;
            if (!this.obTableClient.isOdpMode()) {
                indexTableName = obTableClient.getIndexTableName(tableName, indexName,
                    tableQuery.getScanRangeColumns());
            }

            for (ObNewRange rang : tableQuery.getKeyRanges()) {
                ObRowKey startKey = rang.getStartKey();
                int startKeySize = startKey.getObjs().size();
                ObRowKey endKey = rang.getEndKey();
                int endKeySize = endKey.getObjs().size();
                Object[] start = new Object[startKeySize];
                Object[] end = new Object[endKeySize];
                for (int i = 0; i < startKeySize; i++) {
                    start[i] = startKey.getObj(i).getValue();
                }

                for (int i = 0; i < endKeySize; i++) {
                    end[i] = endKey.getObj(i).getValue();
                }
                ObBorderFlag borderFlag = rang.getBorderFlag();
                // pairs -> List<Pair<logicId, param>>
                List<ObPair<Long, ObTableParam>> pairs = obTableClient.getTables(indexTableName,
                    start, borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd(), false,
                    false, obTableClient.getReadRoute());
                for (ObPair<Long, ObTableParam> pair : pairs) {
                    partitionObTables.put(pair.getLeft(), pair);
                }
            }
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<Long, ObPair<Long, ObTableParam>> entry : partitionObTables.entrySet()) {
            stringBuilder.append("#").append(entry.getValue().getRight().getObTable().getIp())
                .append(":").append(entry.getValue().getRight().getObTable().getPort());
        }
        String endpoint = stringBuilder.toString();
        long getTableTime = System.currentTimeMillis();

        // Defend aggregation of multiple partitions.
        if (tableQuery.isAggregation()) {
            if (partitionObTables.size() > 1) {
                throw new ObTableException(
                    "Not supported aggregate of multiple partitions, the partition size is: "
                            + partitionObTables.size(), ResultCodes.OB_NOT_SUPPORTED.errorCode);
            }
        }

        ObTableClientQueryStreamResult obTableClientQueryStreamResult = new ObTableClientQueryStreamResult();
        obTableClientQueryStreamResult.setTableQuery(tableQuery);
        obTableClientQueryStreamResult.setEntityType(entityType);
        obTableClientQueryStreamResult.setTableName(tableName);
        obTableClientQueryStreamResult.setExpectant(partitionObTables);
        obTableClientQueryStreamResult.setClient(obTableClient);
        obTableClientQueryStreamResult.setOperationTimeout(operationTimeout);
        obTableClientQueryStreamResult.setReadConsistency(obTableClient.getReadConsistency());
        obTableClientQueryStreamResult.init();

        MonitorUtil.info(obTableClientQueryStreamResult, obTableClient.getDatabase(), tableName,
            "QUERY", endpoint, tableQuery, obTableClientQueryStreamResult,
            getTableTime - startTime, System.currentTimeMillis() - getTableTime,
            obTableClient.getslowQueryMonitorThreshold());

        return obTableClientQueryStreamResult;
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

    /*
     * Get row key (used by BatchOperation)
     */
    public Row getRowKey() {
        return this.rowKey;
    }
}

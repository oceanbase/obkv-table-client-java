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
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ObTableClientQueryImpl extends AbstractTableQueryImpl {

    private final String        tableName;
    private final ObTableClient obTableClient;

    /*
     * Ob table client query impl.
     */
    public ObTableClientQueryImpl(String tableName, ObTableClient client) {
        this.tableName = tableName;
        this.obTableClient = client;
        this.tableQuery = new ObTableQuery();
    }

    /*
     * Ob table client query impl.
     */
    public ObTableClientQueryImpl(String tableName, ObTableQuery tableQuery, ObTableClient client) {
        this.tableName = tableName;
        this.obTableClient = client;
        this.tableQuery = tableQuery;
    }

    /*
     * Execute.
     */
    @Override
    public QueryResultSet execute() throws Exception {
        return new QueryResultSet(executeInternal());
    }

    @Override
    public QueryResultSet executeInit(ObPair<Long, ObTable> entry) throws Exception {
        throw new IllegalArgumentException("not support executeInit");
    }

    @Override
    public QueryResultSet executeNext(ObPair<Long, ObTable> entry) throws Exception {
        throw new IllegalArgumentException("not support executeInit");
    }

    /**
     * 只有 limit query 需要，其他不需要
     * @param keys
     * @return
     */
    @Override
    public TableQuery setKeys(String... keys) {
        throw new IllegalArgumentException("Not needed");
    }

    /*
     * Execute internal.
     */
    public ObTableClientQueryStreamResult executeInternal() throws Exception {
        Map<Long, ObPair<Long, ObTable>> partitionObTables = new HashMap<Long, ObPair<Long, ObTable>>();
        if (obTableClient.isOdpMode()) {
            partitionObTables.put(0L, new ObPair<Long, ObTable>(0L, obTableClient.getOdpTable()));
        } else {
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
                List<ObPair<Long, ObTable>> pairs = obTableClient.getTables(tableName, start,
                    borderFlag.isInclusiveStart(), end, borderFlag.isInclusiveEnd(), false, false,
                    obTableClient.getReadRoute());
                for (ObPair<Long, ObTable> pair : pairs) {
                    partitionObTables.put(pair.getLeft(), pair);
                }
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
}

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

import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.stream.ObTableQueryStreamResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.HashMap;
import java.util.Map;

public class ObTableQueryImpl extends AbstractTableQueryImpl {

    private final String        tableName;
    private ObTable             table;

    private ObTableQueryRequest request;

    /*
     * Ob table query impl.
     */
    public ObTableQueryImpl(String tableName, ObTable table) {
        this.tableName = tableName;
        this.table = table;

        resetRequest();
    }

    private void resetRequest() {
        this.request = new ObTableQueryRequest();
        this.tableQuery = new ObTableQuery();
        this.request.setTableName(tableName);
        this.request.setTableQuery(tableQuery);
        // FIXME TableQuery 必须设置 PartitionId
        this.request.setTableId(Constants.OB_INVALID_ID);
        this.request.setPartitionId(Constants.INVALID_TABLET_ID);
    }

    /*
     * Execute.
     */
    @Override
    public QueryResultSet execute() throws Exception {
        Map<Long, ObPair<Long, ObTableParam>> partitionObTable = new HashMap<Long, ObPair<Long, ObTableParam>>();
        partitionObTable.put(0L, new ObPair<Long, ObTableParam>(0L, new ObTableParam(table)));
        ObTableQueryStreamResult obTableQueryStreamResult = new ObTableQueryStreamResult();
        obTableQueryStreamResult.setTableQuery(tableQuery);
        obTableQueryStreamResult.setEntityType(entityType);
        obTableQueryStreamResult.setTableName(tableName);
        obTableQueryStreamResult.setExpectant(partitionObTable);
        obTableQueryStreamResult.setOperationTimeout(operationTimeout);
        obTableQueryStreamResult.init();
        return new QueryResultSet(obTableQueryStreamResult);

    }

    @Override
    public QueryResultSet executeInit(ObPair<Long, ObTableParam> entry) throws Exception {
        throw new IllegalArgumentException("not support executeInit");
    }

    @Override
    public QueryResultSet executeNext(ObPair<Long, ObTableParam> entry) throws Exception {
        throw new IllegalArgumentException("not support executeNext");
    }

    /**
     * 只有 limit query 需要，其他不需要
     * @param keys keys
     * @return query
     */
    @Override
    public TableQuery setKeys(String... keys) {
        throw new IllegalArgumentException("Not needed");
    }

    /*
     * Clear.
     */
    @Override
    public void clear() {
        resetRequest();
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
    @Override
    public String getTableName() {
        return tableName;
    }

    /*
     * Get table.
     */
    public ObTable getTable() {
        return table;
    }

    /*
     * Reset ob table.
     */
    public void resetObTable(ObTable obTable) {
        this.table = obTable;
    }

}

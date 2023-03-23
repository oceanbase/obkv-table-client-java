/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 - 2022 OceanBase
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

package com.alipay.oceanbase.rpc.stream.async;

import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.QueryStreamResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.table.ObTableQueryAsyncImpl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ObTableQueryAsyncResultSet implements QueryStreamResult {
    private final ObTableQueryAsyncImpl obTableQueryAsync;
    private QueryResultSet              resultSet = null;
    private int                         resultSize;
    private ObPair<Long, ObTableParam>  tableEntry;

    public ObTableQueryAsyncResultSet(ObTableQueryAsyncImpl obTableQueryAsync) {
        this.obTableQueryAsync = obTableQueryAsync;
        Map<Long, ObPair<Long, ObTableParam>> partitionObTable = new HashMap<Long, ObPair<Long, ObTableParam>>();
        this.tableEntry = partitionObTable.put(0L, new ObPair<Long, ObTableParam>(0L,
            new ObTableParam(this.obTableQueryAsync.getTable())));
    }

    @Override
    public boolean next() throws Exception {
        // 如果是 null ，就说明它是第一次查询，直接使用老得接口 execute
        if (resultSet == null) {
            resultSet = this.obTableQueryAsync.getTableQuery().executeInit(tableEntry);
            resultSize = resultSet.cacheSize();
        }
        if (resultSet.isHasMore()) {
            if (resultSize == 0) {
                refreshExecute();
            }
        }
        return resultSet.next();
    }

    /**
     * 在更新完 query 以后，重新获取一下结果
     */
    private void refreshExecute() throws Exception {
        resultSet = this.obTableQueryAsync.getTableQuery().executeNext(tableEntry);
        resultSize = resultSet.cacheSize();
    }

    @Override
    public List<ObObj> getRow() {
        List<ObObj> row = resultSet.getQueryStreamResult().getRow();
        resultSize--;
        return row;
    }

    @Override
    public int getRowIndex() {
        return resultSet.getQueryStreamResult().getRowIndex();
    }

    @Override
    public LinkedList<List<ObObj>> getCacheRows() {
        if (resultSet == null) {
            return new LinkedList<List<ObObj>>();
        }
        return resultSet.getQueryStreamResult().getCacheRows();
    }

    @Override
    public List<String> getCacheProperties() {
        return resultSet.getQueryStreamResult().getCacheProperties();
    }

    @Override
    public void init() throws Exception {
        resultSet.getQueryStreamResult().init();
    }

    @Override
    public void close() throws Exception {
        resultSet.getQueryStreamResult().close();
    }
}
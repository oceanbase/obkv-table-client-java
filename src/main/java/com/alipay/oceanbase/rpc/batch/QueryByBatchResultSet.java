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

package com.alipay.oceanbase.rpc.batch;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.QueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class QueryByBatchResultSet implements QueryStreamResult {
    private final QueryByBatch queryByBatch;
    private QueryResultSet     resultSet = null;
    private int                resultSize;

    public QueryByBatchResultSet(QueryByBatch queryByBatch) {
        this.queryByBatch = queryByBatch;
    }

    @Override
    public boolean next() throws Exception {
        // 如果是 null ，就说明它是第一次查询，直接使用老得接口 execute
        if (resultSet == null) {
            resultSet = this.queryByBatch.getTableQuery().execute();
            resultSize = resultSet.cacheSize();
        }
        boolean res = resultSet.next();
        if (!res) {
            // 防止出现 [0,0) 情况
            if (resultSize > 0) {
                // 如果没有 next， getrow 获取的是上一个存在的值
                Map<String, Object> row = resultSet.getRow();
                updateQuery(row, this.queryByBatch.getTableQuery());
                refreshExecute();
                res = resultSet.next();
            }
        }
        return res;
    }

    @Override
    public List<ObObj> getRow() {
        return resultSet.getQueryStreamResult().getRow();
    }

    /**
     * 通过当前的最后一条数据，与之前的查询条件融合
     * 例如，上次查询为 limit["a","z"), 最后一条数据为 "bbb"
     * 则此次查询改造为 limit("bbb", "z")
     * @param row
     * @param tableQuery
     */
    private void updateQuery(Map<String, Object> row, TableQuery tableQuery) {
        List<ObNewRange> ranges = tableQuery.getObTableQuery().getKeyRanges();
        ranges.clear();
        int keyLen = this.queryByBatch.getEnd().length;
        Object[] newBegin = new Object[keyLen];
        for (int i = 0; i < keyLen; i++) {
            newBegin[i] = row.get(this.queryByBatch.getKeys()[i]);
        }
        // 这边 startEquals 永远都是 false， 因为 begin 这条数据已经取过了
        tableQuery.addScanRange(newBegin, false, this.queryByBatch.getEnd(),
            this.queryByBatch.isEndEquals());
    }

    /**
     * 在更新完 query 以后，重新获取一下结果
     */
    private void refreshExecute() throws Exception {
        resultSet = this.queryByBatch.getTableQuery().execute();
        resultSize = resultSet.cacheSize();
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

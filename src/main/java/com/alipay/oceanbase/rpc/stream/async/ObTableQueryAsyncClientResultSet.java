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
import com.alipay.oceanbase.rpc.table.ObTableClientQueryAsyncImpl;
import com.alipay.oceanbase.rpc.table.ObTableParam;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ObTableQueryAsyncClientResultSet implements QueryStreamResult {
    private final ObTableClientQueryAsyncImpl obTableClientQueryAsync;
    private QueryResultSet                    resultSet = null;
    private int                               resultSize;
    private int                               partitionIndex;
    private int                               cur;
    private List<ObPair<Long, ObTableParam>>  lists;

    public ObTableQueryAsyncClientResultSet(ObTableClientQueryAsyncImpl obTableClientQueryAsync)
                                                                                                throws Exception {
        this.obTableClientQueryAsync = obTableClientQueryAsync;
        Map<Long, ObPair<Long, ObTableParam>> expectant = this.obTableClientQueryAsync.getPartitions();
        this.partitionIndex = expectant.size() - 1;
        lists = new ArrayList<ObPair<Long, ObTableParam>>();
        for (Map.Entry<Long, ObPair<Long, ObTableParam>> obPairEntry : expectant.entrySet()) {
            this.lists.add(obPairEntry.getValue());
        }
    }

    @Override
    public boolean next() throws Exception {
        boolean res = false;
        // 对于分区/非分区表，从第一个分区0开始，在每个分区内都执行init和next；如此迭代每一个分区。直到isHasMore=false且分区都遍历完
        if (cur < partitionIndex || partitionIndex == 0 || resultSet == null
            || resultSet.isHasMore()) {
            if (resultSet == null) {
                resultSet = this.obTableClientQueryAsync.getTableQuery()
                    .executeInit(lists.get(cur));
                resultSize = resultSet.cacheSize();
            }
            // 在每个分区中，如果isHasMore==true，那么就需要进入next操作
            if (resultSet.isHasMore()) {
                if (resultSize == 0) {
                    resultSet = this.obTableClientQueryAsync.getTableQuery().executeNext(
                        lists.get(cur));
                    resultSize = resultSet.cacheSize();
                    // 如果当前的next请求结果为空, 说明需要跳转到下一个partition了，开始init
                    if (resultSize == 0 && cur < partitionIndex) {
                        cur++;
                        resultSet = this.obTableClientQueryAsync.getTableQuery().executeInit(
                            lists.get(cur));
                        resultSize = resultSet.cacheSize();
                    }
                }
            }
            // 如果当前partition的数据都处理完成，则跳到其他parititon。并且如果这个parition为空，需要跳到转不为空的partition
            if (resultSize == 0 && partitionIndex != 0) {
                do {
                    cur++;
                    resultSet = this.obTableClientQueryAsync.getTableQuery().executeInit(
                        lists.get(cur));
                    resultSize = resultSet.cacheSize();
                } while (resultSize == 0 && cur < partitionIndex);
            }
        }
        if (resultSize != 0) {
            //对init和next方法得到的结果，再次迭代输出最终结果
            res = resultSet.next();
        }
        return res;
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

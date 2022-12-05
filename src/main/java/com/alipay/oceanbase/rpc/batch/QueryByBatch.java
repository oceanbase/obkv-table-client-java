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

import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHTableFilter;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class QueryByBatch implements TableQuery {
    private final TableQuery tableQuery;
    private Object[]         start, end;
    private boolean          endEquals;
    private String[]         keys   = new String[0];
    private String[]         select = new String[0];

    public QueryByBatch(TableQuery tableQuery) {
        this.tableQuery = tableQuery;
    }

    @Override
    public ObTableQuery getObTableQuery() {
        return this.tableQuery.getObTableQuery();
    }

    @Override
    public String getTableName() {
        return this.tableQuery.getTableName();
    }

    @Override
    public void setEntityType(ObTableEntityType entityType) {
        this.tableQuery.setEntityType(entityType);
    }

    @Override
    public ObTableEntityType getEntityType() {
        return this.tableQuery.getEntityType();
    }

    @Override
    public QueryResultSet execute() {
        return new QueryResultSet(new QueryByBatchResultSet(this));
    }

    @Override
    public QueryResultSet executeInit(ObPair<Long, ObTable> entry) throws Exception {
        throw new IllegalArgumentException("not support executeInit");
    }

    @Override
    public QueryResultSet executeNext(ObPair<Long, ObTable> entry) throws Exception {
        throw new IllegalArgumentException("not support executeNext");
    }

    public TableQuery addScanRange(Object start, Object end) {
        addScanRange(start, true, end, true);
        return this;
    }

    @Override
    public TableQuery addScanRange(Object[] start, Object[] end) {
        return addScanRange(start, true, end, true);
    }

    public TableQuery addScanRange(Object start, boolean startEquals, Object end, boolean endEquals) {
        addScanRange(new Object[] { start }, startEquals, new Object[] { end }, endEquals);
        return this;
    }

    public TableQuery addScanRange(Object[] start, boolean startEquals, Object[] end,
                                   boolean endEquals) {
        this.start = start;
        this.end = end;
        this.endEquals = endEquals;
        this.tableQuery.addScanRange(start, startEquals, end, endEquals);
        return this;
    }

    @Override
    public TableQuery addScanRangeStartsWith(Object start) {
        addScanRangeStartsWith(new Object[] { start });
        return this;
    }

    @Override
    public TableQuery addScanRangeStartsWith(Object[] start) {
        addScanRangeStartsWith(start, true);
        return this;
    }

    @Override
    public TableQuery addScanRangeStartsWith(Object[] start, boolean startEquals) {
        this.start = start;
        this.tableQuery.addScanRangeStartsWith(start, startEquals);
        return this;
    }

    @Override
    public TableQuery addScanRangeEndsWith(Object end) {
        addScanRangeEndsWith(new Object[] { end });
        return this;
    }

    @Override
    public TableQuery addScanRangeEndsWith(Object[] end) {
        addScanRangeEndsWith(end, true);
        return this;
    }

    @Override
    public TableQuery addScanRangeEndsWith(Object[] end, boolean endEquals) {
        this.end = end;
        this.endEquals = endEquals;
        this.tableQuery.addScanRangeEndsWith(end, endEquals);
        return this;
    }

    @Override
    public TableQuery scanOrder(boolean forward) {
        throw new IllegalArgumentException("not support scanOrder for QueryByBatch");
    }

    @Override
    public TableQuery indexName(String indexName) {
        throw new IllegalArgumentException("not support indexName for QueryByBatch");
    }

    @Override
    public TableQuery primaryIndex() {
        throw new IllegalArgumentException("not support primaryIndex for QueryByBatch");
    }

    @Override
    public TableQuery filterString(String filterString) {
        throw new IllegalArgumentException("not support filterString for QueryByBatch");
    }

    /*
     * Set filter
     */
    @Override
    public TableQuery setFilter(ObTableFilter filter) {
        throw new IllegalArgumentException("not support construct filter string for "
                                           + this.getClass().toString());
    }

    @Override
    public TableQuery setHTableFilter(ObHTableFilter obHTableFilter) {
        throw new IllegalArgumentException("not support setHTableFilter for QueryByBatch");
    }

    /*
     * 这边取名为 batch size, 实际转化到查询，就是stream的limit
     * 保证它的查询只是单次的，所以在observer不会转化成stream query
     * @param batchSize
     * @return
     */
    public QueryByBatch setBatchSize(int batchSize) {
        this.tableQuery.limit(batchSize);
        return this;
    }

    @Override
    public TableQuery setOperationTimeout(long operationTimeout) {
        this.tableQuery.setOperationTimeout(operationTimeout);
        return this;
    }

    @Override
    public TableQuery setMaxResultSize(long maxResultSize) {
        throw new IllegalArgumentException("not support setMaxResultSize");
    }

    @Override
    public TableQuery setScanRangeColumns(String... columns) {
        this.getTableQuery().setScanRangeColumns(columns);
        return this;
    }

    @Override
    public void clear() {
        this.tableQuery.clear();
    }

    public TableQuery getTableQuery() {
        return tableQuery;
    }

    /*
     * 必须显式的设置一下 scan 的 keys
     * 因为目前 observer 没有返回 primary key 或者其他 index 的信息
     * 所以拆分查询时，必须手动指定 key
     * 需要和 select 的 key 合并
     * @param keys
     * @return
     */
    @Override
    public TableQuery setKeys(String... keys) {
        this.keys = keys;
        mergeSelect();
        return this;
    }

    @Override
    public TableQuery limit(int limit) {
        throw new IllegalArgumentException("not support limit for QueryByBatch");
    }

    @Override
    public TableQuery limit(int offset, int limit) {
        throw new IllegalArgumentException("not support limit for QueryByBatch");
    }

    /**
     * 需要和 select 的 key 合并
     * @param columns columns
     * @return this
     */
    @Override
    public QueryByBatch select(String... columns) {
        this.select = columns;
        mergeSelect();
        return this;
    }

    /**
     * 只支持两种方式的合并
     * 1. keys: ["a"], select: ["b"], 合并为 ["a", "b"]，即不包含关系
     * 2. keys: ["a"], select: ["a", "b"]， 合并为 ["a", "b"] 即包含关系
     * 其他情况返回不支持
     */
    private void mergeSelect() {
        Set<String> keySet = new HashSet<String>(Arrays.asList(this.keys));
        Set<String> selectSet = new HashSet<String>(Arrays.asList(this.select));
        if (keySet.size() != this.keys.length || selectSet.size() != this.select.length) {
            throw new IllegalArgumentException("key duplicate in (keys or select)");
        }
        Set<String> mix = new HashSet<String>();
        mix.addAll(keySet);
        mix.addAll(selectSet);
        if (mix.size() == keySet.size() + selectSet.size()) {
            String[] mixSelect = new String[this.keys.length + this.select.length];
            System.arraycopy(this.keys, 0, mixSelect, 0, this.keys.length);
            System.arraycopy(this.select, 0, mixSelect, this.keys.length, this.select.length);
            this.tableQuery.select(mixSelect);
        } else if (mix.size() == selectSet.size()) {
            this.tableQuery.select(this.select);
        } else {
            throw new IllegalArgumentException("key duplicate in (keys and select)");
        }

    }

    public String[] getKeys() {
        return keys;
    }

    public Object[] getStart() {
        return start;
    }

    public Object[] getEnd() {
        return end;
    }

    public boolean isEndEquals() {
        return endEquals;
    }
}

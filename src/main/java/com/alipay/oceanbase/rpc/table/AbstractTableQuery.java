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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.List;

public abstract class AbstractTableQuery implements TableQuery {
    private static final String PRIMARY_INDEX_NAME = "PRIMARY";

    protected ObTableEntityType entityType         = ObTableEntityType.KV;

    protected long              operationTimeout   = -1;

    /*
     * Limit.
     */
    @Override
    public TableQuery limit(int limit) {
        return limit(0, limit);
    }

    /*
     * Primary index.
     */
    @Override
    public TableQuery primaryIndex() {
        return indexName(PRIMARY_INDEX_NAME);
    }

    /*
     * Add scan range.
     */
    @Override
    public TableQuery addScanRange(Object start, Object end) {
        return addScanRange(new Object[] { start }, true, new Object[] { end }, true);
    }

    /*
     * Add scan range.
     */
    @Override
    public TableQuery addScanRange(Object[] start, Object[] end) {
        return addScanRange(start, true, end, true);
    }

    /*
     * Add scan range.
     */
    @Override
    public TableQuery addScanRange(Object start, boolean startEquals, Object end, boolean endEquals) {
        return addScanRange(new Object[] { start }, startEquals, new Object[] { end }, endEquals);
    }

    /*
     * Add scan range starts with.
     */
    @Override
    public TableQuery addScanRangeStartsWith(Object start) {
        return addScanRangeStartsWith(new Object[] { start }, true);
    }

    /*
     * Add scan range starts with.
     */
    @Override
    public TableQuery addScanRangeStartsWith(Object[] start) {
        return addScanRangeStartsWith(start, true);
    }

    /*
     * Add scan range ends with.
     */
    @Override
    public TableQuery addScanRangeEndsWith(Object end) {
        return addScanRangeEndsWith(new Object[] { end }, true);
    }

    /*
     * Add scan range ends with.
     */
    @Override
    public TableQuery addScanRangeEndsWith(Object[] end) {
        return addScanRangeEndsWith(end, true);
    }

    /*
     * Set entity type.
     */
    @Override
    public void setEntityType(ObTableEntityType entityType) {
        this.entityType = entityType;
    }

    /*
     * Get entity type.
     */
    @Override
    public ObTableEntityType getEntityType() {
        return entityType;
    }

    /*
     * Set operation timeout.
     */
    @Override
    public TableQuery setOperationTimeout(long operationTimeout) {
        this.operationTimeout = operationTimeout;
        return this;
    }

    /*
     * Set filter
     */
    @Override
    public TableQuery setFilter(ObTableFilter filter) {
        if (null == filter) {
            throw new IllegalArgumentException("input filter is null");
        }
        this.getObTableQuery().setFilterString(filter.toString());
        return this;
    }

    @Override
    public TableQuery setScanRangeColumns(String... columns) {
        this.getObTableQuery().setScanRangeColumns(columns);
        return this;
    }

    @Override
    public List<String> getSelectColumns() {
        throw new ObTableException("only query from BatchOperation support getSelectColumns");
    }

    @Override
    public Row getRowKey() throws Exception {
        throw new ObTableException("only query from BatchOperation support row key");
    }

    @Override
    public TableQuery setRowKey(Row row) throws Exception {
        throw new ObTableException("only query from BatchOperation support row key");
    }
}

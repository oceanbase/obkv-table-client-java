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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHTableFilter;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObScanOrder;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.Arrays;
import java.util.List;

public abstract class AbstractTableQueryImpl extends AbstractTableQuery {

    protected ObTableQuery tableQuery;
    // TableEntry key
    protected String       indexTableName;

    /*
     * Select.
     */
    @Override
    public TableQuery select(String... columns) {
        this.tableQuery.setSelectColumns(Arrays.asList(columns));
        return this;
    }

    /*
     * get select columns' name
     */
    public List<String> getSelectColumns() {
        return this.tableQuery.getSelectColumns();
    }

    /*
     * Limit.
     */
    @Override
    public TableQuery limit(int limit) {

        this.tableQuery.setLimit(limit);
        return this;
    }

    /*
     * Limit.
     */
    @Override
    public TableQuery limit(int offset, int limit) {
        this.tableQuery.setLimit(limit);
        this.tableQuery.setOffset(offset);
        return this;
    }

    /*
     * Add scan range.
     */
    @Override
    public TableQuery addScanRange(Object[] start, boolean startEquals, Object[] end,
                                   boolean endEquals) {
        ObNewRange obNewRange = new ObNewRange();
        obNewRange.setStartKey(ObRowKey.getInstance(start));
        obNewRange.setEndKey(ObRowKey.getInstance(end));

        if (startEquals) {
            obNewRange.getBorderFlag().setInclusiveStart();
        } else {
            obNewRange.getBorderFlag().unsetInclusiveStart();
        }
        if (endEquals) {
            obNewRange.getBorderFlag().setInclusiveEnd();
        } else {
            obNewRange.getBorderFlag().unsetInclusiveEnd();
        }

        tableQuery.addKeyRange(obNewRange);
        return this;
    }

    /*
     * Add scan range starts with.
     */
    @Override
    public TableQuery addScanRangeStartsWith(Object[] start, boolean startEquals) {
        ObNewRange obNewRange = new ObNewRange();
        obNewRange.setStartKey(ObRowKey.getInstance(start));
        obNewRange.setEndKey(ObRowKey.getInstance(ObObj.getMax()));

        if (startEquals) {
            obNewRange.getBorderFlag().setInclusiveStart();
        } else {
            obNewRange.getBorderFlag().unsetInclusiveStart();
        }

        tableQuery.addKeyRange(obNewRange);
        return this;
    }

    /*
     * Add scan range ends with.
     */
    @Override
    public TableQuery addScanRangeEndsWith(Object[] end, boolean endEquals) {
        ObNewRange obNewRange = new ObNewRange();
        obNewRange.setStartKey(ObRowKey.getInstance(ObObj.getMin()));
        obNewRange.setEndKey(ObRowKey.getInstance(end));

        if (endEquals) {
            obNewRange.getBorderFlag().setInclusiveEnd();
        } else {
            obNewRange.getBorderFlag().unsetInclusiveEnd();
        }

        tableQuery.addKeyRange(obNewRange);
        return this;
    }

    /*
     * Scan order.
     */
    @Override
    public TableQuery scanOrder(boolean forward) {
        this.tableQuery.setScanOrder(forward ? ObScanOrder.Forward : ObScanOrder.Reverse);
        return this;
    }

    /*
     * Index name.
     */
    @Override
    public TableQuery indexName(String indexName) {
        this.tableQuery.setIndexName(indexName);
        return this;
    }

    /*
     * Filter string.
     */
    @Override
    public TableQuery filterString(String filterString) {
        this.tableQuery.setFilterString(filterString);
        return this;
    }

    /*
     * Set h table filter.
     */
    @Override
    public TableQuery setHTableFilter(ObHTableFilter obHTableFilter) {
        this.tableQuery.sethTableFilter(obHTableFilter);
        return this;
    }

    /*
     * Set batch size.
     */
    @Override
    public TableQuery setBatchSize(int batchSize) {
        this.tableQuery.setBatchSize(batchSize);
        return this;
    }

    @Override
    public TableQuery setMaxResultSize(long maxResultSize) {
        this.tableQuery.setMaxResultSize(maxResultSize);
        return this;
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }
}

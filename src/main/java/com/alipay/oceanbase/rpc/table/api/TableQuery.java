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

package com.alipay.oceanbase.rpc.table.api;

import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHTableFilter;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;

import java.util.List;

public interface TableQuery {
    public static final String TABLE_COMPARE_FILTER = "TableCompareFilter";

    ObTableQuery getObTableQuery();

    String getTableName();

    TableQuery setRowKey(Row row) throws Exception;

    Row getRowKey() throws Exception;

    List<String> getSelectColumns() throws Exception;

    void setEntityType(ObTableEntityType entityType);

    ObTableEntityType getEntityType();

    QueryResultSet execute() throws Exception;

    QueryResultSet executeInit(ObPair<Long, ObTableParam> entry) throws Exception;

    QueryResultSet executeNext(ObPair<Long, ObTableParam> entry) throws Exception;

    TableQuery select(String... columns);

    TableQuery setKeys(String... keys);

    /**
     * Return row count, -1 indicate unlimited, default: -1
     *
     * @param limit limit row count
     * @return this
     */
    TableQuery limit(int limit);

    /* can not use offset without valid limit value. limit lessThan 0 && offset bigThan 0 */
    /**
     * Row count offset, default: 0
     * 
     * @param offset limit offset
     * @param limit limit count
     * @return this TableQuery
     */
    TableQuery limit(int offset, int limit);

    /* Table API specific interface*/

    /**
     * Add scan range
     *
     * @param start start
     * @param end end
     * @return this TableQuery
     */
    TableQuery addScanRange(Object start, Object end);

    TableQuery addScanRange(Object[] start, Object[] end);

    /*
     * Add scan range
     *
     * @param startEquals true: >= start; false: > start
     * @param endEquals true: <= end; false: < end
     * @return this
     */
    TableQuery addScanRange(Object start, boolean startEquals, Object end, boolean endEquals);

    TableQuery addScanRange(Object[] start, boolean startEquals, Object[] end, boolean endEquals);

    /*
     * Add scan range starts with
     *
     * @param start >= start
     * @return this
     */
    TableQuery addScanRangeStartsWith(Object start);

    TableQuery addScanRangeStartsWith(Object[] start);

    /*
     * Add scan range starts with
     * @param start >= start
     * @param startEquals true: >= start; false: > start
     * @return this
     */
    TableQuery addScanRangeStartsWith(Object[] start, boolean startEquals);

    /*
     * Add scan range ends with
     *
     * @param end <= end
     * @return this
     */
    TableQuery addScanRangeEndsWith(Object end);

    TableQuery addScanRangeEndsWith(Object[] end);

    /*
     * Add scan range ends with
     * @param end <= end
     * @param endEquals true: <= end; false: < end
     * @return this
     */
    TableQuery addScanRangeEndsWith(Object[] end, boolean endEquals);

    /*
     * Scan order, default forward
     *
     * @param forward forward(true) or reverse(false) order
     * @return this
     */
    TableQuery scanOrder(boolean forward);

    /*
     * Set index name
     *
     * @param indexName Table index name
     * @return this
     */
    TableQuery indexName(String indexName);

    /**
     * Use primary index: PRIMARY
     *
     * @return this
     */
    TableQuery primaryIndex();

    /**
     * Set filter string: no support yet
     *
     * @param filterString filter
     * @return this
     */
    TableQuery filterString(String filterString);

    /**
     * Set filter
     *
     * @param filter prepared filter
     * @return this
     */
    TableQuery setFilter(ObTableFilter filter);

    TableQuery setHTableFilter(ObHTableFilter obHTableFilter);

    /**
     * Set batch size
     * default is -1 means one rpc will return all the results
     * zero or negative value is meaningless so will be reset to default
     * when user sets the batch size the stream mode will active
     * @param batchSize batch size
     * @return this
     */
    TableQuery setBatchSize(int batchSize);

    /**
     * Set operation timeout
     * the default of timeout is 10 second
     * Be careful about the timeout when you set the batch size ,which should
     * be completed in query time out
     * @param operationTimeout timeout
     * @return this
     */
    TableQuery setOperationTimeout(long operationTimeout);

    TableQuery setMaxResultSize(long maxResultSize);

    TableQuery setScanRangeColumns(String... columns);

    void clear();
}

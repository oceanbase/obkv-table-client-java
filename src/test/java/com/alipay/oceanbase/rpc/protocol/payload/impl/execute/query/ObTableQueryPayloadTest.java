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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query;

import com.alipay.oceanbase.rpc.protocol.payload.impl.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableConsistencyLevel;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ObTableQueryPayloadTest {

    @Test
    public void test_ObNewRange() {
        ObNewRange obNewRange = getObNewRange();

        byte[] bytes = obNewRange.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObNewRange newObNewRange = new ObNewRange();
        newObNewRange.decode(buf);

        checkObNewRange(obNewRange, newObNewRange);
    }

    @Test
    public void test_ObHTableFilter() {
        ObHTableFilter obHTableFilter = getObHTableFilter();

        byte[] bytes = obHTableFilter.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObHTableFilter newObHTableFilter = new ObHTableFilter();
        newObHTableFilter.decode(buf);

        checkObHTableFilter(obHTableFilter, newObHTableFilter);

    }

    @Test
    public void test_ObTableQuery() {
        ObTableQuery obTableQuery = getObTableQuery();

        byte[] bytes = obTableQuery.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableQuery newObTableQuery = new ObTableQuery();
        newObTableQuery.decode(buf);

        checkObTableQuery(obTableQuery, newObTableQuery);
    }

    @Test
    public void test_ObTableQueryWithHbaseQuery() {
        ObTableQuery obTableQuery = getObTableQuery();
        obTableQuery.sethTableFilter(getObHTableFilter());

        byte[] bytes = obTableQuery.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableQuery newObTableQuery = new ObTableQuery();
        newObTableQuery.decode(buf);

        checkObTableQuery(obTableQuery, newObTableQuery);
        checkObHTableFilter(obTableQuery.gethTableFilter(), newObTableQuery.gethTableFilter());
    }

    @Test
    public void test_ObTableQueryRequest() {
        ObTableQueryRequest obTableQueryRequest = new ObTableQueryRequest();
        obTableQueryRequest.setTableQuery(getObTableQuery());
        obTableQueryRequest.setTableName("test");
        obTableQueryRequest.setTableId(200);
        obTableQueryRequest.setPartitionId(100);
        obTableQueryRequest.setEntityType(ObTableEntityType.KV);
        obTableQueryRequest.setConsistencyLevel(ObTableConsistencyLevel.EVENTUAL);

        byte[] bytes = obTableQueryRequest.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableQueryRequest newObTableQueryRequest = new ObTableQueryRequest();
        newObTableQueryRequest.decode(buf);

        checkObTableQuery(obTableQueryRequest.getTableQuery(),
            newObTableQueryRequest.getTableQuery());
        assertEquals(obTableQueryRequest.getTableName(), newObTableQueryRequest.getTableName());
        assertEquals(obTableQueryRequest.getTableId(), newObTableQueryRequest.getTableId());
        assertEquals(obTableQueryRequest.getPartitionId(), newObTableQueryRequest.getPartitionId());
        assertEquals(obTableQueryRequest.getEntityType(), newObTableQueryRequest.getEntityType());
        assertEquals(obTableQueryRequest.getConsistencyLevel(),
            newObTableQueryRequest.getConsistencyLevel());

    }

    @Test
    public void test_ObTableQueryResult() {
        ObTableQueryResult obTableQueryResult = new ObTableQueryResult();
        obTableQueryResult.setRowCount(0);
        obTableQueryResult.addPropertiesName("test1");
        obTableQueryResult.addPropertiesName("test2");

        byte[] bytes = obTableQueryResult.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableQueryResult newObTableQueryResult = new ObTableQueryResult();
        newObTableQueryResult.decode(buf);

        assertEquals(obTableQueryResult.getRowCount(), newObTableQueryResult.getRowCount());
        assertEquals(obTableQueryResult.getPropertiesNames().size(), newObTableQueryResult
            .getPropertiesNames().size());
        assertEquals(obTableQueryResult.getPropertiesNames().get(0), newObTableQueryResult
            .getPropertiesNames().get(0));
        assertEquals(obTableQueryResult.getPropertiesNames().get(1), newObTableQueryResult
            .getPropertiesNames().get(1));
        buf.release();
    }

    private ObTableQuery getObTableQuery() {
        ObTableQuery obTableQuery = new ObTableQuery();
        obTableQuery.addKeyRange(getObNewRange());
        obTableQuery.addKeyRange(getObNewRange());
        obTableQuery.addSelectColumn("test_selectColumn");
        obTableQuery.setFilterString("test_filterString");
        obTableQuery.setIndexName("test_indexName");
        obTableQuery.setLimit(100);
        obTableQuery.setOffset(200);
        obTableQuery.setScanOrder(ObScanOrder.Forward);

        return obTableQuery;
    }

    private void checkObTableQuery(ObTableQuery obTableQuery, ObTableQuery newObTableQuery) {
        checkObNewRange(obTableQuery.getKeyRanges().get(0), newObTableQuery.getKeyRanges().get(0));
        assertEquals(obTableQuery.getSelectColumns().get(0), newObTableQuery.getSelectColumns()
            .get(0));
        assertEquals(obTableQuery.getFilterString(), newObTableQuery.getFilterString());
        assertEquals(obTableQuery.getIndexName(), newObTableQuery.getIndexName());
        assertEquals(obTableQuery.getLimit(), newObTableQuery.getLimit());
        assertEquals(obTableQuery.getOffset(), newObTableQuery.getOffset());
        assertEquals(obTableQuery.getScanOrder(), newObTableQuery.getScanOrder());

        checkObNewRange(obTableQuery.getKeyRanges().get(1), newObTableQuery.getKeyRanges().get(1));
        assertEquals(obTableQuery.getSelectColumns().get(0), newObTableQuery.getSelectColumns()
            .get(0));
        assertEquals(obTableQuery.getFilterString(), newObTableQuery.getFilterString());
        assertEquals(obTableQuery.getIndexName(), newObTableQuery.getIndexName());
        assertEquals(obTableQuery.getLimit(), newObTableQuery.getLimit());
        assertEquals(obTableQuery.getOffset(), newObTableQuery.getOffset());
        assertEquals(obTableQuery.getScanOrder(), newObTableQuery.getScanOrder());
    }

    private void checkObHTableFilter(ObHTableFilter obHTableFilter, ObHTableFilter newObHTableFilter) {
        assertEquals(obHTableFilter.isValid(), newObHTableFilter.isValid());
        assertEquals(obHTableFilter.getMinStamp(), newObHTableFilter.getMinStamp());
        assertEquals(obHTableFilter.getMaxStamp(), newObHTableFilter.getMaxStamp());
        assertEquals(obHTableFilter.getMaxVersions(), newObHTableFilter.getMaxVersions());
        assertEquals(obHTableFilter.getLimitPerRowPerCf(), newObHTableFilter.getLimitPerRowPerCf());
        assertEquals(obHTableFilter.getOffsetPerRowPerCf(),
            newObHTableFilter.getOffsetPerRowPerCf());
        assertEquals(obHTableFilter.getFilterString(), newObHTableFilter.getFilterString());
        assertEquals(obHTableFilter.getSelectColumnQualifier().size(), newObHTableFilter
            .getSelectColumnQualifier().size());
        assertEquals(new String(obHTableFilter.getSelectColumnQualifier().get(0).bytes),
            new String(newObHTableFilter.getSelectColumnQualifier().get(0).bytes));
        assertEquals(new String(obHTableFilter.getSelectColumnQualifier().get(1).bytes),
            new String(newObHTableFilter.getSelectColumnQualifier().get(1).bytes));
        assertEquals(new String(obHTableFilter.getSelectColumnQualifier().get(2).bytes),
            new String(newObHTableFilter.getSelectColumnQualifier().get(2).bytes));
    }

    private ObHTableFilter getObHTableFilter() {
        ObHTableFilter obHTableFilter = new ObHTableFilter();
        obHTableFilter.setValid(true);
        obHTableFilter.setMinStamp(100);
        obHTableFilter.setMaxStamp(200);
        obHTableFilter.setMaxVersions(300);
        obHTableFilter.setLimitPerRowPerCf(400);
        obHTableFilter.setOffsetPerRowPerCf(500);
        obHTableFilter.setFilterString("123");

        List<ObBytesString> qs = new ArrayList<ObBytesString>();
        qs.add(new ObBytesString("q1".getBytes()));
        qs.add(new ObBytesString("q3".getBytes()));
        qs.add(new ObBytesString("q5".getBytes()));
        obHTableFilter.setSelectColumnQualifier(qs);

        return obHTableFilter;
    }

    private ObNewRange getObNewRange() {
        ObNewRange obNewRange = new ObNewRange();
        obNewRange.setTableId(100);

        ObRowKey rowKey = new ObRowKey();
        ObObj obj = new ObObj();
        obj.setMeta(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 10));
        obj.setValue(12345);
        rowKey.addObj(obj);
        obj = new ObObj();
        obj.setMeta(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 10));
        obj.setValue(45678);
        rowKey.addObj(obj);

        obNewRange.setStartKey(rowKey);
        obNewRange.setEndKey(rowKey);
        return obNewRange;
    }

    private void checkObNewRange(ObNewRange obNewRange, ObNewRange newObNewRange) {
        assertEquals(obNewRange.getTableId(), newObNewRange.getTableId());
        assertEquals(obNewRange.getBorderFlag(), newObNewRange.getBorderFlag());

        assertEquals(obNewRange.getStartKey().getObjCount(), newObNewRange.getStartKey()
            .getObjCount());
        assertEquals(Integer.valueOf(obNewRange.getStartKey().getObj(0).getValue() + ""),
            Integer.valueOf(newObNewRange.getStartKey().getObj(0).getValue() + ""));
        assertEquals(Integer.valueOf(obNewRange.getStartKey().getObj(1).getValue() + ""),
            Integer.valueOf(newObNewRange.getStartKey().getObj(1).getValue() + ""));

        assertEquals(obNewRange.getEndKey().getObjCount(), newObNewRange.getEndKey().getObjCount());
        assertEquals(Integer.valueOf(obNewRange.getEndKey().getObj(0).getValue() + ""),
            Integer.valueOf(newObNewRange.getEndKey().getObj(0).getValue() + ""));
        assertEquals(Integer.valueOf(obNewRange.getEndKey().getObj(1).getValue() + ""),
            Integer.valueOf(newObNewRange.getEndKey().getObj(1).getValue() + ""));
    }
}

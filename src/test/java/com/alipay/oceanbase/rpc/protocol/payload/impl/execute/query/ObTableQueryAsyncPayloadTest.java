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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationLevel;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ObTableQueryAsyncPayloadTest {
    @Test
    public void test_ObTableQueryAsync() {
        ObTableQuery obTableQuery = getObTableQuery();
        byte[] bytes = obTableQuery.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableQuery obTableQuery1 = getObTableQuery();
        obTableQuery1.decode(buf);

        checkObTableQuery(obTableQuery, obTableQuery1);
    }

    @Test
    public void test_ObTableQueryAsyncResult() {
        ObTableQueryAsyncResult obTableQueryAsyncResult = new ObTableQueryAsyncResult();
        ObTableQueryResult obTableQueryResult = new ObTableQueryResult();
        obTableQueryResult.setRowCount(0);
        obTableQueryResult.addPropertiesName("test1");
        obTableQueryResult.addPropertiesName("test2");
        obTableQueryAsyncResult.setAffectedEntity(obTableQueryResult);
        obTableQueryAsyncResult.setEnd(true);
        obTableQueryAsyncResult.setSessionId(0);

        byte[] bytes = obTableQueryAsyncResult.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableQueryAsyncResult newObTableQueryAsyncResult = new ObTableQueryAsyncResult();
        ObTableQueryResult newObTableQueryResult = new ObTableQueryResult();
        newObTableQueryAsyncResult.setAffectedEntity(newObTableQueryResult);
        newObTableQueryAsyncResult.decode(buf);

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

    private void checkObTableQuery(ObTableQuery obTableQueryAsync, ObTableQuery newObTableQueryAsync) {
        ObTableQuery obTableQuery = obTableQueryAsync;
        ObTableQuery obTableQuery1 = newObTableQueryAsync;
        checkObNewRange(obTableQuery.getKeyRanges().get(0), obTableQuery1.getKeyRanges().get(0));
        assertEquals(obTableQuery.getSelectColumns().get(0), obTableQuery1.getSelectColumns()
            .get(0));
        assertEquals(obTableQuery.getFilterString(), obTableQuery1.getFilterString());
        assertEquals(obTableQuery.getIndexName(), obTableQuery1.getIndexName());
        assertEquals(obTableQuery.getLimit(), obTableQuery1.getLimit());
        assertEquals(obTableQuery.getOffset(), obTableQuery1.getOffset());
        assertEquals(obTableQuery.getScanOrder(), obTableQuery1.getScanOrder());

        checkObNewRange(obTableQuery.getKeyRanges().get(1), obTableQuery1.getKeyRanges().get(1));
        assertEquals(obTableQuery.getSelectColumns().get(0), obTableQuery1.getSelectColumns()
            .get(0));
        assertEquals(obTableQuery.getFilterString(), obTableQuery1.getFilterString());
        assertEquals(obTableQuery.getIndexName(), obTableQuery1.getIndexName());
        assertEquals(obTableQuery.getLimit(), obTableQuery1.getLimit());
        assertEquals(obTableQuery.getOffset(), obTableQuery1.getOffset());
        assertEquals(obTableQuery.getScanOrder(), obTableQuery1.getScanOrder());
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
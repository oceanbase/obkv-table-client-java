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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate;

import com.alipay.oceanbase.rpc.protocol.payload.impl.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObScanOrder;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ObTableQueryAndMutatePayloadTest {

    @Test
    public void test_ObTableQueryRequest() {
        ObTableQueryAndMutateRequest obTableQueryAndMutateRequest = new ObTableQueryAndMutateRequest();
        ObTableQueryAndMutate tableQueryAndMutate = new ObTableQueryAndMutate();
        obTableQueryAndMutateRequest.setTableQueryAndMutate(tableQueryAndMutate);

        tableQueryAndMutate.setTableQuery(getObTableQuery());
        tableQueryAndMutate.setMutations(getMutations());

        obTableQueryAndMutateRequest.setTableName("test");
        obTableQueryAndMutateRequest.setTableId(200);
        obTableQueryAndMutateRequest.setPartitionId(100);
        obTableQueryAndMutateRequest.setEntityType(ObTableEntityType.KV);

        byte[] bytes = obTableQueryAndMutateRequest.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableQueryAndMutateRequest newObTableQueryAndMutateRequest = new ObTableQueryAndMutateRequest();
        newObTableQueryAndMutateRequest.decode(buf);

        checkObTableQuery(obTableQueryAndMutateRequest.getTableQueryAndMutate().getTableQuery(),
            newObTableQueryAndMutateRequest.getTableQueryAndMutate().getTableQuery());
        checkMutations(obTableQueryAndMutateRequest.getTableQueryAndMutate().getMutations(),
            newObTableQueryAndMutateRequest.getTableQueryAndMutate().getMutations());
        assertEquals(obTableQueryAndMutateRequest.getTableName(),
            newObTableQueryAndMutateRequest.getTableName());
        assertEquals(obTableQueryAndMutateRequest.getTableId(),
            newObTableQueryAndMutateRequest.getTableId());
        assertEquals(obTableQueryAndMutateRequest.getPartitionId(),
            newObTableQueryAndMutateRequest.getPartitionId());
        assertEquals(obTableQueryAndMutateRequest.getEntityType(),
            newObTableQueryAndMutateRequest.getEntityType());

    }

    @Test
    public void test_ObTableQueryResult() {
        ObTableQueryAndMutateResult obTableQueryAndMutateResult = new ObTableQueryAndMutateResult();
        obTableQueryAndMutateResult.setAffectedRows(100);

        byte[] bytes = obTableQueryAndMutateResult.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableQueryAndMutateResult newObTableQueryAndMutateResult = new ObTableQueryAndMutateResult();
        newObTableQueryAndMutateResult.decode(buf);

        assertEquals(obTableQueryAndMutateResult.getAffectedRows(),
            newObTableQueryAndMutateResult.getAffectedRows());

    }

    private ObTableBatchOperation getMutations() {
        ObTableOperation obTableOperation = new ObTableOperation();
        obTableOperation.setOperationType(ObTableOperationType.INSERT);
        ObITableEntity entity = new ObTableEntity();
        obTableOperation.setEntity(entity);
        ObObj obj = new ObObj();
        obj.setMeta(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 10));
        obj.setValue(12345L);
        entity.addRowKeyValue(obj);

        ObTableBatchOperation mutations = new ObTableBatchOperation();
        mutations.addTableOperation(obTableOperation);

        return mutations;
    }

    private void checkMutations(ObTableBatchOperation batchOperation,
                                ObTableBatchOperation newBatchOperation) {
        assertEquals(batchOperation.getTableOperations().size(), newBatchOperation
            .getTableOperations().size());

        ObTableOperation tableOperation = batchOperation.getTableOperations().get(0);
        ObTableOperation newTableOperation = newBatchOperation.getTableOperations().get(0);

        assertEquals(tableOperation.getOperationType(), newTableOperation.getOperationType());
        assertEquals(tableOperation.getEntity().getRowKeyValue(0).getValue(), newTableOperation
            .getEntity().getRowKeyValue(0).getValue());
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

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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.impl.*;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ObTableBatchOperationRequestTest {

    @Test
    public void test_all() {
        ObTableBatchOperationRequest request = new ObTableBatchOperationRequest();
        request.setCredential(new ObBytesString("123".getBytes()));
        request.setTableName("name");
        request.setTableId(456);
        request.setConsistencyLevel(ObTableConsistencyLevel.EVENTUAL);
        ObTableBatchOperation obTableBatchOperation = new ObTableBatchOperation();
        request.setBatchOperation(obTableBatchOperation);

        // first obTableOperation
        ObTableOperation obTableOperation = new ObTableOperation();
        obTableOperation.setOperationType(ObTableOperationType.INSERT);
        obTableBatchOperation.addTableOperation(obTableOperation);

        ObITableEntity entity = new ObTableEntity();
        obTableOperation.setEntity(entity);
        ObObj obj = new ObObj();
        obj.setMeta(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 10));
        obj.setValue(12345);
        entity.addRowKeyValue(obj);

        ObObj obj2 = new ObObj();
        obj2.setMeta(new ObObjMeta(ObObjType.ObVarcharType, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI, (byte) 11));
        obj2.setValue("67890");
        entity.setProperty("test", obj);
        entity.setProperty("test2", obj2);

        // second obTableOperation
        ObTableOperation obTableOperation2 = new ObTableOperation();
        obTableOperation2.setOperationType(ObTableOperationType.DEL);
        obTableBatchOperation.addTableOperation(obTableOperation2);

        ObITableEntity entity2 = new ObTableEntity();
        obTableOperation2.setEntity(entity2);
        ObObj obj3 = new ObObj();
        obj3.setMeta(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 10));
        obj3.setValue(12345);
        entity2.addRowKeyValue(obj3);

        ObObj obj4 = new ObObj();
        obj4.setMeta(new ObObjMeta(ObObjType.ObVarcharType, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI, (byte) 11));
        obj4.setValue("67890");
        entity2.setProperty("test", obj3);
        entity2.setProperty("test2", obj4);

        byte[] bytes = request.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableBatchOperationRequest deRequest = new ObTableBatchOperationRequest();
        deRequest.decode(buf);

        assertEquals(new String("123".getBytes()), new String(deRequest.getCredential().bytes));
        assertEquals("name", deRequest.getTableName());
        assertEquals(ObTableConsistencyLevel.EVENTUAL, deRequest.getConsistencyLevel());
        assertEquals(2, deRequest.getBatchOperation().getTableOperations().size());
        assertEquals(ObTableOperationType.INSERT, deRequest.getBatchOperation()
            .getTableOperations().get(0).getOperationType());
        assertEquals(ObTableOperationType.DEL, deRequest.getBatchOperation().getTableOperations()
            .get(1).getOperationType());

        ObITableEntity deEntity = deRequest.getBatchOperation().getTableOperations().get(0)
            .getEntity();
        assertNotNull(deEntity);

        assertEquals(1L, deEntity.getRowKeySize());
        ObObj obObj = deEntity.getRowKeyValue(0);
        assertEquals(12345L, obObj.getValue());
        assertEquals(ObObjType.ObInt64Type, obObj.getMeta().getType());
        assertEquals(ObCollationLevel.CS_LEVEL_EXPLICIT, obObj.getMeta().getCsLevel());
        assertEquals(ObCollationType.CS_TYPE_BINARY, obObj.getMeta().getCsType());
        assertEquals((byte) 10, obObj.getMeta().getScale());

        assertEquals(2L, deEntity.getPropertiesCount());
        obObj = deEntity.getProperty("test");
        assertNotNull(obObj);
        assertEquals(12345L, obObj.getValue());
        assertEquals(ObObjType.ObInt64Type, obObj.getMeta().getType());
        assertEquals(ObCollationLevel.CS_LEVEL_EXPLICIT, obObj.getMeta().getCsLevel());
        assertEquals(ObCollationType.CS_TYPE_BINARY, obObj.getMeta().getCsType());
        assertEquals((byte) 10, obObj.getMeta().getScale());
        obObj = deEntity.getProperty("test2");
        assertNotNull(obObj);
        assertEquals("67890", obObj.getValue());
        assertEquals(ObObjType.ObVarcharType, obObj.getMeta().getType());
        assertEquals(ObCollationLevel.CS_LEVEL_EXPLICIT, obObj.getMeta().getCsLevel());
        assertEquals(ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI, obObj.getMeta().getCsType());
        assertEquals((byte) 11, obObj.getMeta().getScale());

    }

}

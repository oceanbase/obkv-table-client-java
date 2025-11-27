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

public class ObTableOperationRequestTest {

    @Test
    public void test_all() {
        ObTableOperationRequest request = new ObTableOperationRequest();
        request.setCredential(new ObBytesString("123".getBytes()));
        request.setTableName("name");
        request.setTableId(456);
        request.setConsistencyLevel(ObReadConsistency.WEAK);
        ObTableOperation obTableOperation = new ObTableOperation();
        obTableOperation.setOperationType(ObTableOperationType.INSERT);
        request.setTableOperation(obTableOperation);

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

        byte[] bytes = request.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableOperationRequest deRequest = new ObTableOperationRequest();
        deRequest.decode(buf);

        assertEquals(new String("123".getBytes()), new String(deRequest.getCredential().bytes));
        assertEquals("name", deRequest.getTableName());
        assertEquals(ObReadConsistency.WEAK, deRequest.getConsistencyLevel());
        assertEquals(ObTableOperationType.INSERT, deRequest.getTableOperation().getOperationType());

        ObITableEntity deEntity = deRequest.getTableOperation().getEntity();
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

    @Test
    public void test_noProperties() {
        ObTableOperationRequest request = new ObTableOperationRequest();
        request.setCredential(new ObBytesString("123".getBytes()));
        request.setTableName("name");
        request.setTableId(456);
        request.setConsistencyLevel(ObReadConsistency.WEAK);
        ObTableOperation obTableOperation = new ObTableOperation();
        obTableOperation.setOperationType(ObTableOperationType.INSERT);
        request.setTableOperation(obTableOperation);

        ObITableEntity entity = new ObTableEntity();
        obTableOperation.setEntity(entity);
        ObObj obj = new ObObj();
        obj.setMeta(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 10));
        obj.setValue(12345);
        entity.addRowKeyValue(obj);

        byte[] bytes = request.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableOperationRequest deRequest = new ObTableOperationRequest();
        deRequest.decode(buf);

        assertEquals(new String("123".getBytes()), new String(deRequest.getCredential().bytes));
        assertEquals("name", deRequest.getTableName());
        assertEquals(ObReadConsistency.WEAK, deRequest.getConsistencyLevel());
        assertEquals(ObTableOperationType.INSERT, deRequest.getTableOperation().getOperationType());

        ObITableEntity deEntity = deRequest.getTableOperation().getEntity();
        assertNotNull(deEntity);

        assertEquals(1L, deEntity.getRowKeySize());
        ObObj obObj = deEntity.getRowKeyValue(0);
        assertEquals(12345L, obObj.getValue());
        assertEquals(ObObjType.ObInt64Type, obObj.getMeta().getType());
        assertEquals(ObCollationLevel.CS_LEVEL_EXPLICIT, obObj.getMeta().getCsLevel());
        assertEquals(ObCollationType.CS_TYPE_BINARY, obObj.getMeta().getCsType());
        assertEquals((byte) 10, obObj.getMeta().getScale());

        assertEquals(0L, deEntity.getPropertiesCount());

    }

}

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
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ObTableBatchOperationResultTest {

    @Test
    public void test() {
        ObTableBatchOperationResult batchResult = new ObTableBatchOperationResult();
        ObTableOperationResult result = new ObTableOperationResult();
        result.getHeader().setErrno(123);
        result.getHeader().setSqlState("HY0001".getBytes());
        result.getHeader().setMsg("msg".getBytes());
        result.setOperationType(ObTableOperationType.GET);

        ObITableEntity entity = new ObTableEntity();
        ObObj obj = new ObObj();
        obj.setMeta(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 10));
        obj.setValue(12345);
        entity.addRowKeyValue(obj);

        ObObj obj2 = new ObObj();
        obj2.setMeta(new ObObjMeta(ObObjType.ObInt64Type, ObCollationLevel.CS_LEVEL_EXPLICIT,
            ObCollationType.CS_TYPE_BINARY, (byte) 11));
        obj2.setValue(67890);
        entity.setProperty("test", obj);
        entity.setProperty("test2", obj2);
        result.setEntity(entity);

        batchResult.addResult(result);
        batchResult.addResult(result);

        byte[] bytes = batchResult.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);

        ObTableBatchOperationResult deBatchResult = new ObTableBatchOperationResult();
        deBatchResult.decode(buf);

        assertEquals(2, deBatchResult.getResults().size());

        ObTableOperationResult deResult = deBatchResult.getResults().get(0);
        assertEquals(123, deResult.getHeader().getErrno());
        assertEquals(new String("HY0001".getBytes()),
            new String(deResult.getHeader().getSqlState()));
        assertEquals(new String("msg".getBytes()), new String(deResult.getHeader().getMsg()));
        assertEquals(ObTableOperationType.GET, deResult.getOperationType());
        ObITableEntity deEntity = deResult.getEntity();
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
        assertEquals(67890L, obObj.getValue());
        assertEquals(ObObjType.ObInt64Type, obObj.getMeta().getType());
        assertEquals(ObCollationLevel.CS_LEVEL_EXPLICIT, obObj.getMeta().getCsLevel());
        assertEquals(ObCollationType.CS_TYPE_BINARY, obObj.getMeta().getCsType());
        assertEquals((byte) 11, obObj.getMeta().getScale());

    }

}

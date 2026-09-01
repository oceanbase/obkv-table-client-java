/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2026 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * #L%
 */
package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHBaseCellBatch;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class ObTableSingleOpEntityHBaseCompactDecodeTest {

    @Test
    public void decodesKqtvDirectlyIntoCompactBatch() {
        ObTableSingleOpEntity decoded = decode(newKqtvEntity(), true);

        ObHBaseCellBatch batch = decoded.getHBaseCellBatch();
        assertNotNull(batch);
        assertEquals(1, batch.size());
        assertArrayEquals(bytes("r1"), batch.getRowKey(0));
        assertArrayEquals(bytes("cf\0q1"), batch.getQualifier(0));
        assertEquals(100L, batch.getTimestamp(0));
        assertArrayEquals(bytes("v0"), batch.getValue(0));
        assertEquals(0, decoded.getPropertiesValues().size());
    }

    @Test
    public void absentHBaseDecodeContextUsesGenericObObjValues() {
        ObTableSingleOpEntity decoded = decode(newKqtvEntity(), false);

        assertNull(decoded.getHBaseCellBatch());
        assertEquals(4, decoded.getPropertiesValues().size());
        assertArrayEquals(bytes("r1"), (byte[]) decoded.getPropertiesValues().get(0).getValue());
        assertEquals(100L, decoded.getPropertiesValues().get(2).getValue());
    }

    @Test
    public void nonKqtvSchemaFallsBackToGenericDecode() {
        ObTableSingleOpEntity encoded = new ObTableSingleOpEntity();
        addCell(encoded, "VALUE", "r1", "cf\0q1", 100L, "v1");
        prepareForEncode(encoded, "K", "Q", "T", "VALUE");
        ObTableSingleOpEntity decoded = decode(encoded, true, Arrays.asList("K", "Q", "T", "VALUE"));

        assertNull(decoded.getHBaseCellBatch());
        assertEquals(4, decoded.getPropertiesValues().size());
    }

    private static ObTableSingleOpEntity newKqtvEntity() {
        ObTableSingleOpEntity entity = new ObTableSingleOpEntity();
        addCell(entity, "V", "r1", "cf\0q1", 100L, "v0");
        prepareForEncode(entity, "K", "Q", "T", "V");
        return entity;
    }

    private static void prepareForEncode(ObTableSingleOpEntity entity, String... columns) {
        entity.adjustRowkeyColumnName(new LinkedHashMap<String, Long>());
        Map<String, Long> propertyIndexes = new LinkedHashMap<String, Long>();
        for (int i = 0; i < columns.length; i++) {
            propertyIndexes.put(columns[i], (long) i);
        }
        entity.adjustPropertiesColumnName(propertyIndexes);
    }

    private static ObTableSingleOpEntity decode(ObTableSingleOpEntity encoded, boolean enabled) {
        return decode(encoded, enabled, Arrays.asList("K", "Q", "T", "V"));
    }

    private static ObTableSingleOpEntity decode(ObTableSingleOpEntity encoded, boolean enabled,
                                                java.util.List<String> columns) {
        ByteBuf buf = Unpooled.wrappedBuffer(encoded.encode());
        try {
            ObTableSingleOpEntity decoded = new ObTableSingleOpEntity();
            decoded.setAggPropertiesNames(columns);
            decoded.setDecodeHBaseKqtv(enabled);
            decoded.decode(buf);
            return decoded;
        } finally {
            buf.release();
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static ObObj binary(String value) {
        ObObjMeta meta = ObObjType.ObVarcharType.getDefaultObjMeta();
        meta.setCsType(ObCollationType.CS_TYPE_BINARY);
        return new ObObj(meta, bytes(value));
    }

    private static void addCell(ObTableSingleOpEntity entity, String valueColumn, String row,
                                String qualifier, long timestamp, String value) {
        entity.addPropertyValue("K", binary(row));
        entity.addPropertyValue("Q", binary(qualifier));
        entity.addPropertyValue("T", ObObj.hbasePutInt64(timestamp));
        entity.addPropertyValue(valueColumn, binary(value));
    }
}

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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import org.junit.Assert;
import org.junit.Test;

import java.util.Random;

public class ObHbasePutCellCodecTest {

    @Test
    public void testCompactCellMatchesLegacyAtBoundaries() {
        int[] lengths = new int[] { 0, 1, 7, 16, 63, 64, 127, 128, 255, 500 };
        Random random = new Random(20260807L);
        for (int length : lengths) {
            byte[] qualifier = new byte[length + 9];
            byte[] value = new byte[length + 13];
            random.nextBytes(qualifier);
            random.nextBytes(value);
            assertCellEquals(qualifier, 4, length, -123456789L, value, 6, length, false,
                Long.MAX_VALUE);
            assertCellEquals(qualifier, 4, length, Long.MIN_VALUE + length, value, 6, length, true,
                86400000L + length);
        }
    }

    @Test
    public void testCompactCfRowsMatchesLegacyForMultipleRuns() {
        ObHbaseCfRows legacy = new ObHbaseCfRows();
        ObHbaseCfRows compact = new ObHbaseCfRows();
        legacy.setRealTableName("test$table$family");
        compact.setRealTableName("test$table$family");

        appendRun(legacy, compact, 0, Long.MAX_VALUE, 3, 11);
        appendRun(legacy, compact, 2, 3600000L, 5, 37);

        Assert.assertEquals(legacy.getPayloadContentSize(), compact.getPayloadContentSize());
        Assert.assertEquals(legacy.getPayloadSize(), compact.getPayloadSize());
        Assert.assertArrayEquals(legacy.encode(), compact.encode());
    }

    @Test
    public void testCompactStateValidation() {
        ObHbaseCfRows rows = new ObHbaseCfRows();
        rows.setRealTableName("t$f");
        rows.beginCompactKeyCells(0, 1, Long.MAX_VALUE);
        try {
            rows.getPayloadContentSize();
            Assert.fail("incomplete compact run must fail");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("incomplete"));
        }

        try {
            rows.appendCell(new ObHbaseCell(false));
            Assert.fail("compact and legacy cells must not be mixed");
        } catch (IllegalStateException expected) {
            Assert.assertTrue(expected.getMessage().contains("cannot be mixed"));
        }
    }

    @Test
    public void testCompactRoutingValuesMatchLegacy() {
        byte[] qualifier = new byte[] { 9, 8, 7, 6, 5 };
        byte[] value = new byte[] { 4, 3, 2, 1 };
        long timestamp = -20260807L;
        ObHbaseCfRows legacy = new ObHbaseCfRows();
        legacy.beginKeyCells(0, 1);
        legacy.appendCell(newLegacyCell(qualifier, 1, 3, timestamp, value, 0, value.length,
            Long.MAX_VALUE));
        ObHbaseCfRows compact = new ObHbaseCfRows();
        compact.beginCompactKeyCells(0, 1, Long.MAX_VALUE);
        compact.appendCompactCell(qualifier, 1, 3, timestamp, value, 0, value.length);

        Assert.assertEquals(legacy.getFirstCellQualifierValue(),
            compact.getFirstCellQualifierValue());
        Assert.assertEquals(legacy.getFirstCellTimestampValue(),
            compact.getFirstCellTimestampValue());
    }

    @Test
    public void testInvalidSliceRejected() {
        ObHbaseCfRows rows = new ObHbaseCfRows();
        rows.beginCompactKeyCells(0, 1, Long.MAX_VALUE);
        try {
            rows.appendCompactCell(new byte[4], 3, 2, -1, new byte[1], 0, 1);
            Assert.fail("invalid qualifier slice must fail");
        } catch (IllegalArgumentException expected) {
            Assert.assertTrue(expected.getMessage().contains("qualifier"));
        }
    }

    private static void appendRun(ObHbaseCfRows legacy, ObHbaseCfRows compact, int keyIndex,
                                  long ttl, int cellCount, int seed) {
        legacy.beginKeyCells(keyIndex, cellCount);
        compact.reserveAdditionalCompactCells(cellCount);
        compact.beginCompactKeyCells(keyIndex, cellCount, ttl);
        Random random = new Random(seed);
        for (int i = 0; i < cellCount; i++) {
            int qualifierLength = i == 0 ? 0 : 7 + i * 31;
            int valueLength = 20 + i * 73;
            byte[] qualifier = new byte[qualifierLength + 5];
            byte[] value = new byte[valueLength + 9];
            random.nextBytes(qualifier);
            random.nextBytes(value);
            long timestamp = -(1000L + i);

            ObHbaseCell legacyCell = newLegacyCell(qualifier, 2, qualifierLength, timestamp, value,
                4, valueLength, ttl);
            legacy.appendCell(legacyCell);
            compact.appendCompactCell(qualifier, 2, qualifierLength, timestamp, value, 4,
                valueLength);
        }
    }

    private static void assertCellEquals(byte[] qualifier, int qualifierOffset,
                                         int qualifierLength, long timestamp, byte[] value,
                                         int valueOffset, int valueLength, boolean hasTtl, long ttl) {
        ObHbaseCell legacy = newLegacyCell(qualifier, qualifierOffset, qualifierLength, timestamp,
            value, valueOffset, valueLength, hasTtl ? ttl : Long.MAX_VALUE);
        long compactSize = ObHbasePutCellCodec.getCellPayloadSize(qualifierLength, timestamp,
            valueLength, hasTtl, ttl);
        Assert.assertEquals(legacy.getPayloadSize(), compactSize);

        ObByteBuf compact = new ObByteBuf((int) compactSize);
        ObHbasePutCellCodec.encodeCell(compact, qualifier, qualifierOffset, qualifierLength,
            timestamp, value, valueOffset, valueLength, hasTtl, ttl);
        Assert.assertEquals(compact.bytes.length, compact.pos);
        Assert.assertArrayEquals(legacy.encode(), compact.bytes);
    }

    private static ObHbaseCell newLegacyCell(byte[] qualifier, int qualifierOffset,
                                             int qualifierLength, long timestamp, byte[] value,
                                             int valueOffset, int valueLength, long ttl) {
        boolean hasTtl = ttl != Long.MAX_VALUE;
        ObHbaseCell cell = new ObHbaseCell(hasTtl);
        cell.setQ(ObObj.hbasePutVarchar(new ObBytesString(qualifier, qualifierOffset,
            qualifierLength)));
        cell.setT(ObObj.hbasePutInt64(timestamp));
        cell.setV(ObObj.hbasePutVarchar(new ObBytesString(value, valueOffset, valueLength)));
        if (hasTtl) {
            cell.setTTL(ObObj.hbasePutInt64(ttl));
        }
        return cell;
    }
}

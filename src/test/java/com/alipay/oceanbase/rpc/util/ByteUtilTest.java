/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.util;

import org.junit.Assert;
import org.junit.Test;

import static com.alipay.oceanbase.rpc.util.ByteUtil.compareByteArrays;
import static org.junit.Assert.*;

public class ByteUtilTest {
    @Test
    public void testcompareByteArrays() {
        {
            byte[] array1 = { 1, 2, 3 };
            byte[] array2 = { 1, 2, 3 };
            Assert.assertEquals(0, compareByteArrays(array1, array2));
        }
        {
            byte[] array1 = { 2, 2, 3 };
            byte[] array2 = { 1, 2, 3 };
            Assert.assertTrue(compareByteArrays(array1, array2) > 0);
        }
        {
            byte[] array1 = { 1, 2, 3, 4 };
            byte[] array2 = { 1, 2, 3 };
            assertTrue(compareByteArrays(array1, array2) > 0);
        }
        {
            byte[] array1 = {};
            byte[] array2 = {};
            assertEquals(0, compareByteArrays(array1, array2));
        }
    }

    @Test
    public void testincrementByteArray() {
        {
            byte[] input = { 0x01, 0x02, 0x03 };
            byte[] expected = { 0x01, 0x02, 0x04 };
            assertArrayEquals(expected, ByteUtil.incrementByteArray(input));
        }
        {
            byte[] input = { (byte) 0xFF, (byte) 0xFF };
            byte[] expected = { 0x01, 0x00, 0x00 };
            assertArrayEquals(expected, ByteUtil.incrementByteArray(input));
        }
        {
            byte[] input = {};
            byte[] expected = { 0x01 };
            assertArrayEquals(expected, ByteUtil.incrementByteArray(input));
        }
        {
            byte[] expected = { 0x01 };
            assertArrayEquals(expected, ByteUtil.incrementByteArray(null));
        }
    }

    @Test
    public void testdecrementByteArray() {
        {
            byte[] input = { 0x01 };
            byte[] expected = { 0x00 };
            assertArrayEquals(expected, ByteUtil.decrementByteArray(input));
        }
        {
            byte[] input = { 0x01, 0x00 };
            byte[] expected = { 0x00, (byte) 0xFF };
            assertArrayEquals(expected, ByteUtil.decrementByteArray(input));
        }
        {
            byte[] input = { 0x02, 0x00 };
            byte[] expected = { 0x01, (byte) 0xFF };
            assertArrayEquals(expected, ByteUtil.decrementByteArray(input));
        }
        {
            byte[] input = { 0x01, 0x00, 0x00 };
            byte[] expected = { 0x00, (byte) 0xFF, (byte) 0xFF };
            assertArrayEquals(expected, ByteUtil.decrementByteArray(input));
        }
        {
            byte[] input = { (byte) 0xFF, (byte) 0xFF };
            byte[] expected = { (byte) 0xFF, (byte) 0xFE };
            assertArrayEquals(expected, ByteUtil.decrementByteArray(input));
        }
    }
}

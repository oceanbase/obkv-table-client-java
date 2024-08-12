/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

public class ByteUtil {
    /**
     * Compares two byte arrays lexicographically.  
     *
     * This method interprets each byte as an unsigned value between 0 and 255   
     * and performs a comparison on the arrays byte-by-byte. If two corresponding  
     * bytes differ, it returns the difference between the first non-equal byte   
     * values (treated as unsigned). If one array is a prefix of the other,   
     * the shorter array is considered to be less than the longer one.  
     *
     * @param array1 the first byte array to be compared  
     * @param array2 the second byte array to be compared  
     * @return a negative integer if array1 comes before array2,   
     *         a positive integer if array1 comes after array2,   
     *         or zero if they are equal  
     */
    static public int compareByteArrays(byte[] array1, byte[] array2) {
        for (int i = 0; i < Math.min(array1.length, array2.length); i++) {
            int a = (array1[i] & 0xFF);
            int b = (array2[i] & 0xFF);
            if (a != b) {
                return a - b;
            }
        }
        return array1.length - array2.length;
    }

    /**
     * Increments a byte array treated as an unsigned integer by one.  
     *
     * This method treats the input byte array as a non-negative big-endian   
     * integer, and increments its value by 1. The most significant byte is at   
     * the beginning of the array. If the increment operation causes an overflow,  
     * the resulting array will be extended to accommodate the carry.  
     *
     * @param input the byte array representing a non-negative integer  
     * @return a new byte array representing the incremented value  
     */
    static public byte[] incrementByteArray(byte[] input) {
        if (input == null || input.length == 0) {
            return new byte[] { 1 };
        }
        byte[] result = input.clone();
        for (int i = result.length - 1; i >= 0; i--) {
            result[i] += 1;

            if ((result[i] & 0xFF) != 0) {
                return result;
            }
            result[i] = 0;
        }

        byte[] extendedResult = new byte[result.length + 1];
        extendedResult[0] = 1;
        return extendedResult;
    }

    /**
     * Decrements a byte array treated as an unsigned integer by one.  
     *
     * This method treats the input byte array as a non-negative big-endian   
     * integer, and decrements its value by 1. If the entire array represents  
     * zero, the function should handle it appropriately, typically returning   
     * an array representing zero.  
     *
     * @param input the byte array representing a non-negative integer  
     * @return a new byte array representing the decremented value  
     * @throws IllegalArgumentException if the input represents zero  
     */
    static public byte[] decrementByteArray(byte[] input) {
        if (input == null || input.length == 0 || isZero(input)) {
            throw new IllegalArgumentException("Input array must represent a positive integer.");
        }

        byte[] result = input.clone();

        // Traverse the array from the least significant byte to the most  
        for (int i = result.length - 1; i >= 0; i--) {
            if ((result[i] & 0xFF) > 0) {
                result[i] -= 1;
                return result;
            }

            // If the byte is zero, we need to borrow from the next  
            result[i] = (byte) 0xFF; // Set current byte to 255 after borrow  
        }

        // Handle cases where we subtract from a number like 1000 -> 999  
        if (result[0] == 0) {
            byte[] shortenedResult = new byte[result.length - 1];
            System.arraycopy(result, 1, shortenedResult, 0, shortenedResult.length);
            return shortenedResult;
        }

        return result;
    }

    /**
     * Helper method to check if a byte array represents zero.  
     *
     * @param array the byte array to check  
     * @return true if the byte array is zero, false otherwise  
     */
    static private boolean isZero(byte[] array) {
        for (byte b : array) {
            if (b != 0) {
                return false;
            }
        }
        return true;
    }

}
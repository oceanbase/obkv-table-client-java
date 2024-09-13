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

package com.alipay.oceanbase.rpc.direct_load.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadLogger;
import com.alipay.oceanbase.rpc.direct_load.exception.*;

import io.netty.util.internal.ObjectUtil;

public class ObDirectLoadUtil {

    private ObDirectLoadUtil() {
        ObjectUtil.checkNotNull(null, null);
    }

    /**
     * Checks that the given argument is not null.
     */
    public static <T> T checkNotNull(T arg, final String name, ObDirectLoadLogger logger)
                                                                                         throws ObDirectLoadException {
        if (arg == null) {
            logger.warn("Param '" + name + "' must not be null");
            throw new ObDirectLoadIllegalArgumentException("Param '" + name + "' must not be null");
        }
        return arg;
    }

    /**
     * Checks that the given argument is in range [start, end].
     */
    public static int checkInRange(int i, int start, int end, final String name,
                                   ObDirectLoadLogger logger) throws ObDirectLoadException {
        if (i < start || i > end) {
            logger.warn("Param '" + name + "' is illegal value (expected: " + start + "-" + end
                        + "), value:" + i);
            throw new ObDirectLoadIllegalArgumentException("Param '" + name
                                                           + "' is illegal value (expected: "
                                                           + start + "-" + end + "), value:" + i);
        }
        return i;
    }

    /**
     * Checks that the given argument is strictly positive.
     */
    public static int checkPositive(int i, String name, ObDirectLoadLogger logger)
                                                                                  throws ObDirectLoadException {
        if (i <= 0) {
            logger.warn("Param '" + name + "' is illegal value (expected: > 0), value:" + i);
            throw new ObDirectLoadIllegalArgumentException(
                "Param '" + name + "' is illegal value (expected: > 0), value:" + i);
        }
        return i;
    }

    /**
     * Checks that the given argument is strictly positive.
     */
    public static long checkPositive(long l, String name, ObDirectLoadLogger logger)
                                                                                    throws ObDirectLoadException {
        if (l <= 0) {
            logger.warn("Param '" + name + "' is illegal value (expected: > 0), value:" + l);
            throw new ObDirectLoadIllegalArgumentException(
                "Param '" + name + "' is illegal value (expected: > 0), value:" + l);
        }
        return l;
    }

    /**
     * Checks that the given argument is positive or zero.
     */
    public static int checkPositiveOrZero(int i, String name, ObDirectLoadLogger logger)
                                                                                        throws ObDirectLoadException {
        if (i < 0) {
            logger.warn("Param '" + name + "' is illegal value (expected: >= 0), value:" + i);
            throw new ObDirectLoadIllegalArgumentException(
                "Param '" + name + "' is illegal value (expected: >= 0), value:" + i);
        }
        return i;
    }

    /**
     * Checks that the given argument is positive or zero.
     */
    public static long checkPositiveOrZero(long l, String name, ObDirectLoadLogger logger)
                                                                                          throws ObDirectLoadException {
        if (l < 0) {
            logger.warn("Param '" + name + "' is illegal value (expected: >= 0), value:" + l);
            throw new ObDirectLoadIllegalArgumentException(
                "Param '" + name + "' is illegal value (expected: >= 0), value:" + l);
        }
        return l;
    }

    /**
     * Checks that the given argument is valid.
     */
    public static <T> T checkNonValid(T value, T invalidValue, String name,
                                      ObDirectLoadLogger logger) throws ObDirectLoadException {
        if (value == null || value.equals(invalidValue)) {
            logger.warn("Param '" + name + "' must not be null or invalid value, value:" + value);
            throw new ObDirectLoadIllegalArgumentException(
                "Param '" + name + "' must not be null or invalid value, value:" + value);
        }
        return value;
    }

    /**
     * Checks that the given argument is neither null nor empty.
     */
    public static <T> T[] checkNonEmpty(T[] array, final String name, ObDirectLoadLogger logger)
                                                                                                throws ObDirectLoadException {
        if (array == null || array.length == 0) {
            logger.warn("Param '" + name + "' must not be null or empty, value:"
                        + Arrays.toString(array));
            throw new ObDirectLoadIllegalArgumentException("Param '" + name
                                                           + "' must not be null or empty, value:"
                                                           + Arrays.toString(array));
        }
        return array;
    }

    /**
     * Checks that the given argument is neither null nor empty.
     */
    public static byte[] checkNonEmpty(byte[] array, final String name, ObDirectLoadLogger logger)
                                                                                                  throws ObDirectLoadException {
        if (array == null || array.length == 0) {
            logger.warn("Param '" + name + "' must not be null or empty, value:"
                        + Arrays.toString(array));
            throw new ObDirectLoadIllegalArgumentException("Param '" + name
                                                           + "' must not be null or empty, value:"
                                                           + Arrays.toString(array));
        }
        return array;
    }

    /**
     * Checks that the given argument is neither null nor empty.
     */
    public static char[] checkNonEmpty(char[] array, final String name, ObDirectLoadLogger logger)
                                                                                                  throws ObDirectLoadException {
        if (array == null || array.length == 0) {
            logger.warn("Param '" + name + "' must not be null or empty, value:"
                        + Arrays.toString(array));
            throw new ObDirectLoadIllegalArgumentException("Param '" + name
                                                           + "' must not be null or empty, value:"
                                                           + Arrays.toString(array));
        }
        return array;
    }

    /**
     * Checks that the given argument is neither null nor empty.
     */
    public static <T extends Collection<?>> T checkNonEmpty(T collection, final String name,
                                                            ObDirectLoadLogger logger)
                                                                                      throws ObDirectLoadException {
        if (collection == null || collection.isEmpty()) {
            logger.warn("Param '" + name + "' must not be null or empty, value:" + collection);
            throw new ObDirectLoadIllegalArgumentException("Param '" + name
                                                           + "' must not be null or empty, value:"
                                                           + collection);
        }
        return collection;
    }

    /**
     * Checks that the given argument is neither null nor empty.
     */
    public static String checkNonEmpty(final String value, final String name,
                                       ObDirectLoadLogger logger) throws ObDirectLoadException {
        if (value == null || value.isEmpty()) {
            logger.warn("Param '" + name + "' must not be null or empty, value:" + value);
            throw new ObDirectLoadIllegalArgumentException("Param '" + name
                                                           + "' must not be null or empty, value:"
                                                           + value);
        }
        return value;
    }

    /**
     * Checks that the given argument is neither null nor empty.
     */
    public static <K, V, T extends Map<K, V>> T checkNonEmpty(T value, final String name,
                                                              ObDirectLoadLogger logger)
                                                                                        throws ObDirectLoadException {
        if (value == null || value.isEmpty()) {
            logger.warn("Param '" + name + "' must not be null or empty, value:" + value);
            throw new ObDirectLoadIllegalArgumentException("Param '" + name
                                                           + "' must not be null or empty, value:"
                                                           + value);
        }
        return value;
    }

    /**
     * Checks that the given argument is not null.
     */
    public static String checkNonEmptyArrayParam(String value, int index, String name,
                                                 ObDirectLoadLogger logger)
                                                                           throws ObDirectLoadException {
        if (value == null || value.isEmpty()) {
            logger.warn("Array index " + index + " of parameter '" + name
                        + "' must not be null or empty, value:" + value);
            throw new ObDirectLoadIllegalArgumentException("Array index " + index
                                                           + " of parameter '" + name
                                                           + "' must not be null or empty, value:"
                                                           + value);
        }
        return value;
    }

    /**
     * Check that the given array is neither null nor empty and does not contain elements
     * is neither null nor empty elements.
     */
    public static String[] deepCheckNonEmpty(String[] array, final String name,
                                             ObDirectLoadLogger logger)
                                                                       throws ObDirectLoadException {
        checkNonEmpty(array, name, logger);
        for (int index = 0; index < array.length; ++index) {
            checkNonEmptyArrayParam(array[index], index, name, logger);
        }
        return array;
    }

    /**
     * Check that the given array is neither null nor empty and does not contain elements
     * duplicate elements.
     */
    public static String[] checkNonEmptyAndUnique(String[] array, final String name,
                                                  ObDirectLoadLogger logger)
                                                                            throws ObDirectLoadException {
        deepCheckNonEmpty(array, name, logger);
        for (int i = 0; i < array.length - 1; ++i) {
            for (int j = i + 1; j < array.length; ++j) {
                if (array[i].equals(array[j])) {
                    logger.warn("Param '" + name + "' must not contain repeated elements, array:"
                                + Arrays.toString(array));
                    throw new ObDirectLoadIllegalArgumentException(
                        "Param '" + name + "' must not contain repeated elements, array:"
                                + Arrays.toString(array));
                }
            }
        }
        return array;
    }

}

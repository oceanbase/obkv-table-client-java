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

import java.util.*;

public enum ObTableOperationType {

    GET(0), INSERT(1), DEL(2), UPDATE(3), //
    INSERT_OR_UPDATE(4), // INSERT or UPDATE, columns not in arguments will remain unchanged
    REPLACE(5), // DELETE & INSERT, columns not in arguments will change to default value
    INCREMENT(6), // the column must be can be cast to long. if exist increase, else  insert
    APPEND(7), // append column value
    SCAN(8), // query
    TTL(9), // observer internal type, not used by client
    CHECK_AND_INSERT_UP(10), PUT(11), // override row
    TRIGGER(12), // internal op type
    INVALID(15);

    private int                                       value;
    private static Map<Integer, ObTableOperationType> map             = new HashMap<Integer, ObTableOperationType>();
    private static final List<Boolean>                needEncodeQuery = Arrays.asList(false, // GET
                                                                          false, // INSERT
                                                                          false, // DEL
                                                                          false, // UPDATE
                                                                          false, // INSERT_OR_UPDATE
                                                                          false, // REPLACE
                                                                          false, // INCREMENT
                                                                          false, // APPEND
                                                                          false, // SCAN
                                                                          false, // TTL
                                                                          true, // CHECK_AND_INSERT_UP
                                                                          false, // PUT
                                                                          false // INVALID
                                                                          );

    ObTableOperationType(int value) {
        this.value = value;
    }

    static {
        for (ObTableOperationType type : ObTableOperationType.values()) {
            map.put(type.value, type);
        }
    }

    /*
     * Value of.
     */
    public static ObTableOperationType valueOf(int value) {
        return map.get(value);
    }

    /*
     * Get value.
     */
    public int getValue() {
        return value;
    }

    /*
     * Get byte value.
     */
    public byte getByteValue() {
        return (byte) value;
    }

    /*
     * Is readonly.
     */
    public boolean isReadonly() {
        return this.value == GET.value;
    }

    public static boolean needEncodeQuery(ObTableOperationType type) {
        return needEncodeQuery.get(type.value);
    }
}

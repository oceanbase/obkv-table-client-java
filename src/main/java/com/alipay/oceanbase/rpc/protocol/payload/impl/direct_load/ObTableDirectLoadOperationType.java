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

package com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load;

import java.util.HashMap;
import java.util.Map;

public enum ObTableDirectLoadOperationType {

    BEGIN(0), COMMIT(1), ABORT(2), GET_STATUS(3), INSERT(4), HEART_BEAT(5), DETACH(6), MAX_TYPE(7);

    private final int                                                 value;
    private static final Map<Integer, ObTableDirectLoadOperationType> map = new HashMap<Integer, ObTableDirectLoadOperationType>();

    static {
        for (ObTableDirectLoadOperationType type : ObTableDirectLoadOperationType.values()) {
            map.put(type.value, type);
        }
    }

    public static ObTableDirectLoadOperationType valueOf(int value) {
        return map.get(value);
    }

    ObTableDirectLoadOperationType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public byte getByteValue() {
        return (byte) value;
    }

}

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import java.util.*;

public enum OHOperationType {
    INVALID(0),
    PUT(1),
    PUT_LIST(2),
    DELETE(3),
    DELETE_LIST(4),
    GET(5),
    GET_LIST(6),
    EXISTS(7),
    EXISTS_LIST(8),
    BATCH(9),
    BATCH_CALLBACK(10),
    SCAN(11),
    CHECK_AND_PUT(12),
    CHECK_AND_DELETE(13),
    CHECK_AND_MUTATE(14),
    APPEND(15),
    INCREMENT(16),
    INCREMENT_COLUMN_VALUE(17),
    MUTATE_ROW(18);

    private final int                                  value;
    private static final Map<Integer, OHOperationType> map = new HashMap<Integer, OHOperationType>();

    static {
        for (OHOperationType type : OHOperationType.values()) {
            map.put(type.value, type);
        }
    }

    OHOperationType(int value) {
        this.value = value;
    }

    public static OHOperationType valueOf(int value) {
        return map.get(value);
    }

    public int getValue() {
        return value;
    }

    public byte getByteValue() {
        return (byte) value;
    }
}

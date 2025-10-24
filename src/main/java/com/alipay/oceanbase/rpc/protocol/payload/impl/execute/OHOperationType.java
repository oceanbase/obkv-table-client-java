package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import java.util.*;

public enum OHOperationType {
    PUT(0),
    PUT_LIST(1),
    DELETE(2),
    DELETE_LIST(3),
    GET(4),
    GET_LIST(5),
    EXISTS(6),
    EXISTS_LIST(7),
    BATCH(8),
    BATCH_CALLBACK(9),
    SCAN(10),
    CHECK_AND_PUT(11),
    CHECK_AND_DELETE(12),
    CHECK_AND_MUTATE(13),
    APPEND(14),
    INCREMENT(15),
    INCREMENT_COLUMN_VALUE(16),
    MUTATE_ROW(17);

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

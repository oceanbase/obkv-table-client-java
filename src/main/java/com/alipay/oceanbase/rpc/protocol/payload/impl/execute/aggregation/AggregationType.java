package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation;

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;

import java.util.HashMap;
import java.util.Map;

public enum AggregationType {
    MAX(0),
    MIN(1),
    COUNT(2),
    SUM(3),
    AVG(4);

    public byte getByteValue() {
        return (byte) value;
    }
    AggregationType(int value_) {
        this.value = value_;
    }
    private int value;
};

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import java.util.HashMap;
import java.util.Map;

public enum ObBinlogRowImageType {
    MINIMAL(0), NOBLOB(1), FULL(2);
    private int value;
    private static Map<Integer, ObBinlogRowImageType> map             = new HashMap<Integer, ObBinlogRowImageType>();
    ObBinlogRowImageType(int value) { this.value = value; }
    static {
        for (ObBinlogRowImageType type : ObBinlogRowImageType.values()) {
            map.put(type.value, type);
        }
    }

    /*
     * Value of.
     */
    public static ObBinlogRowImageType valueOf(int value) {
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
}

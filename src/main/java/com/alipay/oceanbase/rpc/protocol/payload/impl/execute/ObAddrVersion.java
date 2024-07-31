package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

public enum ObAddrVersion {
    ObAddrIPV4((byte) 4),
    ObAddrIPV6((byte) 6);

    private final byte value;

    ObAddrVersion(byte value) {
        this.value = value;
    }

    public byte getValue() {
        return value;
    }

    public static ObAddrVersion fromValue(int value){
        for (ObAddrVersion obAddrVersion : values()) {
            if (obAddrVersion.value == value) {
                return obAddrVersion;
            }
        }
        return ObAddrIPV4; //默认使用IPV4, 或者抛异常。
    }
}
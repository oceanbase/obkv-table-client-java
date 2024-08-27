package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

public enum ObFetchPartitionMetaType {
    GET_PARTITION_META("GET_PARTITION_META", 0), INVALID("INVALID", 1);

    private final String name;
    private final int   index;

    ObFetchPartitionMetaType(String name, int index) {
        this.name = name;
        this.index = index;
    }

    /*
     * Value of.
     */
    public static ObFetchPartitionMetaType valueOf(int index) {
        if (GET_PARTITION_META.index == index) {
            return GET_PARTITION_META;
        } else {
            return INVALID;
        }
    }

    /*
     * Get index.
     */
    public int getIndex() {
        return index;
    }

    /*
     * Get name.
     */
    public String getName() {
        return name;
    }

}

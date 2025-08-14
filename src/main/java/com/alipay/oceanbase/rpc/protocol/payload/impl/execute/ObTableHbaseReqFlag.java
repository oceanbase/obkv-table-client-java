package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

public class ObTableHbaseReqFlag {
    private static final int FLAG_SERVER_CAN_RETRY             = 1 << 0;
    private long flags = 0;

    public long getValue() {
        return flags;
    }

    public void setValue(long flags) {
        this.flags = flags;
    }

    public void setFlagServerCanRetry(boolean serverCanRetry) {
        if (serverCanRetry) {
            flags |= FLAG_SERVER_CAN_RETRY;
        } else {
            flags &= ~FLAG_SERVER_CAN_RETRY;
        }
    }

    public boolean getFlagServerCanRetry() {
        return (this.flags & FLAG_SERVER_CAN_RETRY) == 1;
    }
}

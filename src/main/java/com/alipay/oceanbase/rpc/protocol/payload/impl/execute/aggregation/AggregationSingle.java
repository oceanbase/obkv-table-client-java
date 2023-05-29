package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation;

import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation.AggregationType;
import com.alipay.oceanbase.rpc.util.Serialization;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class AggregationSingle {
    public AggregationSingle(AggregationType aggregationType_, String column_) {
        this.agg_column = column_;
        this.agg_type = aggregationType_;
    }
    public int GetTypeNumber() {
        return agg_type.getByteValue();
    }
    public String GetAggregationColumn() {
        return this.agg_column;
    }

    public byte[] encode() {
        byte[] bytes = new byte[(int) this.getPayloadSize()];
        int idx = 0;

        int headerLen = (int) getObUniVersionHeaderLength(version, this.getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(version, this.getPayloadContentSize()), 0, bytes,
                idx, headerLen);
        idx += headerLen;

        int len =  Serialization.getNeedBytes(agg_type.getByteValue());
        System.arraycopy(Serialization.encodeI8(agg_type.getByteValue()), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(agg_column);
        System.arraycopy(Serialization.encodeVString(agg_column), 0, bytes, idx, len);

        return bytes;
    }

    public long getPayloadSize() {
        return getObUniVersionHeaderLength(version, this.getPayloadContentSize()) + this.getPayloadContentSize();
    }

    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(agg_type.getByteValue()) + Serialization.getNeedBytes(agg_column);
    }
    private AggregationType agg_type;
    private String agg_column;

    private final int version = 1;
}

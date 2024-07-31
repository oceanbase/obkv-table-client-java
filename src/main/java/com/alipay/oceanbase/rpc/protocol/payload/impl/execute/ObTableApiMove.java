package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

public class ObTableApiMove extends AbstractPayload {

    private ObTableMoveReplicaInfo replica;
    private long reserved;

    public ObTableApiMove() {
        replica = new ObTableMoveReplicaInfo();
        reserved = 0L;
    }

    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_MOVE;
    }

    @Override
    protected int encodeHeader(byte[] bytes, int idx) {
        return super.encodeHeader(bytes, idx);
    }

    public ObTableMoveReplicaInfo getReplica() {
        return replica;
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);
        System.arraycopy(replica.encode(), 0, bytes, idx, Serialization.getNeedBytes(replica.encode()));
        System.arraycopy(Serialization.encodeVi64(reserved), 0, bytes, idx, Serialization.getNeedBytes(reserved));

        return bytes;
    }

    @Override
    public ObTableApiMove decode(ByteBuf buf) {
        super.decode(buf);

        replica.decode(buf);

        reserved = Serialization.decodeVi64(buf);
        return this;
    }

    @Override
    public long getPayloadContentSize() {
        return 0;
    }
}

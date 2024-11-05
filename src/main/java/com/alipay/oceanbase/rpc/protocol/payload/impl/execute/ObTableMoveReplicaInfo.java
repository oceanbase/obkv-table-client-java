/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.location.model.ObReplicaType;
import com.alipay.oceanbase.rpc.location.model.ObServerRole;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.ObGlobal.OB_VERSION;

public class ObTableMoveReplicaInfo extends AbstractPayload {

    private long          tableId;
    private long          schemaVersion;
    private long          partitionId;
    private ObAddr        server;
    private ObServerRole  obServerRole;
    private ObReplicaType obReplicaType;
    private long          partRenewTime;
    private long          reserved;

    public ObTableMoveReplicaInfo() {
        server = new ObAddr();
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);
        System.arraycopy(Serialization.encodeVi64(tableId), 0, bytes, idx,
            Serialization.getNeedBytes(tableId));
        System.arraycopy(Serialization.encodeVi64(schemaVersion), 0, bytes, idx,
            Serialization.getNeedBytes(schemaVersion));
        if (OB_VERSION >= 4) {
            System.arraycopy(Serialization.encodeI64(partitionId), 0, bytes, idx,
                Serialization.getNeedBytes(partitionId));
        } else {
            System.arraycopy(Serialization.encodeVi64(partitionId), 0, bytes, idx,
                Serialization.getNeedBytes(partitionId));
        }
        System.arraycopy(server.encode(), 0, bytes, idx,
            Serialization.getNeedBytes(server.encode()));
        System.arraycopy(Serialization.encodeVi32(obServerRole.getIndex()), 0, bytes, idx,
            Serialization.getNeedBytes(obServerRole.getIndex()));
        System.arraycopy(Serialization.encodeVi32(obReplicaType.getIndex()), 0, bytes, idx,
            Serialization.getNeedBytes(obReplicaType.getIndex()));
        System.arraycopy(Serialization.encodeVi64(partRenewTime), 0, bytes, idx,
            Serialization.getNeedBytes(partRenewTime));
        System.arraycopy(Serialization.encodeVi64(reserved), 0, bytes, idx,
            Serialization.getNeedBytes(reserved));
        return bytes;
    }

    @Override
    public long getPayloadContentSize() {
        return 0;
    }

    public ObAddr getServer() {
        return server;
    }

    @Override
    public ObTableMoveReplicaInfo decode(ByteBuf buf) {
        super.decode(buf);
        tableId = Serialization.decodeVi64(buf);
        schemaVersion = Serialization.decodeVi64(buf);
        if (OB_VERSION >= 4) {
            partitionId = Serialization.decodeI64(buf);
        } else {
            partitionId = Serialization.decodeVi64(buf);
        }
        server.decode(buf);
        obServerRole = ObServerRole.getRole(Serialization.decodeI8(buf));
        obReplicaType = ObReplicaType.getReplicaType(Serialization.decodeVi32(buf));
        partRenewTime = Serialization.decodeVi64(buf);
        reserved = Serialization.decodeVi64(buf);
        return this;
    }
}

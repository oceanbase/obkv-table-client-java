/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
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

package com.alipay.oceanbase.rpc.bolt.protocol;

import com.alipay.oceanbase.rpc.exception.ObTableRoutingWrongException;
import com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableDirectLoadResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableBatchOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableLSOpResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.login.ObTableLoginResult;
import com.alipay.remoting.CommandCode;

public enum ObTablePacketCode implements CommandCode {

    OB_TABLE_API_LOGIN(Pcodes.OB_TABLE_API_LOGIN) {
        /*
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            return new ObTableLoginResult();
        }
    }, //
    OB_TABLE_API_EXECUTE(Pcodes.OB_TABLE_API_EXECUTE) {
        /*
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            return new ObTableOperationResult();
        }
    }, //
    OB_TABLE_API_BATCH_EXECUTE(Pcodes.OB_TABLE_API_BATCH_EXECUTE) {
        /*
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            return new ObTableBatchOperationResult();
        }
    }, //
    OB_TABLE_API_EXECUTE_QUERY(Pcodes.OB_TABLE_API_EXECUTE_QUERY) {
        /*
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            return new ObTableQueryResult(header);
        }
    }, //
    OB_TABLE_API_QUERY_AND_MUTATE(Pcodes.OB_TABLE_API_QUERY_AND_MUTATE) {
        /*
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            return new ObTableQueryAndMutateResult();
        }
    }, //
    OB_TABLE_API_EXECUTE_QUERY_SYNC(Pcodes.OB_TABLE_API_EXECUTE_QUERY_SYNC) {
        /**
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            return new ObTableQueryAsyncResult();
        }
    }, //
    OB_TABLE_API_DIRECT_LOAD(Pcodes.OB_TABLE_API_DIRECT_LOAD) {
        /**
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            return new ObTableDirectLoadResult();
        }
    }, //
    OB_TABLE_API_LS_EXECUTE(Pcodes.OB_TABLE_API_LS_EXECUTE) {
        /**
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            return new ObTableLSOpResult();
        }
    }, //
    OB_TABLE_API_MOVE(Pcodes.OB_TABLE_API_MOVE) {
        /**
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            throw new ObTableRoutingWrongException(
                "Receive rerouting response packet. "
                        + "Java client is not supported and need to Refresh table router entry");
        }
    }, //
    OB_ERROR_PACKET(Pcodes.OB_ERROR_PACKET) {
        /*
         * New payload.
         */
        @Override
        public ObPayload newPayload(ObRpcPacketHeader header) {
            throw new IllegalArgumentException("OB_ERROR_PACKET has no payload implementation");
        }
    };

    private short value;

    ObTablePacketCode(int value) {
        this.value = (short) value;
    }

    /*
     * Value.
     */
    public short value() {
        return this.value;
    }

    /*
     * Value of.
     */
    public static ObTablePacketCode valueOf(short value) {
        switch (value) {
            case Pcodes.OB_TABLE_API_LOGIN:
                return OB_TABLE_API_LOGIN;
            case Pcodes.OB_TABLE_API_EXECUTE:
                return OB_TABLE_API_EXECUTE;
            case Pcodes.OB_TABLE_API_BATCH_EXECUTE:
                return OB_TABLE_API_BATCH_EXECUTE;
            case Pcodes.OB_TABLE_API_EXECUTE_QUERY:
                return OB_TABLE_API_EXECUTE_QUERY;
            case Pcodes.OB_TABLE_API_QUERY_AND_MUTATE:
                return OB_TABLE_API_QUERY_AND_MUTATE;
            case Pcodes.OB_TABLE_API_EXECUTE_QUERY_SYNC:
                return OB_TABLE_API_EXECUTE_QUERY_SYNC;
            case Pcodes.OB_TABLE_API_DIRECT_LOAD:
                return OB_TABLE_API_DIRECT_LOAD;
            case Pcodes.OB_TABLE_API_LS_EXECUTE:
                return OB_TABLE_API_LS_EXECUTE;
            case Pcodes.OB_TABLE_API_MOVE:
                throw new ObTableRoutingWrongException(
                    "Receive rerouting response packet. "
                            + "Java client is not supported and need to Refresh table router entry");
            case Pcodes.OB_ERROR_PACKET:
                return OB_ERROR_PACKET;
        }
        throw new IllegalArgumentException("Unknown Rpc command code value ," + value);
    }

    public abstract ObPayload newPayload(ObRpcPacketHeader header);

}

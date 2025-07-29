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

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class ObHbaseResult extends AbstractPayload {
    private ObTableResult           header        = new ObTableResult();                  // errno + msg + sql_state
    private ObTableOperationType    operationType = ObTableOperationType.INSERT_OR_UPDATE; // HBase Put
    private List<ObHbaseCellResult> cellResults;                                          // for errno feedback and HBase Get

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_HBASE_EXECUTE;
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        return bytes;
    }

    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode version
        super.decode(buf);

        // 1. decode errno + sqlstate + msg
        int errno = Serialization.decodeVi32(buf);
        byte[] sqlState = Serialization.decodeBytes(buf);
        byte[] msg = Serialization.decodeBytes(buf);
        this.header.setErrno(errno);
        this.header.setSqlState(sqlState);
        this.header.setMsg(msg);

        // 2. encode operationType
        this.operationType = ObTableOperationType.valueOf(Serialization.decodeI8(buf.readByte()));

        // 3. decode cell results if HBase Get, otherwise the len will be 0
        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; ++i) {
            ObHbaseCellResult cellResult = new ObHbaseCellResult();
            cellResult.decode(buf);
            this.cellResults.add(cellResult);
        }

        return this;
    }

    @Override
    public long getPayloadContentSize() {
        return 0;
    }

    public ObTableResult getHeader() {
        return this.header;
    }

    public ObTableOperationType getOperationType() {
        return operationType;
    }

    public List<ObHbaseCellResult> getCellResults() {
        return cellResults;
    }
}

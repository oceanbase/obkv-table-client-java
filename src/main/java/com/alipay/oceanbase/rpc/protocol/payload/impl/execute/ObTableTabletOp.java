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
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutate;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/*
OB_UNIS_DEF_SERIALIZE(ObTableTabletOp,
                      table_id_,
                      tablet_id_,
                      option_flag_,
                      single_ops_);
 */
public class ObTableTabletOp extends AbstractPayload {
    private List<ObTableSingleOp> singleOperations = new ArrayList<>();
    private long tabletId = Constants.INVALID_TABLET_ID; // i64
    ObTableTabletOpFlag optionFlag = new ObTableTabletOpFlag();

    private static final int tabletIdSize = 8;

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode tablet id
        System.arraycopy(Serialization.encodeI64(tabletId), 0, bytes, idx, 8);
        idx += 8;

        // 2. encode option flag
        int len = Serialization.getNeedBytes(optionFlag.getValue());
        System.arraycopy(Serialization.encodeVi64(optionFlag.getValue()), 0, bytes, idx, len);
        idx += len;

        // 4. encode Operation
        len = Serialization.getNeedBytes(singleOperations.size());
        System.arraycopy(Serialization.encodeVi64(singleOperations.size()), 0, bytes, idx, len);
        idx += len;
        for (ObTableSingleOp singleOperation : singleOperations) {
            len = (int) singleOperation.getPayloadSize();
            System.arraycopy(singleOperation.encode(), 0, bytes, idx, len);
            idx += len;
        }

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode header
        super.decode(buf);

        // 1. decode tablet id
        this.tabletId = Serialization.decodeI64(buf);

        // 2. decode other flags
        long flagsValue = Serialization.decodeVi64(buf);
        optionFlag.setValue(flagsValue);

        // 3. decode Operation
        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            ObTableSingleOp singleOperation = new ObTableSingleOp();
            singleOperation.decode(buf);
            singleOperations.add(singleOperation);
        }

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long payloadContentSize = 0;
        payloadContentSize += Serialization.getNeedBytes(singleOperations.size());
        for (ObTableSingleOp operation : singleOperations) {
            payloadContentSize += operation.getPayloadSize();
        }

        return payloadContentSize + tabletIdSize + Serialization.getNeedBytes(optionFlag.getValue());
    }

    /*
     * Get table operations.
     */
    public List<ObTableSingleOp> getSingleOperations() {
        return singleOperations;
    }

    /*
     * Set table operations.
     */
    public void setSingleOperations(List<ObTableSingleOp> singleOperations) {
        setIsSameType(true);
        ObTableOperationType prevType = null;
        for (ObTableSingleOp o : singleOperations) {
            if (prevType != null && prevType != o.getSingleOpType()) {
                setIsSameType(false);
            } else {
                prevType = o.getSingleOpType();
            }
        }
        this.singleOperations = singleOperations;
    }

    public void setTabletId(long tabletId) {
        this.tabletId = tabletId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public boolean isSameType() { return optionFlag.getFlagIsSameType(); }

    public void setIsSameType(boolean isSameType) { optionFlag.setFlagIsSameType(isSameType);}

}

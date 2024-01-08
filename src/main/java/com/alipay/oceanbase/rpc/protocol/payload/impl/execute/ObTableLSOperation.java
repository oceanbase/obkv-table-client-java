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
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableLSOperation extends AbstractPayload {

    private List<ObTableTabletOp> tabletOperations = new ArrayList<ObTableTabletOp>();
    private long                  lsId             = INVALID_LS_ID;                   // i64

    private static final int      LS_ID_SIZE       = 8;
    private static final long     INVALID_LS_ID    = -1;
    private ObTableLSOpFlag       optionFlag       = new ObTableLSOpFlag();

    /*
    OB_UNIS_DEF_SERIALIZE(ObTableLSOp,
                      ls_id_,
                      option_flag_,
                      tablet_ops_);
     */

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode ls id
        System.arraycopy(Serialization.encodeI64(lsId), 0, bytes, idx, 8);
        idx += 8;

        // 2. encode option flag
        int len = Serialization.getNeedBytes(optionFlag.getValue());
        System.arraycopy(Serialization.encodeVi64(optionFlag.getValue()), 0, bytes, idx, len);
        idx += len;

        // 3. encode Operation
        len = Serialization.getNeedBytes(tabletOperations.size());
        System.arraycopy(Serialization.encodeVi64(tabletOperations.size()), 0, bytes, idx, len);
        idx += len;
        for (ObTableTabletOp tabletOperation : tabletOperations) {
            len = (int) tabletOperation.getPayloadSize();
            System.arraycopy(tabletOperation.encode(), 0, bytes, idx, len);
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

        // 1. decode others
        this.lsId = Serialization.decodeI64(buf);

        // 2. decode flags
        long flagValue = Serialization.decodeVi64(buf);
        optionFlag.setValue(flagValue);

        // 3. decode Operation
        int len = (int) Serialization.decodeVi64(buf);
        tabletOperations = new ArrayList<ObTableTabletOp>(len);
        for (int i = 0; i < len; i++) {
            ObTableTabletOp tabletOperation = new ObTableTabletOp();
            tabletOperation.decode(buf);
            tabletOperations.add(tabletOperation);
        }

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long payloadContentSize = 0;
        payloadContentSize += Serialization.getNeedBytes(tabletOperations.size());
        for (ObTableTabletOp operation : tabletOperations) {
            payloadContentSize += operation.getPayloadSize();
        }

        return payloadContentSize + LS_ID_SIZE + Serialization.getNeedBytes(optionFlag.getValue());
    }

    /*
     * Get table operations.
     */
    public List<ObTableTabletOp> getTabletOperations() {
        return tabletOperations;
    }

    /*
     * Add table operation.
     */
    public void addTabletOperation(ObTableTabletOp tabletOperation) {
        this.tabletOperations.add(tabletOperation);
        int length = this.tabletOperations.size();
        if (length == 1 && tabletOperation.isSameType()) {
           setIsSameType(true);
           return;
        }

        if (isSameType()
            && length > 1
            && !(tabletOperation.isSameType() && (this.tabletOperations.get(length - 1)
                .getSingleOperations().get(0).getSingleOpType() == this.tabletOperations
                .get(length - 2).getSingleOperations().get(0).getSingleOpType()))) {
            setIsSameType(false);
        }
    }

    public void setLsId(long lsId) {
        this.lsId = lsId;
    }

    public boolean isSameType() {
        return optionFlag.getFlagIsSameType();
    }

    public void setIsSameType(boolean isSameType) {
        optionFlag.setFlagIsSameType(isSameType);
    }

}

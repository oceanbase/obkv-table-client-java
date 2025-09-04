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
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.*;

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

    private Set<String> rowKeyNamesSet = new LinkedHashSet<>();
    private Set<String> propertiesNamesSet = new LinkedHashSet<>();
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

    public void encode(ObByteBuf buf) {
        encodeHeader(buf);
        int posStart = buf.pos;

        // 1. encode tablet id
        Serialization.encodeI64(buf, tabletId);

        // 2. encode option flag
        Serialization.encodeVi64(buf, optionFlag.getValue());

        // 3. encode Operation
        Serialization.encodeVi64(buf, singleOperations.size());
        for (ObTableSingleOp singleOperation : singleOperations) {
            singleOperation.encode(buf);
        }
        int writeBufferLength = buf.pos - posStart;
        if (writeBufferLength != this.payLoadContentSize) {
            throw new IllegalArgumentException("error in encode ObTableTabletOp (" +
                    "writeBufferLength:" + writeBufferLength + ", payLoadContentSize:" + this.payLoadContentSize + ")"+
                    "ObTableTabletOp details: " + this.toString() +
                    "memberEncodeLengths: " + memberEncodeLengths.toString()); 
        }
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
        if (this.payLoadContentSize == INVALID_PAYLOAD_CONTENT_SIZE) {
            long payloadContentSize = 0;
            
            // 清空之前的长度记录
            memberEncodeLengths.clear();
            
            // 1. singleOperations 相关长度
            long singleOperationsCountSize = Serialization.getNeedBytes(singleOperations.size());
            payloadContentSize += singleOperationsCountSize;
            memberEncodeLengths.add(singleOperationsCountSize);
            
            for (ObTableSingleOp operation : singleOperations) {
                long operationSize = operation.getPayloadSize();
                payloadContentSize += operationSize;
                memberEncodeLengths.add(operationSize);
            }

            // 2. 其他固定字段长度
            long tabletIdSizeValue = tabletIdSize;
            payloadContentSize += tabletIdSizeValue;
            memberEncodeLengths.add(tabletIdSizeValue);
            
            long optionFlagSize = Serialization.getNeedBytes(optionFlag.getValue());
            payloadContentSize += optionFlagSize;
            memberEncodeLengths.add(optionFlagSize);

            this.payLoadContentSize = payloadContentSize;
        }
        return this.payLoadContentSize;
    }

    /**
     * Reset the cached payload content size and propagate to child objects
     */
    @Override
    public void resetPayloadContentSize() {
        super.resetPayloadContentSize();
        for (ObTableSingleOp operation : singleOperations) {
            if (operation != null) {
                operation.resetPayloadContentSize();
            }
        }
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
        setIsSamePropertiesNames(true);
        ObTableOperationType prevType = null;
        List<String> prevPropertiesNames = null;
        for (ObTableSingleOp o : singleOperations) {
            // get union column names
            rowKeyNamesSet.addAll(o.getQuery().getScanRangeColumns());
            for (ObTableSingleOpEntity e: o.getEntities()) {
                rowKeyNamesSet.addAll(e.getRowKeyNames());
                propertiesNamesSet.addAll(e.getPropertiesNames());

                if (!isSamePropertiesNames()) {
                    // do nothing
                } else if (prevPropertiesNames != null && isSamePropertiesNames() &&
                        !prevPropertiesNames.equals(e.getPropertiesNames())) {
                    setIsSamePropertiesNames(false);
                } else {
                    prevPropertiesNames = e.getPropertiesNames();
                }
            }

            if (!isSameType()) {
                // do nothing
            } else if (prevType != null && prevType != o.getSingleOpType()) {
                setIsSameType(false);
            } else {
                prevType = o.getSingleOpType();
            }
        }

        if (isSameType()) {
            boolean isHbaseOps = singleOperations.get(0).getQuery().isHbaseQuery();
            if ((isHbaseOps && singleOperations.get(0).getSingleOpType() == ObTableOperationType.SCAN)
                || (!isHbaseOps && singleOperations.get(0).getSingleOpType() == ObTableOperationType.GET)) {
                setIsReadOnly(true);
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

    public boolean isSamePropertiesNames() {
        return optionFlag.getFlagIsSamePropertiesNames();
    }

    public void setIsSameType(boolean isSameType) { optionFlag.setFlagIsSameType(isSameType);}

    public void setIsReadOnly(boolean isReadOnly) { optionFlag.setFlagIsReadOnly(isReadOnly);}

    public void setIsSamePropertiesNames(boolean isSamePropertiesNames) {
        optionFlag.setFlagIsSamePropertiesNames(isSamePropertiesNames);
    }

    public Set<String> getRowKeyNamesSet() {
        return rowKeyNamesSet;
    }

    public Set<String> getPropertiesNamesSet() {
        return propertiesNamesSet;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ObTableTabletOp{");
        sb.append("tabletId=").append(tabletId);
        sb.append(", optionFlag=").append(optionFlag);
        sb.append(", singleOperations=");
        if (singleOperations != null) {
            sb.append('[');
            for (int i = 0; i < singleOperations.size(); i++) {
                if (i > 0) sb.append(", ");
                sb.append("singleOp[").append(i).append("]=").append(singleOperations.get(i));
            }
            sb.append(']');
        } else {
            sb.append("null");
        }
        sb.append('}');
        return sb.toString();
    }

}

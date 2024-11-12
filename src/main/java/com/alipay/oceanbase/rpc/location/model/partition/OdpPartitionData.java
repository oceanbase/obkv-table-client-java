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

package com.alipay.oceanbase.rpc.location.model.partition;

import com.alipay.oceanbase.rpc.constant.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.OdpSinglePartKey;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class OdpPartitionData extends AbstractPayload {
    private long                   level;
    private long                   partNum;
    private String                 partExr          = Constants.EMPTY_STRING;
    private long                   partType;
    private long                   partSpace;
    private String                 partRangeType    = Constants.EMPTY_STRING;
    private long                   subPartNum;
    private String                 subPartExr       = Constants.EMPTY_STRING;
    private long                   subPartType;
    private long                   subPartSpace;
    private String                 subPartRangeType = Constants.EMPTY_STRING;
    private List<OdpSinglePartKey> singlePartKeys   = new ArrayList<OdpSinglePartKey>();

    public OdpPartitionData() {
    }

    public OdpPartitionData(ObPartitionInfo info) {
        level = info.getLevel().getIndex();
        ObPartDesc firstPartDesc = info.getFirstPartDesc();
        if (firstPartDesc != null) {
            partNum = firstPartDesc.getPartNum();
            partExr = firstPartDesc.getPartExpr();
            partType = firstPartDesc.getPartFuncType().getIndex();
            if (partType == ObPartFuncType.HASH.getIndex()) {
                partSpace = ((ObHashPartDesc) firstPartDesc).getPartSpace();
            } else if (partType == ObPartFuncType.KEY.getIndex()) {
                partSpace = ((ObKeyPartDesc) firstPartDesc).getPartSpace();
            } else if (partType == ObPartFuncType.RANGE.getIndex()) {
                ObRangePartDesc firstRangePartDesc = (ObRangePartDesc) firstPartDesc;
                partSpace = firstRangePartDesc.getPartSpace();
                StringBuilder str = new StringBuilder();
                for (ObObjType type : firstRangePartDesc.getOrderedCompareColumnTypes()) {
                    str.append(type.getValue()).append(",");
                }
                str.deleteCharAt(str.length() - 1);
                partRangeType = str.toString();
            }
            ObPartDesc subPartDesc = info.getSubPartDesc();
            if (subPartDesc != null) {
                subPartNum = subPartDesc.getPartNum();
                subPartExr = subPartDesc.getPartExpr();
                subPartType = subPartDesc.getPartFuncType().getIndex();
                if (subPartType == ObPartFuncType.HASH.getIndex()) {
                    subPartSpace = ((ObHashPartDesc) subPartDesc).getPartSpace();
                } else if (subPartType == ObPartFuncType.KEY.getIndex()) {
                    subPartSpace = ((ObKeyPartDesc) subPartDesc).getPartSpace();
                } else if (subPartType == ObPartFuncType.RANGE.getIndex()) {
                    ObRangePartDesc subRangePartDesc = (ObRangePartDesc) subPartDesc;
                    subPartSpace = subRangePartDesc.getPartSpace();
                    StringBuilder str = new StringBuilder();
                    for (ObObjType type : subRangePartDesc.getOrderedCompareColumnTypes()) {
                        str.append(type.getValue()).append(",");
                    }
                    str.deleteCharAt(str.length() - 1);
                    subPartRangeType = str.toString();
                }
            }
            for (ObColumn column : info.getPartColumns()) {
                OdpSinglePartKey partKey = new OdpSinglePartKey(column);
                singlePartKeys.add(partKey);
            }
        }
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        int len = Serialization.getNeedBytes(level);
        System.arraycopy(Serialization.encodeVi64(level), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(partNum);
        System.arraycopy(Serialization.encodeVi64(partNum), 0, bytes, idx, len);
        idx += len;
        byte[] strbytes = Serialization.encodeVString(partExr);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        len = Serialization.getNeedBytes(partType);
        System.arraycopy(Serialization.encodeVi64(partType), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(partSpace);
        System.arraycopy(Serialization.encodeVi64(partSpace), 0, bytes, idx, len);
        idx += len;
        strbytes = Serialization.encodeVString(partRangeType);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        len = Serialization.getNeedBytes(subPartNum);
        System.arraycopy(Serialization.encodeVi64(subPartNum), 0, bytes, idx, len);
        idx += len;
        strbytes = Serialization.encodeVString(subPartExr);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;
        len = Serialization.getNeedBytes(subPartType);
        System.arraycopy(Serialization.encodeVi64(subPartType), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(subPartSpace);
        System.arraycopy(Serialization.encodeVi64(subPartSpace), 0, bytes, idx, len);
        idx += len;
        strbytes = Serialization.encodeVString(subPartRangeType);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        len = Serialization.getNeedBytes(singlePartKeys.size());
        System.arraycopy(Serialization.encodeVi64(singlePartKeys.size()), 0, bytes, idx, len);
        idx += len;
        for (OdpSinglePartKey singlePartKey : singlePartKeys) {
            len = (int) singlePartKey.getPayloadSize();
            System.arraycopy(singlePartKey.encode(), 0, bytes, idx, len);
            idx += len;
        }

        return bytes;
    }

    @Override
    public OdpPartitionData decode(ByteBuf buf) {
        super.decode(buf);

        level = Serialization.decodeVi64(buf);
        if (level != 1 && level != 2) {
            return this;
        }
        partNum = Serialization.decodeVi64(buf);
        partExr = Serialization.decodeVString(buf);
        partType = Serialization.decodeVi64(buf);
        partSpace = Serialization.decodeVi64(buf);
        partRangeType = Serialization.decodeVString(buf);
        subPartNum = Serialization.decodeVi64(buf);
        subPartExr = Serialization.decodeVString(buf);
        subPartType = Serialization.decodeVi64(buf);
        subPartSpace = Serialization.decodeVi64(buf);
        subPartRangeType = Serialization.decodeVString(buf);
        long len = Serialization.decodeVi64(buf);
        for (long i = 0; i < len; ++i) {
            OdpSinglePartKey singlePartKey = new OdpSinglePartKey();
            singlePartKey.decode(buf);
            singlePartKeys.add(singlePartKey);
        }
        return this;
    }

    @Override
    public long getPayloadContentSize() {
        long size = Serialization.getNeedBytes(level) + Serialization.getNeedBytes(partNum)
                    + Serialization.getNeedBytes(partExr) + Serialization.getNeedBytes(partType)
                    + Serialization.getNeedBytes(partSpace)
                    + Serialization.getNeedBytes(partRangeType)
                    + Serialization.getNeedBytes(subPartNum)
                    + Serialization.getNeedBytes(subPartExr)
                    + Serialization.getNeedBytes(subPartType)
                    + Serialization.getNeedBytes(subPartSpace)
                    + Serialization.getNeedBytes(subPartRangeType);
        size += Serialization.getNeedBytes(singlePartKeys.size());
        for (OdpSinglePartKey singlePartKey : singlePartKeys) {
            size += singlePartKey.getPayloadSize();
        }

        return size;
    }

    public long getLevel() {
        return level;
    }

    public long getPartNum() {
        return partNum;
    }

    public String getPartExr() {
        return partExr;
    }

    public long getPartType() {
        return partType;
    }

    public long getPartSpace() {
        return partSpace;
    }

    public String getPartRangeType() {
        return partRangeType;
    }

    public long getSubPartNum() {
        return subPartNum;
    }

    public String getSubPartExr() {
        return subPartExr;
    }

    public long getSubPartType() {
        return subPartType;
    }

    public long getSubPartSpace() {
        return subPartSpace;
    }

    public String getSubPartRangeType() {
        return subPartRangeType;
    }

    public List<OdpSinglePartKey> getSinglePartKeys() {
        return singlePartKeys;
    }
}

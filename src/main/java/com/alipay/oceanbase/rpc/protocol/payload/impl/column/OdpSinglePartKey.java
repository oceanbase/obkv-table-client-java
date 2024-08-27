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

package com.alipay.oceanbase.rpc.protocol.payload.impl.column;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

public class OdpSinglePartKey extends AbstractPayload {
    private long   columnLevel;
    private long   index;
    private long   obObjTypeIdx;
    private String columnName   = "";
    private String partKeyExtra = "";
    private long   obCollationTypeIdx;

    public OdpSinglePartKey() {}

    public OdpSinglePartKey(ObColumn column) {
        columnLevel = -1;
        index = column.getIndex();
        obObjTypeIdx = column.getObObjType().getValue();
        columnName = column.getColumnName();
        if (column instanceof ObGeneratedColumn) {
            String str = "";
            ObGeneratedColumn genCol = (ObGeneratedColumn) column;
            ObGeneratedColumnSubStrFunc subStrFunc = (ObGeneratedColumnSubStrFunc) genCol.getObGeneratedColumnSimpleFunc();
            str += "substr(" + subStrFunc.getRefColumnNames().get(0) + ","
                    + subStrFunc.getPos() + "," + subStrFunc.getPos() + ")";
            partKeyExtra = str;
        }
        obCollationTypeIdx = column.getObCollationType().getValue();
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        int len = Serialization.getNeedBytes(columnLevel);
        System.arraycopy(Serialization.encodeVi64(columnLevel), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(index);
        System.arraycopy(Serialization.encodeVi64(index), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(obObjTypeIdx);
        System.arraycopy(Serialization.encodeVi64(obObjTypeIdx), 0, bytes, idx, len);
        idx += len;

        byte[] strbytes = Serialization.encodeVString(columnName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        strbytes = Serialization.encodeVString(partKeyExtra);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        len = Serialization.getNeedBytes(obCollationTypeIdx);
        System.arraycopy(Serialization.encodeVi64(obObjTypeIdx), 0, bytes, idx, len);

        return bytes;
    }

    @Override
    public OdpSinglePartKey decode(ByteBuf buf) {
        super.decode(buf);

        columnLevel = Serialization.decodeVi64(buf);
        index = Serialization.decodeVi64(buf);
        obObjTypeIdx = Serialization.decodeVi64(buf);
        columnName = Serialization.decodeVString(buf);
        partKeyExtra = Serialization.decodeVString(buf);
        obCollationTypeIdx = Serialization.decodeVi64(buf);

        return this;
    }

    public long getColumnLevel() {
        return columnLevel;
    }

    public long getIndex() {
        return index;
    }

    public long getObObjTypeIdx() {
        return obObjTypeIdx;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getPartKeyExtra() {
        return partKeyExtra;
    }

    public long getObCollationTypeIdx() {
        return obCollationTypeIdx;
    }

    @Override
    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(columnLevel)
                + Serialization.getNeedBytes(index)
                + Serialization.getNeedBytes(obObjTypeIdx)
                + Serialization.getNeedBytes(columnName)
                + Serialization.getNeedBytes(partKeyExtra)
                + Serialization.getNeedBytes(obCollationTypeIdx);
    }

}

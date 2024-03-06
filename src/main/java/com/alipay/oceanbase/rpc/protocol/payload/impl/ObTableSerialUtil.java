/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2024 OceanBase
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

package com.alipay.oceanbase.rpc.protocol.payload.impl;

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObBorderFlag;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableObjType.*;

public class ObTableSerialUtil {
    static public int getEncodedSize(ObObj obj) {
        return getTableObjType(obj).getEncodedSize(obj);
    }

    static public byte[] encode(ObObj obj) {
        return getTableObjType(obj).encode(obj);
    }

    static public void decode(ByteBuf buf, ObObj obj) {
        ObTableObjType tableObjType = decodeTableObjType(buf);
        tableObjType.decode(buf, obj);
    }

    public static ObTableObjType decodeTableObjType(ByteBuf buf) {
        if (buf == null) {
            throw new IllegalArgumentException("cannot get ObTableObjType, buf is null");
        }
        byte type = Serialization.decodeI8(buf);
        ObTableObjType objType = ObTableObjType.valueOf(type);
        if (objType == null) {
            throw new IllegalArgumentException("cannot get table object type from value");
        }
        return objType;
    }

    static public int getEncodedSize(ObNewRange range) {
        int encodedSize = 0;
        encodedSize += Serialization.getNeedBytes(range.getTableId());
        encodedSize += 1; // borderFlag
        ObRowKey startKey = range.getStartKey();
        long rowkeySize = startKey.getObjCount();
        encodedSize += Serialization.getNeedBytes(rowkeySize);
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = startKey.getObj(i);
            encodedSize += ObTableSerialUtil.getEncodedSize(obObj);
        }

        ObRowKey endKey = range.getEndKey();
        rowkeySize = endKey.getObjCount();
        encodedSize += Serialization.getNeedBytes(rowkeySize);
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = endKey.getObj(i);
            encodedSize += ObTableSerialUtil.getEncodedSize(obObj);
        }

        if (ObGlobal.obVsnMajor() >= 4) {
            encodedSize += Serialization.getNeedBytes(range.getFlag());
        }

        return encodedSize;
    }

    static public byte[] encode(ObNewRange range) {
        byte[] bytes = new byte[getEncodedSize(range)];
        int idx = 0;

        long tableId = range.getTableId();
        int len = Serialization.getNeedBytes(tableId);
        System.arraycopy(Serialization.encodeVi64(tableId), 0, bytes, idx, len);
        idx += len;
        System
            .arraycopy(Serialization.encodeI8(range.getBorderFlag().getValue()), 0, bytes, idx, 1);
        idx += 1;

        ObRowKey startKey = range.getStartKey();
        long rowkeySize = startKey.getObjCount();
        len = Serialization.getNeedBytes(rowkeySize);
        System.arraycopy(Serialization.encodeVi64(rowkeySize), 0, bytes, idx, len);
        idx += len;
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = startKey.getObj(i);
            byte[] objBytes = ObTableSerialUtil.encode(obObj);
            System.arraycopy(objBytes, 0, bytes, idx, objBytes.length);
            idx += objBytes.length;
        }

        ObRowKey endKey = range.getEndKey();
        rowkeySize = endKey.getObjCount();
        len = Serialization.getNeedBytes(rowkeySize);
        System.arraycopy(Serialization.encodeVi64(rowkeySize), 0, bytes, idx, len);
        idx += len;
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = endKey.getObj(i);
            byte[] objBytes = ObTableSerialUtil.encode(obObj);
            System.arraycopy(objBytes, 0, bytes, idx, objBytes.length);
            idx += objBytes.length;
        }

        if (ObGlobal.obVsnMajor() >= 4) {
            long flag = range.getFlag();
            len = Serialization.getNeedBytes(flag);
            System.arraycopy(Serialization.encodeVi64(flag), 0, bytes, idx, len);
            idx += len;
        }

        return bytes;
    }

    static public void decode(ByteBuf buf, ObNewRange range) {
        range.setTableId(Serialization.decodeVi64(buf));
        range.setBorderFlag(ObBorderFlag.valueOf(Serialization.decodeI8(buf.readByte())));

        long rowkeySize = Serialization.decodeVi64(buf);
        ObRowKey startKey = new ObRowKey();
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = new ObObj();
            decode(buf, obObj);
            startKey.addObj(obObj);
        }
        range.setStartKey(startKey);

        rowkeySize = Serialization.decodeVi64(buf);
        ObRowKey endKey = new ObRowKey();
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = new ObObj();
            decode(buf, obObj);
            endKey.addObj(obObj);
        }
        range.setEndKey(endKey);

        if (ObGlobal.obVsnMajor() >= 4) {
            range.setFlag(Serialization.decodeVi64(buf));
        }
    }
}

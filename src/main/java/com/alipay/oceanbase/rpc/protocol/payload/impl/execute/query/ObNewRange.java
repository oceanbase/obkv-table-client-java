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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query;

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/*
 * int ObNewRange::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
 * {
 * int ret = OB_SUCCESS;
 * if (NULL == buf || buf_len <= 0) {
 * ret = OB_INVALID_ARGUMENT;
 * COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(ret));
 * } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, static_cast<int64_t>(table_id_)))) {
 * COMMON_LOG(WARN, "serialize table_id failed.", KP(buf), K(buf_len), K(pos), K_(table_id), K(ret));
 * } else if (OB_FAIL(serialization::encode_i8(buf, buf_len, pos, border_flag_.get_data()))) {
 * COMMON_LOG(WARN, "serialize border_flag failed.", KP(buf), K(buf_len), K(pos), K_(border_flag), K(ret));
 * } else if (OB_FAIL(start_key_.serialize(buf, buf_len, pos))) {
 * COMMON_LOG(WARN, "serialize start_key failed.", KP(buf), K(buf_len), K(pos), K_(start_key), K(ret));
 * } else if (OB_FAIL(end_key_.serialize(buf, buf_len, pos))) {
 * COMMON_LOG(WARN, "serialize end_key failed.", KP(buf), K(buf_len), K(pos), K_(end_key), K(ret));
 * }
 * return ret;
 * }
 *
 */
public class ObNewRange implements ObSimplePayload {

    private long         tableId    = Constants.OB_INVALID_ID;
    private ObBorderFlag borderFlag = new ObBorderFlag();
    private ObRowKey     startKey;
    private ObRowKey     endKey;
    private long         flag       = 0L;

    /*
     * Ob new range.
     */
    public ObNewRange() {
        borderFlag.setInclusiveStart();
        borderFlag.setInclusiveEnd();
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[getEncodedSize()];
        int idx = 0;

        int len = Serialization.getNeedBytes(tableId);
        System.arraycopy(Serialization.encodeVi64(tableId), 0, bytes, idx, len);
        idx += len;
        System.arraycopy(Serialization.encodeI8(borderFlag.getValue()), 0, bytes, idx, 1);
        idx += 1;

        long rowkeySize = startKey.getObjCount();
        len = Serialization.getNeedBytes(rowkeySize);
        System.arraycopy(Serialization.encodeVi64(rowkeySize), 0, bytes, idx, len);
        idx += len;
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = startKey.getObj(i);
            byte[] objBytes = obObj.encode();
            System.arraycopy(objBytes, 0, bytes, idx, objBytes.length);
            idx += objBytes.length;
        }

        rowkeySize = endKey.getObjCount();
        len = Serialization.getNeedBytes(rowkeySize);
        System.arraycopy(Serialization.encodeVi64(rowkeySize), 0, bytes, idx, len);
        idx += len;
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = endKey.getObj(i);
            byte[] objBytes = obObj.encode();
            System.arraycopy(objBytes, 0, bytes, idx, objBytes.length);
            idx += objBytes.length;
        }

        if (ObGlobal.obVsnMajor() >= 4) {
            len = Serialization.getNeedBytes(flag);
            System.arraycopy(Serialization.encodeVi64(flag), 0, bytes, idx, len);
            idx += len;
        }

        return bytes;
    }

    /*
     * Encode.
     */
    @Override
    public void encode(ObByteBuf buf) {
        Serialization.encodeVi64(buf, tableId);
        Serialization.encodeI8(buf, borderFlag.getValue());
        long startKeyObjCount = startKey.getObjCount();
        Serialization.encodeVi64(buf, startKeyObjCount);
        for (int i = 0; i < startKeyObjCount; ++i) {
            startKey.getObj(i).encode(buf);
        }
        long endKeyObjCount = endKey.getObjCount();
        Serialization.encodeVi64(buf, endKeyObjCount);
        for (int i = 0; i < endKeyObjCount; ++i) {
            endKey.getObj(i).encode(buf);
        }
        if (ObGlobal.obVsnMajor() >= 4) {
            Serialization.encodeVi64(buf, flag);
        }
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        this.tableId = Serialization.decodeVi64(buf);
        this.borderFlag = ObBorderFlag.valueOf(Serialization.decodeI8(buf.readByte()));

        long rowkeySize = Serialization.decodeVi64(buf);
        this.startKey = new ObRowKey();
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = new ObObj();
            obObj.decode(buf);
            this.startKey.addObj(obObj);
        }

        rowkeySize = Serialization.decodeVi64(buf);
        this.endKey = new ObRowKey();
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = new ObObj();
            obObj.decode(buf);
            this.endKey.addObj(obObj);
        }

        if (ObGlobal.obVsnMajor() >= 4) {
            this.flag = Serialization.decodeVi64(buf);
        }

        return this;
    }

    /*
     * Get encoded size.
     */
    @Override
    public int getEncodedSize() {
        int encodedSize = 0;
        encodedSize += Serialization.getNeedBytes(tableId);
        encodedSize += 1;
        long rowkeySize = startKey.getObjCount();
        encodedSize += Serialization.getNeedBytes(rowkeySize);
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = startKey.getObj(i);
            encodedSize += obObj.getEncodedSize();
        }

        rowkeySize = endKey.getObjCount();
        encodedSize += Serialization.getNeedBytes(rowkeySize);
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = endKey.getObj(i);
            encodedSize += obObj.getEncodedSize();
        }

        if (ObGlobal.obVsnMajor() >= 4) {
            encodedSize += Serialization.getNeedBytes(flag);
        }

        return encodedSize;
    }

    /*
     * Get table id.
     */
    public long getTableId() {
        return tableId;
    }

    /*
     * Set table id.
     */
    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    /*
     * Get border flag.
     */
    public ObBorderFlag getBorderFlag() {
        return borderFlag;
    }

    /*
     * Set border flag.
     */
    public void setBorderFlag(ObBorderFlag borderFlag) {
        this.borderFlag = borderFlag;
    }

    /*
     * Get start key.
     */
    public ObRowKey getStartKey() {
        return startKey;
    }

    /*
     * Set start key.
     */
    public void setStartKey(ObRowKey startKey) {
        this.startKey = startKey;
    }

    /*
     * Get end key.
     */
    public ObRowKey getEndKey() {
        return endKey;
    }

    /*
     * Set end key.
     */
    public void setEndKey(ObRowKey endKey) {
        this.endKey = endKey;
    }

    /*
     * get whole range.
     */
    public static ObNewRange getWholeRange() {
        ObNewRange range = new ObNewRange();
        range.startKey = ObRowKey.getInstance(ObObj.getMin());
        range.endKey = ObRowKey.getInstance(ObObj.getMax());
        range.borderFlag.unsetInclusiveStart();
        range.borderFlag.unsetInclusiveEnd();
        return range;
    }

    public long getFlag() {
        return flag;
    }

    public void setFlag(long flag) {
        this.flag = flag;
    }

}

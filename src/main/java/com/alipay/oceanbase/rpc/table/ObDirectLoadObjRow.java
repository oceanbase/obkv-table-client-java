/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

package com.alipay.oceanbase.rpc.table;

import java.util.List;

import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;

public class ObDirectLoadObjRow implements ObSimplePayload {

    private long    SeqNo = 0;
    private ObObj[] cells = new ObObj[0];

    public ObDirectLoadObjRow() {
    }

    public ObDirectLoadObjRow(ObObj[] cells) {
        setCells(cells);
        setSeqNo(0);
    }

    public ObDirectLoadObjRow(List<ObObj> cells) {
        setCells(cells);
        setSeqNo(0);
    }

    public ObObj[] getCells() {
        return cells;
    }

    public void setCells(ObObj[] cells) {
        if (cells == null)
            throw new NullPointerException();
        this.cells = cells;
    }

    public void setCells(List<ObObj> cells) {
        if (cells == null)
            throw new NullPointerException();
        this.cells = cells.toArray(new ObObj[0]);
    }

    public void setSeqNo(long SeqNo) {
        this.SeqNo = SeqNo;
    }

    public long getSeqNo() {
        return SeqNo;
    }

    /**
     * Encode.
     */
    @Override
    public byte[] encode() {
        int needBytes = getEncodedSize();
        ObByteBuf buf = new ObByteBuf(needBytes);
        encode(buf);
        return buf.bytes;
    }

    /**
     * Encode.
     */
    @Override
    public void encode(ObByteBuf buf) {
        Serialization.encodeVi64(buf, SeqNo);
        Serialization.encodeVi32(buf, cells.length);
        for (int i = 0; i < cells.length; ++i) {
            cells[i].encode(buf);
        }
    }

    /**
     * Decode.
     */
    @Override
    public ObDirectLoadObjRow decode(ByteBuf buf) {
        SeqNo = Serialization.decodeVi64(buf);
        int count = Serialization.decodeVi32(buf);
        cells = new ObObj[count];
        for (int i = 0; i < count; ++i) {
            ObObj obj = new ObObj();
            obj.decode(buf);
            cells[i] = obj;
        }
        return this;
    }

    /**
     * Get encoded size.
     */
    @Override
    public int getEncodedSize() {
        int size = 0;
        size += Serialization.getNeedBytes(SeqNo);
        size += Serialization.getNeedBytes(cells.length);
        for (int i = 0; i < cells.length; ++i) {
            size += cells[i].getEncodedSize();
        }
        return size;
    }

}

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

import java.util.ArrayList;
import java.util.List;

import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

import io.netty.buffer.ByteBuf;

public class ObDirectLoadBucket implements ObSimplePayload {

    private ArrayList<ObDirectLoadObjRow> rowList = new ArrayList<ObDirectLoadObjRow>();

    public ObDirectLoadBucket() {
    }

    public boolean isEmpty() {
        return rowList.isEmpty();
    }

    public int getRowCount() {
        return rowList.size();
    }

    public void addRow(ObObj[] row) {
        rowList.add(new ObDirectLoadObjRow(row));
    }

    public void addRow(List<ObObj> row) {
        rowList.add(new ObDirectLoadObjRow(row));
    }

    public void addRow(ObDirectLoadObjRow row) {
        if (row == null) {
            throw new NullPointerException();
        }
        rowList.add(row);
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
        Serialization.encodeVi32(buf, rowList.size());
        for (int i = 0; i < rowList.size(); ++i) {
            rowList.get(i).encode(buf);
        }
    }

    /**
     * Decode.
     */
    @Override
    public ObDirectLoadBucket decode(ByteBuf buf) {
        int count = Serialization.decodeVi32(buf);
        for (int i = 0; i < count; ++i) {
            ObDirectLoadObjRow row = new ObDirectLoadObjRow();
            row.decode(buf);
            rowList.add(row);
        }
        return this;
    }

    /**
     * Get encoded size.
     */
    @Override
    public int getEncodedSize() {
        int size = 0;
        size += Serialization.getNeedBytes(rowList.size());
        for (int i = 0; i < rowList.size(); ++i) {
            size += rowList.get(i).getEncodedSize();
        }
        return size;
    }

}

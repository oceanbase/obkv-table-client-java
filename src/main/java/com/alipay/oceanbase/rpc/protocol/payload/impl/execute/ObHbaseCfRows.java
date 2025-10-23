/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

import java.util.ArrayList;
import java.util.List;

public class ObHbaseCfRows extends AbstractPayload {
    private String realTableName; // column family
    private List<Integer> keyIndex = new ArrayList<>(); // original keys index
    private List<Integer> cellNumArray = new ArrayList<>(); // the number of original cells to each key
    private List<ObHbaseCell> cells = new ArrayList<>(); // original cells

    public ObHbaseCfRows() {}

    public ObHbaseCfRows(List<Integer> keyIndex, List<Integer> cellNumArray, List<ObHbaseCell> cells) {
        this.keyIndex = keyIndex;
        this.cellNumArray = cellNumArray;
        this.cells = cells;
    }

    public String getRealTableName() {
        return realTableName;
    }

    public void setRealTableName(String realTableName) {
        this.realTableName = realTableName;
    }

    public void add(Integer index, Integer cellNum, List<ObHbaseCell> cells) {
        this.keyIndex.add(index);
        this.cellNumArray.add(cellNum);
        this.cells.addAll(cells);
    }

    public List<Integer> getKeyIndex() {
        return keyIndex;
    }

    public int getKeyIndex(int idx) {
        return keyIndex.get(idx);
    }

    public List<Integer> getCellNumArray() {
        return cellNumArray;
    }

    public int getCellNum(int idx) {
        return cellNumArray.get(idx);
    }

    public List<ObHbaseCell> getCells() {
        return cells;
    }

    public void encode(ObByteBuf buf) {
        // 0. encode header
        encodeHeader(buf);

        // 1. encode family
        Serialization.encodeVString(buf, realTableName);

        // 2. encode keyIndex
        Serialization.encodeVi64(buf, keyIndex.size());
        for (long idx : keyIndex) {
            Serialization.encodeVi64(buf, idx);
        }

        // 3. encode cellNumArray
        Serialization.encodeVi64(buf, cellNumArray.size());
        for (long cellNum : cellNumArray) {
            Serialization.encodeVi64(buf, cellNum);
        }

        // 4. encode cells without length
        for (ObHbaseCell cell : cells) {
            cell.encode(buf);
        }
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode family
        byte[] strbytes = Serialization.encodeVString(realTableName);
        System.arraycopy(strbytes, 0, bytes, idx, strbytes.length);
        idx += strbytes.length;

        // 2. encode keyIndex
        int len = Serialization.getNeedBytes(keyIndex.size());
        System.arraycopy(Serialization.encodeVi64(keyIndex.size()), 0, bytes, idx, len);
        idx += len;
        for (long index : keyIndex) {
            len = Serialization.getNeedBytes(index);
            System.arraycopy(Serialization.encodeVi64(index), 0, bytes, idx, len);
            idx += len;
        }

        // 3. encode cellNumArray
        len = Serialization.getNeedBytes(cellNumArray.size());
        System.arraycopy(Serialization.encodeVi64(cellNumArray.size()), 0, bytes, idx, len);
        idx += len;
        for (long cellNum : cellNumArray) {
            len = Serialization.getNeedBytes(cellNum);
            System.arraycopy(Serialization.encodeVi64(cellNum), 0, bytes, idx, len);
            idx += len;
        }

        // 4. encode cells without length
        for (ObHbaseCell cell : cells) {
            len = (int) cell.getPayloadSize();
            System.arraycopy(cell.encode(), 0, bytes, idx, len);
            idx += len;
        }

        return bytes;
    }

    @Override
    public long getPayloadContentSize() {
        if (this.payLoadContentSize == INVALID_PAYLOAD_CONTENT_SIZE) {
            long payloadContentSize = 0;
            // add family len
            payloadContentSize += Serialization.getNeedBytes(realTableName);

            // add key index array size and index
            payloadContentSize += Serialization.getNeedBytes(keyIndex.size());
            for (Integer index : keyIndex) {
                payloadContentSize += Serialization.getNeedBytes(index);
            }

            // add cell num array size and cell num
            payloadContentSize += Serialization.getNeedBytes(cellNumArray.size());
            for (Integer cellNum : cellNumArray) {
                payloadContentSize += Serialization.getNeedBytes(cellNum);
            }

            // only add cells size
            for (ObHbaseCell cell : cells) {
                payloadContentSize += cell.getPayloadSize();
            }
            this.payLoadContentSize = payloadContentSize;
        }
        return this.payLoadContentSize;
    }

    @Override
    public void resetPayloadContentSize() {
        super.resetPayloadContentSize();
        for (ObHbaseCell cell : cells) {
            if (cell != null) {
                cell.resetPayloadContentSize();
            }
        }
    }

}

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

package com.alipay.oceanbase.rpc.direct_load;

import java.util.ArrayList;
import java.util.List;

import com.alipay.oceanbase.rpc.direct_load.exception.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;

public class ObDirectLoadBucket {

    /**
     * buffer的格式如下
     * +----------------+-----------+------+------+-----+
     * | payload length | row count | row1 | row2 | ... |
     * +----------------+-----------+------+------+-----+
     */

    private static final ObDirectLoadLogger logger              = ObDirectLoadLogger.getLogger();
    private final static int                integerReservedSize = 5;                             // 预留5个字节用来编码Integer
    private final static int                reservedSize        = integerReservedSize * 2;       // 预留2个Integer
    private final static int                defaultBufferSize   = 1 * 1024 * 1024;               // 1M

    private final int                       bufferSize;
    private ArrayList<ObByteBuf>            payloadBufferList   = new ArrayList<ObByteBuf>(64);
    private int                             totalRowCount       = 0;

    private ObByteBuf                       buffer              = null;
    private int                             currentRowCount     = 0;
    private Row                             row                 = new Row();

    public ObDirectLoadBucket() {
        bufferSize = defaultBufferSize;
    }

    public ObDirectLoadBucket(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public boolean isEmpty() {
        return (getRowNum() == 0);
    }

    public int getRowNum() {
        return totalRowCount + currentRowCount;
    }

    @Override
    public String toString() {
        return String.format("{rowNum:%d}", getRowNum());
    }

    public void addRow(ObObj[] cells) throws ObDirectLoadException {
        if (cells == null || cells.length == 0) {
            logger.warn("cells cannot be null or empty, cells:" + cells);
            throw new ObDirectLoadIllegalArgumentException("cells cannot be null or empty, cells:"
                                                           + cells);
        }
        row.setCells(cells);
        appendRow(row);
    }

    public void addRow(List<ObObj> cells) throws ObDirectLoadException {
        if (cells == null || cells.isEmpty()) {
            logger.warn("cells cannot be null or empty, cells:" + cells);
            throw new ObDirectLoadIllegalArgumentException("cells cannot be null or empty, cells:"
                                                           + cells);
        }
        row.setCells(cells);
        appendRow(row);
    }

    private void appendRow(Row row) {
        final int rowEncodedSize = row.getEncodedSize();
        while (true) {
            if (buffer == null) {
                allocBuffer(rowEncodedSize);
            } else if (buffer.writableBytes() < rowEncodedSize) {
                sealBuffer();
            } else {
                row.encode(buffer);
                ++currentRowCount;
                break;
            }
        }
    }

    private void allocBuffer(int encodedSize) {
        final int needSize = encodedSize + reservedSize;
        final int allocBufferSize = (needSize + bufferSize - 1) / bufferSize * bufferSize;
        buffer = new ObByteBuf(allocBufferSize);
        buffer.reserve(reservedSize);
    }

    private void sealBuffer() {
        // 编码row count
        encodeVi32(buffer.bytes, integerReservedSize, currentRowCount);
        // 编码payload length
        encodeVi32(buffer.bytes, 0, buffer.readableBytes() - integerReservedSize);
        payloadBufferList.add(buffer);
        totalRowCount += currentRowCount;
        currentRowCount = 0;
        buffer = null;
    }

    private void encodeVi32(byte[] buf, int pos, int value) {
        // 前面的byte的高位都设置为1
        for (int i = 0; i < integerReservedSize - 1; ++i, ++pos) {
            buf[pos] = (byte) (value | 0x80);
            value >>>= 7;
        }
        // 最后一个byte的高位设置为0
        buf[pos] = (byte) (value & 0x7f);
    }

    public List<ObByteBuf> getPayloadBufferList() {
        if (buffer != null) {
            sealBuffer();
        }
        return payloadBufferList;
    }

    private static class Row {

        private final long SeqNo = 0;
        private ObObj[]    cells = null;

        public Row() {
        }

        public void setCells(ObObj[] cells) {
            this.cells = cells;
        }

        public void setCells(List<ObObj> cells) {
            this.cells = cells.toArray(new ObObj[0]);
        }

        /**
         * Encode.
         */
        public void encode(ObByteBuf buf) {
            Serialization.encodeVi64(buf, SeqNo);
            Serialization.encodeVi32(buf, cells.length);
            for (int i = 0; i < cells.length; ++i) {
                cells[i].encode(buf);
            }
        }

        /**
         * Get encoded size.
         */
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

}

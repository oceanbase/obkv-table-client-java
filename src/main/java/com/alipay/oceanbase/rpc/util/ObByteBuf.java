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

package com.alipay.oceanbase.rpc.util;

public class ObByteBuf {

    public byte[] bytes;
    public int    pos;

    public ObByteBuf(int capacity) {
        if (capacity < 0) {
            throw new IllegalArgumentException(
                String.format("invalid args, capacity(%d)", capacity));
        }
        this.bytes = new byte[capacity];
        this.pos = 0;
    }

    public ObByteBuf(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException(String.format("invalid args, bytes(null)"));
        }
        this.bytes = bytes;
        this.pos = 0;
    }

    /**
     * Get capacity
     * @return capacity
     */
    public int capacity() {
        return bytes.length;
    }

    /**
     * Write byte
     * @param b byte
     */
    public void writeByte(byte b) {
        if (pos + 1 > bytes.length) {
            throw new IllegalArgumentException(String.format(
                "size overflow, capacity(%d), pos(%d), data_size(1)", bytes.length, pos));
        }
        bytes[pos++] = b;
    }

    /**
     * Write bytes
     * @param src bytes
     */
    public void writeBytes(byte[] src) {
        if (pos + src.length > bytes.length) {
            throw new IllegalArgumentException(String.format(
                "size overflow, capacity(%d), pos(%d), data_size(%d)", bytes.length, pos,
                src.length));
        }
        System.arraycopy(src, 0, bytes, pos, src.length);
        pos += src.length;
    }

    /**
     * Write bytes
     * @param src data
     * @param srcPos start pos
     * @param srcLength len
     */
    public void writeBytes(byte[] src, int srcPos, int srcLength) {
        if (this.pos + srcLength > bytes.length) {
            throw new IllegalArgumentException(
                String.format("size overflow, capacity(%d), pos(%d), data_size(%d)", bytes.length,
                    pos, srcLength));
        }
        System.arraycopy(src, srcPos, bytes, pos, srcLength);
        pos += srcLength;
    }

}
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

package com.alipay.oceanbase.rpc.protocol.payload.impl;

import com.alipay.oceanbase.rpc.protocol.payload.ObSimplePayload;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.ObVString;
import io.netty.buffer.ByteBuf;

public class ObObj implements ObSimplePayload {

    private static ObObj MAX_OBJECT;
    private static ObObj MIN_OBJECT;
    private static long  MAX_OBJECT_VALUE = -2L;
    private static long  MIN_OBJECT_VALUE = -3L;

    static {
        MAX_OBJECT = new ObObj(ObObjType.ObExtendType.getDefaultObjMeta(), MAX_OBJECT_VALUE);
        MIN_OBJECT = new ObObj(ObObjType.ObExtendType.getDefaultObjMeta(), MIN_OBJECT_VALUE);
    }

    /*
     * Ob obj.
     */
    public ObObj() {
        this.meta = new ObObjMeta();
        this.value = null;
    }

    /*
     * Ob obj.
     */
    public ObObj(ObObjMeta meta, Object value) {
        this.meta = meta;
        setValue(value);
    }

    // 1. 序列化 meta
    private ObObjMeta meta;
    // 2. 序列化 valueLength 还是 nmb_desc_？
    private int       valueLength;
    /*
     *3. 序列化 value。
     *  obj_val_serialize<OBJTYPE>,
     *  obj_val_deserialize<OBJTYPE>,
     *  obj_val_get_serialize_size<OBJTYPE>
     */
    private Object    value;

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[getEncodedSize()];
        int idx = 0;

        System.arraycopy(meta.encode(), 0, bytes, idx, 4);
        idx += 4;

        byte[] valueBytes = meta.getType().encode(value);
        System.arraycopy(valueBytes, 0, bytes, idx, valueBytes.length);
        idx += valueBytes.length;

        return bytes;
    }

    /*
     * Encode.
     */
    @Override
    public void encode(ObByteBuf buf) {
        meta.encode(buf);
        buf.writeBytes(meta.getType().encode(value));
    }

    /*
     * Decode.
     */
    @Override
    public ObObj decode(ByteBuf buf) {
        meta.decode(buf);
        this.value = meta.getType().decode(buf, meta.getCsType());

        return this;
    }

    /*
     * Get encoded size.
     */
    @Override
    public int getEncodedSize() {
        return meta.getEncodedSize() + meta.getType().getEncodedSize(value);
    }

    /*
     * Get meta.
     */
    public ObObjMeta getMeta() {
        return meta;
    }

    /*
     * Set meta.
     */
    public void setMeta(ObObjMeta meta) {
        this.meta = meta;
    }

    /*
     * Get value length.
     */
    public int getValueLength() {
        return valueLength;
    }

    /*
     * Set value length.
     */
    public void setValueLength(int valueLength) {
        this.valueLength = valueLength;
    }

    /*
     * Get value.
     */
    public Object getValue() {
        return value;
    }

    /*
     * Set value.
     */
    public void setValue(Object value) {
        if (value instanceof String) {
            this.value = new ObVString((String) value);
        } else {
            this.value = value;
        }
    }

    /*
     * Get instance.
     */
    public static ObObj getInstance(Object value) {
        ObObjMeta meta = ObObjType.defaultObjMeta(value);
        if (value instanceof ObObj) {
            return new ObObj(meta, ((ObObj) value).getValue());
        } else {
            return new ObObj(meta, value);
        }
    }

    /*
     * Get max.
     */
    public static ObObj getMax() {
        return MAX_OBJECT;
    }

    /*
     * Get min.
     */
    public static ObObj getMin() {
        return MIN_OBJECT;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "ObObj{" + "meta=" + meta + ", valueLength=" + valueLength + ", value=" + value
               + '}';
    }

    public boolean isMinObj() {
        return (getMeta().getType() == ObObjType.ObExtendType)
               && ((long) getValue() == MIN_OBJECT_VALUE);
    }

    public boolean isMaxObj() {
        return (getMeta().getType() == ObObjType.ObExtendType)
               && ((long) getValue() == MAX_OBJECT_VALUE);
    }

    // set value directly, do not wrapped by ObVString when type is varchar
    public void setValueFromTableObj(Object value) {
        this.value = value;
    }
}

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
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

/**
 * ob_object.cpp
 *
DEFINE_SERIALIZE(ObObjMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(type_);
  OB_UNIS_ENCODE(cs_level_);
  OB_UNIS_ENCODE(cs_type_);
  OB_UNIS_ENCODE(scale_);
  return ret;
}

DEFINE_DESERIALIZE(ObObjMeta)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(type_);
  OB_UNIS_DECODE(cs_level_);
  OB_UNIS_DECODE(cs_type_);
  OB_UNIS_DECODE(scale_);
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObObjMeta)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(type_);
  OB_UNIS_ADD_LEN(cs_level_);
  OB_UNIS_ADD_LEN(cs_type_);
  OB_UNIS_ADD_LEN(scale_);
  return len;
}

 */
public class ObObjMeta implements ObSimplePayload {

    private ObObjType        type;
    private ObCollationLevel csLevel; // collation level QUESTION: ObCollationLevel 定义
    private ObCollationType  csType; // collation type QUESTION: ObCollationType 定义
    private byte             scale;  // scale, 当type_ 为ObBitType时，该字段存储bit的length

    /*
     * Ob obj meta.
     */
    public ObObjMeta() {
    }

    /*
     * Ob obj meta.
     */
    public ObObjMeta(ObObjType type, ObCollationLevel csLevel, ObCollationType csType, byte scale) {
        this.type = type;
        this.csLevel = csLevel;
        this.csType = csType;
        this.scale = scale;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[getEncodedSize()];
        int idx = 0;

        System.arraycopy(Serialization.encodeI8(type.getByteValue()), 0, bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeI8(csLevel.getByteValue()), 0, bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeI8(csType.getByteValue()), 0, bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeI8(scale), 0, bytes, idx, 1);
        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        this.type = ObObjType.valueOf(Serialization.decodeI8(buf.readByte()));
        this.csLevel = ObCollationLevel.valueOf(Serialization.decodeI8(buf.readByte()));
        this.csType = ObCollationType.valueOf(Serialization.decodeI8(buf.readByte()));
        this.scale = Serialization.decodeI8(buf.readByte());

        return this;
    }

    /*
     * Get encoded size.
     */
    @Override
    public int getEncodedSize() {
        return 4;
    }

    /*
     * Get type.
     */
    public ObObjType getType() {
        return type;
    }

    /*
     * Set type.
     */
    public void setType(ObObjType type) {
        this.type = type;
    }

    /*
     * Get cs level.
     */
    public ObCollationLevel getCsLevel() {
        return csLevel;
    }

    /*
     * Set cs level.
     */
    public void setCsLevel(ObCollationLevel csLevel) {
        this.csLevel = csLevel;
    }

    /*
     * Get cs type.
     */
    public ObCollationType getCsType() {
        return csType;
    }

    /*
     * Set cs type.
     */
    public void setCsType(ObCollationType csType) {
        this.csType = csType;
    }

    /*
     * Get scale.
     */
    public byte getScale() {
        return scale;
    }

    /*
     * Set scale.
     */
    public void setScale(byte scale) {
        this.scale = scale;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "ObObjMeta{" + "type=" + type + ", csLevel=" + csLevel + ", csType=" + csType
               + ", scale=" + scale + '}';
    }
}

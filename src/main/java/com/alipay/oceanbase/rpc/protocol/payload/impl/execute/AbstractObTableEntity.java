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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.Map;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/*
 *
 <code>
OB_DEF_SERIALIZE(ObITableEntity)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    const int64_t rowkey_size = get_rowkey_size();
    OB_UNIS_ENCODE(rowkey_size);
    ObObj obj;
    for (int64_t i = 0; i < rowkey_size && OB_SUCCESS == ret; ++i) {
      if (OB_FAIL(this->get_rowkey_value(i, obj))) {
        LOG_WARN("failed to get value", K(ret), K(i));
      }
      OB_UNIS_ENCODE(obj);
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<std::pair<ObString, ObObj>, 8> properties;
    if (OB_FAIL(this->get_properties(properties))) {  // @todo optimize, use iterator
      LOG_WARN("failed to get properties", K(ret));
    } else {
      const int64_t properties_count = properties.count();
      OB_UNIS_ENCODE(properties_count);
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        const std::pair<ObString, ObObj> &kv_pair = properties.at(i);
        OB_UNIS_ENCODE(kv_pair.first);
        OB_UNIS_ENCODE(kv_pair.second);
      }
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObITableEntity)
{
  int ret = OB_SUCCESS;
  reset();
  ObString key;
  ObObj value;
  if (OB_SUCC(ret)) {
    int64_t rowkey_size = -1;
    OB_UNIS_DECODE(rowkey_size);
    for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_size; ++i) {
      OB_UNIS_DECODE(value);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(this->add_rowkey_value(value))) {
          LOG_WARN("failed to add rowkey value", K(ret), K(value));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t properties_count = -1;
    OB_UNIS_DECODE(properties_count);
    for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
      OB_UNIS_DECODE(key);
      OB_UNIS_DECODE(value);
      if (OB_SUCC(ret)) {
        if (OB_FAIL(this->set_property(key, value))) {
          LOG_WARN("failed to set property", K(ret), K(key), K(value));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObITableEntity)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  ObString key;
  ObObj value;
  const int64_t rowkey_size = get_rowkey_size();
  OB_UNIS_ADD_LEN(rowkey_size);
  for (int64_t i = 0; i < rowkey_size && OB_SUCCESS == ret; ++i) {
    if (OB_FAIL(this->get_rowkey_value(i, value))) {
      LOG_WARN("failed to get value", K(ret), K(i));
    }
    OB_UNIS_ADD_LEN(value);
  }
  if (OB_SUCC(ret)) {
    ObSEArray<std::pair<ObString, ObObj>, 8> properties;
    if (OB_FAIL(this->get_properties(properties))) {  // @todo optimize, use iterator
      LOG_WARN("failed to get properties", K(ret));
    } else {
      const int64_t properties_count = properties.count();
      OB_UNIS_ADD_LEN(properties_count);
      for (int64_t i = 0; i < properties_count && OB_SUCCESS == ret; ++i) {
        const std::pair<ObString, ObObj> &kv_pair = properties.at(i);
        OB_UNIS_ADD_LEN(kv_pair.first);
        OB_UNIS_ADD_LEN(kv_pair.second);
      }
    }
  }
  return len;
}
 </code>
 */
public abstract class AbstractObTableEntity extends AbstractPayload implements ObITableEntity {

    private long version = 1;

    /**
     * Get version.
     */
    @Override
    public long getVersion() {
        return version;
    }

    /**
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. header: ver + plen
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        long rowkeySize = this.getRowKeySize();
        int len = Serialization.getNeedBytes(rowkeySize);
        System.arraycopy(Serialization.encodeVi64(rowkeySize), 0, bytes, idx, len);
        idx += len;
        for (int i = 0; i < rowkeySize; i++) {
            ObObj obObj = this.getRowKeyValue(i);
            byte[] objBytes = obObj.encode();
            System.arraycopy(objBytes, 0, bytes, idx, objBytes.length);
            idx += objBytes.length;
        }
        long propertiesCount = this.getPropertiesCount();
        len = Serialization.getNeedBytes(propertiesCount);
        System.arraycopy(Serialization.encodeVi64(propertiesCount), 0, bytes, idx, len);
        idx += len;
        Map<String, ObObj> properties = this.getProperties();
        for (Map.Entry<String, ObObj> entry : properties.entrySet()) {
            String name = entry.getKey();
            ObObj property = entry.getValue();

            int keyLen = Serialization.getNeedBytes(name);
            System.arraycopy(Serialization.encodeVString(name), 0, bytes, idx, keyLen);
            idx += keyLen;
            byte[] objBytes = property.encode();
            System.arraycopy(objBytes, 0, bytes, idx, objBytes.length);
            idx += objBytes.length;
        }

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {

        // 0. decode header
        super.decode(buf);

        // 1. decode Operation
        long rowkeySize = Serialization.decodeVi64(buf);
        for (long i = 0; i < rowkeySize; i++) {
            ObObj obj = new ObObj();
            this.addRowKeyValue(obj.decode(buf));
        }
        long propertiesCount = Serialization.decodeVi64(buf);
        for (int i = 0; i < propertiesCount; i++) {
            String name = Serialization.decodeVString(buf);
            ObObj property = new ObObj();
            this.setProperty(name, property.decode(buf));
        }

        return this;
    }

    /*
     * Get payload size.
     */
    @Override
    public long getPayloadSize() {
        long contentSize = getPayloadContentSize();
        return Serialization.getObUniVersionHeaderLength(getVersion(), contentSize) + contentSize;

    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        int size = Serialization.getNeedBytes(this.getRowKeySize())
                   + Serialization.getNeedBytes(this.getPropertiesCount());
        for (long i = 0; i < this.getRowKeySize(); i++) {
            size += this.getRowKeyValue(i).getEncodedSize();
        }
        for (Map.Entry<String, ObObj> entry : this.getProperties().entrySet()) {
            size += Serialization.getNeedBytes(entry.getKey());
            size += entry.getValue().getEncodedSize();
        }

        return size;
    }

}

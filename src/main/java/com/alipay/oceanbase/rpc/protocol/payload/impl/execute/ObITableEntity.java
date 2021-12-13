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

import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;

import java.util.Map;

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
public interface ObITableEntity extends ObPayload {

    /**
     * RowKey is Primary key, consist of multiple column, one column is RowKeyValue
     *
     * Set one RowKeyValue with index
     *
     * @param idx RowKeyValue index
     * @param rowKeyValue value
     */
    void setRowKeyValue(long idx, ObObj rowKeyValue);

    /**
     * Add one RowKeyValue
     *
     * @param rowKeyValue value
     */
    void addRowKeyValue(ObObj rowKeyValue);

    /**
     * Get one RowKeyValue with index
     *
     * @param idx RowKeyValue index
     * @return RowKeyvalu
     */
    ObObj getRowKeyValue(long idx);

    /**
     * @return RowKey value size
     */
    long getRowKeySize();

    /*
     * Set rowkey size
     */
    void setRowKeySize(long rowKeySize);

    /**
     * @return RowKey obj
     */
    ObRowKey getRowKey();

    /**
     * Property is the columns except RowKey
     *
     * Set one property
     *
     * @param propName column name
     * @param propValue column value
     */
    void setProperty(String propName, ObObj propValue);

    /**
     * Get one property value
     *
     * @param propName column name
     * @return column value
     */
    ObObj getProperty(String propName);

    /**
     * @return All property values
     */
    Map<String, ObObj> getProperties();

    /**
     * @return All property simple property without ob meta info
     */
    Map<String, Object> getSimpleProperties();

    /**
     * @return Properties count
     */
    long getPropertiesCount();

}

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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;

import java.util.HashMap;
import java.util.Map;

public class ObTableEntity extends AbstractObTableEntity {

    private ObRowKey           rowKey     = new ObRowKey();
    private Map<String, ObObj> properties = new HashMap<String, ObObj>();

    /**
     * Set row key value.
     */
    @Override
    public void setRowKeyValue(long idx, ObObj rowKeyValue) {
        rowKey.setObj((int) idx, rowKeyValue);
    }

    /**
     * Add row key value.
     */
    @Override
    public void addRowKeyValue(ObObj rowKeyValue) {
        rowKey.addObj(rowKeyValue);
    }

    /**
     * Get row key value.
     */
    @Override
    public ObObj getRowKeyValue(long idx) {
        return rowKey.getObj((int) idx);
    }

    /**
     * Get row key size.
     */
    @Override
    public long getRowKeySize() {
        return rowKey.getObjCount();
    }

    /**
     * Set row key size.
     */
    @Override
    public void setRowKeySize(long rowKeySize) {
        // ignore
    }

    /**
     * Get row key.
     */
    @Override
    public ObRowKey getRowKey() {
        return rowKey;
    }

    /**
     * Set property.
     */
    @Override
    public void setProperty(String propName, ObObj propValue) {
        this.properties.put(propName, propValue);
    }

    /**
     * Get property.
     */
    @Override
    public ObObj getProperty(String propName) {
        return this.properties.get(propName);
    }

    @Override
    public Map<String, ObObj> getProperties() {
        return this.properties;
    }

    public Map<String, Object> getSimpleProperties() {
        Map<String, Object> values = new HashMap<String, Object>((int) getPropertiesCount());
        for (Map.Entry<String, ObObj> entry : getProperties().entrySet()) {
            values.put(entry.getKey(), entry.getValue().getValue());
        }
        return values;
    }

    /**
     * Get properties count.
     */
    @Override
    public long getPropertiesCount() {
        return this.properties.size();
    }

}

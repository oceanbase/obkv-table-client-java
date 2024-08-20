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

import com.google.common.base.Objects;

import java.util.ArrayList;
import java.util.List;

/**
 * ob_rowkey.h
 * int ObRowkey::serialize
 *
 */
public class ObRowKey {

    private List<ObObj> objs = new ArrayList<ObObj>();

    /*
     * Get obj count.
     */
    public long getObjCount() {
        return objs.size();
    }

    /*
     * Get objs.
     */
    public List<ObObj> getObjs() {
        return objs;
    }

    public void setObjs(List<ObObj> objs) {
        this.objs = objs;
    }

    /*
     * Get obj.
     */
    public ObObj getObj(int idx) {
        return objs.get(idx);
    }

    /*
     * Set obj.
     */
    public void setObj(int idx, ObObj obObj) {
        objs.set(idx, obObj);
    }

    /*
     * Add obj.
     */
    public void addObj(ObObj obObj) {
        objs.add(obObj);
    }

    /*
     * Get instance.
     */
    public static ObRowKey getInstance(ObObj... values) {
        ObRowKey rowKey = new ObRowKey();
        for (ObObj value : values) {
            rowKey.addObj(value);
        }

        return rowKey;
    }

    /*
     * Get instance.
     */
    public static ObRowKey getInstance(Object... values) {
        ObRowKey rowKey = new ObRowKey();
        for (Object value : values) {
            if (value instanceof ObObj) {
                rowKey.addObj((ObObj) value);
            } else {
                rowKey.addObj(ObObj.getInstance(value));
            }
        }

        return rowKey;
    }

    /*
     * Get instance.
     */
    public static ObRowKey getInstance(List<Object> values) {
        ObRowKey rowKey = new ObRowKey();
        for (Object value : values) {
            if (value instanceof ObObj) {
                rowKey.addObj((ObObj) value);
            } else {
                rowKey.addObj(ObObj.getInstance(value));
            }
        }

        return rowKey;
    }

    /*
     * Equals.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ObRowKey obRowKey = (ObRowKey) o;
        return Objects.equal(objs, obRowKey.objs);
    }

    /*
     * Hash code.
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(objs);
    }

}

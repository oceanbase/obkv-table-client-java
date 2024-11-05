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

package com.alipay.oceanbase.rpc.property;

import java.util.Properties;

import static com.alipay.oceanbase.rpc.constant.Constants.OB_TABLE_CLIENT_PREFIX;

public abstract class AbstractPropertyAware {
    protected Properties properties = new Properties();

    /*
     * Parse to int.
     */
    public int parseToInt(String key) throws NumberFormatException {
        return Integer.parseInt(System.getProperty(OB_TABLE_CLIENT_PREFIX + key, getProperty(key)));
    }

    /*
     * Parse to int.
     */
    public int parseToInt(String key, int defaultV) {
        try {
            return parseToInt(key);
        } catch (NumberFormatException e) {
            return defaultV;
        }
    }

    /*
     * Parse to long.
     */
    public long parseToLong(String key) throws NumberFormatException {
        return Long.parseLong(System.getProperty(OB_TABLE_CLIENT_PREFIX + key, getProperty(key)));
    }

    /*
     * Parse to long.
     */
    public long parseToLong(String key, long defaultV) {
        try {
            return parseToLong(key);
        } catch (NumberFormatException e) {
            return defaultV;
        }
    }

    public boolean parseToBoolean(String key) throws Exception {
        if (System.getProperty(OB_TABLE_CLIENT_PREFIX + key) == null && getProperty(key) == null) {
            throw new Exception();
        }
        return Boolean.parseBoolean(System.getProperty(OB_TABLE_CLIENT_PREFIX + key,
            getProperty(key)));
    }

    public boolean parseToBoolean(String key, boolean defaultV) {
        try {
            return parseToBoolean(key);
        } catch (Exception e) {
            return defaultV;
        }
    }

    /*
     * Get property.
     */
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    /*
     * Add property.
     */
    public void addProperty(String key, String value) {
        this.properties.put(key, value);
    }

    /*
     * Set properties.
     */
    public void setProperties(Properties properties) {
        this.properties = properties;
    }

    /*
     * Get properties.
     */
    public Properties getProperties() {
        return properties;
    }
}

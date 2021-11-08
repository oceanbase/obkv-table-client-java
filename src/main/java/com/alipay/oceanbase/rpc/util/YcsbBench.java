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

package com.alipay.oceanbase.rpc.util;

import com.alipay.oceanbase.rpc.table.ObTable;
import com.yahoo.ycsb.*;

import java.util.*;

public class YcsbBench extends DB {

    public static final String HOST_PROPERTY     = "ob.table.host";
    public static final String PORT_PROPERTY     = "ob.table.port";
    public static final String TENANT_PROPERTY   = "ob.table.tenant";
    public static final String USERNAME_PROPERTY = "ob.table.username";
    public static final String PASSWORD_PROPERTY = "ob.table.password";
    public static final String DB_PROPERTY       = "ob.table.database";

    ObTable                    client;
    String[]                   fields            = new String[] { "FIELD0", "FIELD1", "FIELD2",
            "FIELD3", "FIELD4", "FIELD5", "FIELD6", "FIELD7", "FIELD8", "FIELD9" };

    /**
     * Init.
     */
    public void init() throws DBException {
        Properties props = getProperties();
        int port = 0;

        String portString = props.getProperty(PORT_PROPERTY);
        if (portString != null) {
            port = Integer.parseInt(portString);
        }
        String host = props.getProperty(HOST_PROPERTY);
        String tenant = props.getProperty(TENANT_PROPERTY);
        String db = props.getProperty(DB_PROPERTY);
        String user = props.getProperty(USERNAME_PROPERTY);
        String password = props.getProperty(PASSWORD_PROPERTY);

        try {
            client = new ObTable.Builder(host, port) //
                .setLoginInfo(tenant, user, password, db) //
                .build();
        } catch (Exception e) {
            throw new DBException(e);
        }

    }

    /**
     * Read.
     */
    @Override
    public Status read(String table, String key, Set<String> fields,
                       HashMap<String, ByteIterator> result) {
        try {
            String[] fs = this.fields;
            if (fields != null) {
                fs = fields.toArray(new String[] {});
            }
            Iterator i$ = client.get(table, key, fs).entrySet().iterator();

            while (i$.hasNext()) {
                Map.Entry<String, Object> entry = (Map.Entry) i$.next();
                result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
            }

            return result.isEmpty() ? Status.ERROR : Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
        }
        return null;
    }

    /**
     * Scan.
     */
    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
        return null;
    }

    /**
     * Update.
     */
    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        Map<String, String> vs = StringByteIterator.getStringMap(values);
        String[] fields = vs.keySet().toArray(new String[] {});
        String[] fieldValues = vs.values().toArray(new String[] {});
        try {
            client.update(table, key, fields, fieldValues);

            return Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
        }
        return Status.ERROR;
    }

    /**
     * Insert.
     */
    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        Map<String, String> vs = StringByteIterator.getStringMap(values);
        String[] fields = vs.keySet().toArray(new String[] {});
        String[] fieldValues = vs.values().toArray(new String[] {});
        try {
            client.insertOrUpdate(table, key, fields, fieldValues);

            return Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
        }
        return Status.ERROR;
    }

    /**
     * Delete.
     */
    @Override
    public Status delete(String table, String key) {
        try {
            return client.delete(table, key) == 0 ? Status.ERROR : Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
        }

        return Status.ERROR;
    }
}

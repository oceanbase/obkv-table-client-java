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

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.yahoo.ycsb.*;

import java.util.*;

public class YcsbBenchClient extends DB {

    public static final String OB_TABLE_CLIENT_FULL_USER_NAME   = "ob.table.client.fullUserName";
    public static final String OB_TABLE_CLIENT_PARAM_URL        = "ob.table.client.paramURL";
    public static final String OB_TABLE_CLIENT_PASSWORD         = "ob.table.client.password";
    public static final String OB_TABLE_CLIENT_SYS_USER_NAME    = "ob.table.client.sysUserName";
    public static final String OB_TABLE_CLIENT_SYS_PASSWORD     = "ob.table.client.sysPassword";
    public static final String OB_TABLE_CLIENT_SYS_ENC_PASSWORD = "ob.table.client.sysEncPassword";

    ObTableClient              obTableClient;
    String[]                   fields                           = new String[] { "FIELD0",
            "FIELD1", "FIELD2", "FIELD3", "FIELD4", "FIELD5", "FIELD6", "FIELD7", "FIELD8",
            "FIELD9"                                           };

    /**
     * Init.
     */
    public void init() throws DBException {
        Properties props = getProperties();

        String fullUserName = props.getProperty(OB_TABLE_CLIENT_FULL_USER_NAME);

        if (fullUserName == null) {
            throw new DBException("fullUserName can not be null");
        }

        String paramURL = props.getProperty(OB_TABLE_CLIENT_PARAM_URL);

        if (paramURL == null) {
            throw new DBException("paramURL can not be null");
        }

        String password = props.getProperty(OB_TABLE_CLIENT_PASSWORD);

        if (password == null) {
            password = "";
        }

        String sysUserName = props.getProperty(OB_TABLE_CLIENT_SYS_USER_NAME);
        if (password == null) {
            throw new DBException("sysUserName can not be null");
        }

        String sysPassword = props.getProperty(OB_TABLE_CLIENT_SYS_PASSWORD);
        String encryptedSysPassword = props.getProperty(OB_TABLE_CLIENT_SYS_ENC_PASSWORD);

        try {
            final ObTableClient obTableClient = new ObTableClient();
            obTableClient.setFullUserName(fullUserName);
            obTableClient.setParamURL(paramURL);
            obTableClient.setPassword(password);
            obTableClient.setSysUserName(sysUserName);
            obTableClient.setSysPassword(sysPassword);
            obTableClient.setEncSysPassword(encryptedSysPassword);
            obTableClient.init();
            this.obTableClient = obTableClient;
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
            Iterator i$ = obTableClient.get(table, key, fs).entrySet().iterator();

            while (i$.hasNext()) {
                Map.Entry<String, Object> entry = (Map.Entry) i$.next();
                result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
            }

            return result.isEmpty() ? Status.ERROR : Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
            System.err.println("fullUserName:" + obTableClient.getFullUserName() + " url :" // NOPMD
                               + obTableClient.getParamURL()); // NOPMD
        }
        return null;
    }

    /**
     * Scan.
     */
    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {

        try {
            String[] fs = this.fields;
            if (fields != null) {
                fs = fields.toArray(new String[] {});
            }
            TableQuery tableQuery = obTableClient.query(table);
            tableQuery.limit(recordcount);
            tableQuery.select(fs);
            tableQuery.addScanRange(startkey, ObObj.getMax());
            QueryResultSet set = tableQuery.execute();

            while (set.next()) {
                HashMap<String, ByteIterator> map = new HashMap<String, ByteIterator>();
                Map<String, Object> row = set.getRow();
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    map.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
                }
                result.add(map);
            }

            return result.isEmpty() ? Status.ERROR : Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
            System.err.println("fullUserName:" + obTableClient.getFullUserName() + " url :" // NOPMD
                               + obTableClient.getParamURL()); // NOPMD
        }
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
            obTableClient.update(table, key, fields, fieldValues);

            return Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
            System.err.println("fullUserName:" + obTableClient.getFullUserName() + " url :" // NOPMD
                               + obTableClient.getParamURL()); // NOPMD
        }
        return Status.ERROR;
    }

    /**
     * Insert.
     */
    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            Map<String, String> vs = StringByteIterator.getStringMap(values);
            String[] fields = vs.keySet().toArray(new String[] {});
            String[] fieldValues = vs.values().toArray(new String[] {});
            obTableClient.insertOrUpdate(table, key, fields, fieldValues);

            return Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
            System.err.println("fullUserName:" + obTableClient.getFullUserName() + " url :" // NOPMD
                               + obTableClient.getParamURL());// NOPMD
        }
        return Status.ERROR;
    }

    /**
     * Delete.
     */
    @Override
    public Status delete(String table, String key) {
        try {
            return obTableClient.delete(table, key) == 0 ? Status.ERROR : Status.OK;
        } catch (Exception e) {
            e.printStackTrace(); // NOPMD
            System.err.println("fullUserName:" + obTableClient.getFullUserName() + " url :" // NOPMD
                               + obTableClient.getParamURL()); // NOPMD
        }

        return Status.ERROR;
    }
}

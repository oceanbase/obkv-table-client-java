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

package com.oceanbase.example;


import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;

import java.util.Map;
import java.util.List;


/*
* 此demo只用作指导使用tableapi，测试前请先使用下列schema在SQL中建立table
* */

/* 测试表schema:
CREATE TABLE IF NOT EXISTS `kv_table` (
    `key` varchar(20) NOT NULL,
    `val` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`key`)
);
*/

public class KVClient {
    private ObTableClient obTableClient = null;
    private String tableName = "kv_table";

    public boolean initial() {
        try {
            obTableClient = new ObTableClient();
            obTableClient.setFullUserName("your user name"); // e.g. root@sys#ocp
            obTableClient.setParamURL("your configurl + database=xxx"); // e.g. http://ip:port/services?Action=ObRootServiceInfo&ObRegion=ocp&database=test
            obTableClient.setPassword("your user passwd");
            obTableClient.setSysUserName("your sys user"); // e.g. proxyro@sys
            obTableClient.setSysPassword("your sys user passwd");
            obTableClient.init();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("fail to init KV client, " + e);
            obTableClient = null;
            return false;
        }
        System.out.println("initial kv client success");
        return true;
    }

    public void close() {
        if (obTableClient == null) {
            return;
        }
        try {
            obTableClient.close();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("fail to close kv client, " + e);
        }
    }

    public boolean put(Object key, String value) {
        if (obTableClient == null) {
            System.out.println("kv client is null");
            return false;
        }
        try {
            long rows = obTableClient.insert(tableName, key, new String[]{"val"}, new Object[]{value});
            if (rows != 1) {
                System.out.println("fail to put kv data");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("fail to put kv data, " + e);
            return false;
        }
        System.out.println("put kv data success");
        return true;
    }

    // return all column of row
    public String get(Object key) {
        if (obTableClient == null) {
            System.out.println("kv client is null");
            return null;
        }
        try {
            Map<String, Object> result = obTableClient.get(tableName, key, new String[]{"val"});
            return (String)result.get("val");
        } catch (Exception e) {
            System.out.println("fail to get kv data");
            return null;
        }
    }

    public boolean update(Object key, String val) {
        if (obTableClient == null) {
            System.out.println("kv client is null");
            return false;
        }
        try {
            long rows = obTableClient.update(tableName, key, new String[]{"val"}, new Object[]{val});
            if (rows != 1) {
                System.out.println("fail to put kv data");
                return false;
            }
        } catch (Exception e) {
            System.out.println("fail to get kv data");
            return false;
        }
        System.out.println("update kv data success");
        return true;
    }
    // del
    public boolean del(Object key) {
        if (obTableClient == null) {
            System.out.println("kv client is null");
            return false;
        }
        try {
            long rows = obTableClient.delete(tableName, key);
            if (rows != 1) {
                System.out.println("del kv data success, row = 0");
                return true;
            }
        } catch (Exception e) {
            System.out.println("fail to del kv data");
            return false;
        }
        System.out.println("del kv data success, row = 1");
        return true;
    }

    // insert or update
    public boolean insertOrUpdate(Object key, String val) {
        if (obTableClient == null) {
            System.out.println("kv client is null");
            return false;
        }
        try {
            long rows = obTableClient.insertOrUpdate(tableName, key, new String[]{"val"}, new Object[]{val});
            if (rows != 1) {
                System.out.println("insertOrUpdate kv data success, row = 0");
                return true;
            }
        } catch (Exception e) {
            System.out.println("fail to insertOrUpdate kv data" + e);
            return false;
        }
        System.out.println("insertOrUpdate kv data success, row = 1");
        return true;
    }

    // replace
    public boolean replace(Object key,  String replace_val) {
        if (obTableClient == null) {
            System.out.println("kv client is null");
            return false;
        }
        try {
            long rows = obTableClient.replace(tableName, key, new String[]{"val"}, new Object[]{replace_val});
            if (rows != 1) {
                System.out.println("replace kv data success, row = 0");
                return true;
            }
        } catch (Exception e) {
            System.out.println("fail to replace kv data" + e);
            return false;
        }
        System.out.println("replace kv data success, row = 1");
        return true;
    }

    // append
    public String append(Object key, String append_str) {
        if (obTableClient == null) {
            System.out.println("kv client is null");
            return null;
        }
        try {
            Map<String, Object> result = obTableClient.append(tableName, key, new String[]{"val"}, new Object[]{append_str}, (boolean)true);
            return (String)result.get("val");
        } catch (Exception e) {
            System.out.println("fail to increment kv data: " + e);
            return null;
        }
    }

    // batch
    public boolean batch() {
        if (obTableClient == null) {
            System.out.println("kv client is null");
            return false;
        }
        try {
            TableBatchOps batchOps = obTableClient.batch(tableName);
            batchOps.get((String)"key1", new String[]{"val"});
            batchOps.insert((String)"key5", new String[]{"val"}, new Object[]{"insert key5"});
            batchOps.update((String)"key5", new String[]{"val"}, new Object[]{"update key5"});
            batchOps.insertOrUpdate((String)"key6", new String[]{"val"}, new Object[]{"insertOrUpdate key6"});
            batchOps.append((String)"key3", new String[]{"val"}, new Object[]{"_append"}, (boolean)true);
            List<Object> retObj = batchOps.execute();
            if (retObj.size() != 5) {
                System.out.println("batch Ops error");
                return false;
            }
            System.out.println("batch Ops success.");
            return true;
        } catch (Exception e) {
            System.out.println("fail to execute batch ops: " + e);
            return false;
        }
    }
}

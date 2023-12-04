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
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.Map;
import java.util.List;


/*
* 此demo只用作指导使用tableapi，测试前请先使用下列schema在SQL中建立table
* */

/* 测试表schema:
CREATE TABLE IF NOT EXISTS `test_table` (
    `c1` bigint NOT NULL,
    `c2` int NOT NULL,
    `c3` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`)
);
*/

public class TableClient {
    private ObTableClient obTableClient = null;
    private String tableName = "test_table";

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
            System.out.println("fail to init table client, " + e);
            obTableClient = null;
            return false;
        }
        System.out.println("initial table client success");
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
            System.out.println("fail to close table client, " + e);
        }
    }

    public boolean put(Object key, Object[] values) {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            long rows = obTableClient.insert(tableName, key, new String[]{"c2", "c3"}, values);
            if (rows != 1) {
                System.out.println("fail to put table data");
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("fail to put table data, " + e);
            return false;
        }
        System.out.println("put table data success");
        return true;
    }

    // return all column of row
    public Object[] get(Object key) {
        Object[] colResult = new Object[2];
        if (obTableClient == null) {
            System.out.println("table client is null");
            return null;
        }
        try {
            Map<String, Object> result = obTableClient.get(tableName, key, new String[]{"c2","c3"});
            colResult[0] = (int)result.get("c2");
            colResult[1] = (String)result.get("c3");
            return colResult;
        } catch (Exception e) {
            System.out.println("fail to get table data");
            return null;
        }
    }

    public boolean update(Object key, Object[] val) {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            long rows = obTableClient.update(tableName, key, new String[]{"c2","c3"}, val);
            if (rows != 1) {
                System.out.println("fail to put table data");
                return false;
            }
        } catch (Exception e) {
            System.out.println("fail to get table data");
            return false;
        }
        System.out.println("update table data success");
        return true;
    }
    // del
    public boolean del(Object key) {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            long rows = obTableClient.delete(tableName, key);
            if (rows != 1) {
                System.out.println("del table data success, row = 0");
                return true;
            }
        } catch (Exception e) {
            System.out.println("fail to del table data");
            return false;
        }
        System.out.println("del table data success, row = 1");
        return true;
    }

    // insert or update
    public boolean insertOrUpdate(Object key, Object[] val) {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            long rows = obTableClient.insertOrUpdate(tableName, key, new String[]{"c2", "c3"}, val);
            if (rows != 1) {
                System.out.println("insertOrUpdate table data success, row = 0");
                return true;
            }
        } catch (Exception e) {
            System.out.println("fail to insertOrUpdate table data" + e);
            return false;
        }
        System.out.println("insertOrUpdate table data success, row = 1");
        return true;
    }

    // replace
    public boolean replace(Object key, Object[] replace_val) {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            long rows = obTableClient.replace(tableName, key, new String[]{"c2","c3"}, replace_val);
            if (rows != 1) {
                System.out.println("replace table data success, row = 0");
                return true;
            }
        } catch (Exception e) {
            System.out.println("fail to replace table data" + e);
            return false;
        }
        System.out.println("replace table data success, row = 1");
        return true;
    }

    // increment
    public boolean increment(Object key, int inc_val) {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            // get old value first
            Map<String, Object> result;
            int oldVal, newVal;
            result = obTableClient.get(tableName, key, new String[]{"c2","c3"});
            if (result == null) {
                return false;
            }
            oldVal = (int)result.get("c2");
            // increment and get new value;
            result = obTableClient.increment(tableName, key, new String[]{"c2"}, new Object[]{inc_val}, (boolean)true);
            if (result == null) {
                return false;
            }
            newVal = (int)result.get("c2");
            if (oldVal + inc_val != newVal) {
                return false;
            }
            return true;
        } catch (Exception e) {
            System.out.println("fail to increment table data: " + e);
            return false;
        }
    }

    // append
    public String append(Object key, String append_str) {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return null;
        }
        try {
            Map<String, Object> result = obTableClient.append(tableName, key, new String[]{"c3"}, new Object[]{append_str}, (boolean)true);
            return (String)result.get("c3");
        } catch (Exception e) {
            System.out.println("fail to increment table data: " + e);
            return null;
        }
    }

    // scan
    public boolean query() {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            TableQuery tblQuery = obTableClient.query(tableName);
            QueryResultSet resultSet = tblQuery.select("c1","c2","c3").primaryIndex()
                    .addScanRange((long)1, (long)5).execute();
            // print scan result
            System.out.println("query result size: " + resultSet.cacheSize());
            int size = resultSet.cacheSize();
            for (int i = 0; i < size; i++) {
                resultSet.next();
                Map<String, Object> row = resultSet.getRow(); // get one row
                for (Map.Entry<String, Object> entry : row.entrySet()) {
                    System.out.println(entry.getKey() + "," + entry.getValue());
                }
            }
        } catch (Exception e) {
            System.out.println("fail to increment table data: " + e);
            return false;
        }
        return true;
    }

    // batch
    public boolean batch() {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            TableBatchOps batchOps = obTableClient.batch(tableName);
            batchOps.get((long)1, new String[]{"c2", "c3"});
            batchOps.insert((long)5, new String[]{"c2", "c3"}, new Object[]{(int)5, "batch new c3_5"});
            batchOps.update((long)5, new String[]{"c2"}, new Object[]{(int)55});
            batchOps.insertOrUpdate((long)6, new String[]{"c2","c3"}, new Object[]{(int)6, "batch new c3_6"});
            batchOps.increment((long)6, new String[]{"c2"}, new Object[]{(int)1}, (boolean)true);
            batchOps.append((long)6, new String[]{"c3"}, new Object[]{"_append"}, (boolean)true);
            List<Object> retObj = batchOps.execute();
            if (retObj.size() != 6) {
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

    // batch for single tablet: one of the the operation execute failed,
    // batch will rollback and return the first error code
    public boolean batch2() {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            TableBatchOps batchOps = obTableClient.batch(tableName);
            batchOps.get((long)1, new String[]{"c2", "c3"});
            batchOps.insert((long)5, new String[]{"c2", "c3"}, new Object[]{(int)5, "batch new c3_5"});
            // insert a row with duplicated primary key, return error
            batchOps.insert((long)5, new String[]{"c2", "c3"}, new Object[]{(int)5, "batch new c3_5"});
            batchOps.update((long)5, new String[]{"c2"}, new Object[]{(int)55});
            List<Object> retObj = batchOps.execute();
            if (retObj.size() != 4) {
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

    /*
        schema:
        CREATE TABLE IF NOT EXISTS `test_partition_table` (
            `c1` bigint NOT NULL,
            `c2` int NOT NULL,
            `c3` varchar(20) DEFAULT NULL,
            PRIMARY KEY (`c1`)
        ) partition by key(`c1`) partitions 3;

        Note: We cannot ensure atomicity when a batch has operations across tablets/partitions,
              only ensure atomicity for operations on the same partition.
        For example:
            test_partition_table is a partition table and will have three tablets in observer.
            Rows with key '1' and '4' are in same tablets, they will execute atomic. However,
            Rows with key '1' and '3' are in diferent tablets, their execution are not atomic
            if one of them execute failed.
    */
    public boolean batch3() {
        if (obTableClient == null) {
            System.out.println("table client is null");
            return false;
        }
        try {
            tableName = "test_partition_table";
            TableBatchOps batchOps = obTableClient.batch(tableName);
            batchOps.insert((long)1, new String[]{"c2", "c3"}, new Object[]{(int)1, "batch new c3_5"});
            batchOps.insert((long)2, new String[]{"c2", "c3"}, new Object[]{(int)2, "batch new c3_5"});
            batchOps.insert((long)3, new String[]{"c2", "c3"}, new Object[]{(int)3, "batch new c3_5"});
            batchOps.insert((long)4, new String[]{"c2", "c3"}, new Object[]{(int)4, "batch new c3_5"});
            List<Object> retObj = batchOps.execute();
            if (retObj.size() != 4) {
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

    // todo: others op.
}

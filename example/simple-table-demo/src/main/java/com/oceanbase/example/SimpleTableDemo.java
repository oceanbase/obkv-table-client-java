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

public class SimpleTableDemo {

    public static void main(String[] args) {
        TableClient tableClient = new TableClient();
        try {
            if (!tableClient.initial()) {
                return;
            }
            // del
            tableClient.del((long)1);
            tableClient.del((long)2);
            tableClient.del((long)3);

            // get after del
            System.out.println("get after delete ...");
            System.out.println("key_1 -> " + tableClient.get((long)1));
            System.out.println("key_2 -> " + tableClient.get((long)2));
            System.out.println("key_3 -> " + tableClient.get((long)3));

            // insert
            tableClient.put((long)1, new Object[]{(int)11, "c3_1"});
            tableClient.put((long)2, new Object[]{(int)22, "c3_2"});
            tableClient.put((long)3, new Object[]{(int)33, "c3_3"});

            // get after insert
            System.out.println("get after insert ...");
            System.out.println("key_1 -> " + "c2:" + tableClient.get((long)1)[0] + ", c3:" + tableClient.get((long)1)[1]);
            System.out.println("key_2 -> " + "c2:" + tableClient.get((long)2)[0] + ", c3:" + tableClient.get((long)2)[1]);
            System.out.println("key_3 -> " + "c2:" + tableClient.get((long)3)[0] + ", c3:" + tableClient.get((long)3)[1]);

            // update
            boolean updateRet = tableClient.update((long)1,  new Object[]{(int)111, "update c3_1"});
            if (!updateRet) {
                System.out.println("update error");
            } else {
                // get after update
                System.out.println("key_1 -> " + "c2:" + tableClient.get((long)1)[0] + ", c3:" + tableClient.get((long)1)[1]);
            }

            // insert or update
            // insert new row
            boolean insertOrUpdateRet1 = tableClient.insertOrUpdate((long)4, new Object[]{(int)44, "insert c3_4"});
            if (!insertOrUpdateRet1) {
                System.out.println("insertOrUpdate error");
            }
            // insert or update old row
            boolean insertOrUpdateRet2 = tableClient.insertOrUpdate((long)3, new Object[]{(int)333, "update old c3_3"});
            if (!insertOrUpdateRet2) {
                System.out.println("insertOrUpdate error");
            } else {
                // get after insertOrUpdate
                System.out.println("get after insertOrUpdate ...");
                System.out.println("key_3 -> " + "c2:" + tableClient.get((long)3)[0] + ", c3:" + tableClient.get((long)3)[1]);
                System.out.println("key_4 -> " + "c2:" + tableClient.get((long)4)[0] + ", c3:" + tableClient.get((long)4)[1]);
            }

            // replace of row 2
            boolean replace_ret = tableClient.replace((long)2, new Object[]{(int)222, "replace c3_2"});
            if (!replace_ret) {
                System.out.println("replace_ret error");
            } else {
                System.out.println("key_2 -> " + "c2:" + tableClient.get((long)2)[0] + ", c3:" + tableClient.get((long)2)[1]);
            }

            // increment of row 4 c2
            boolean inc_result = tableClient.increment((long)4, (int)1);
            if (!inc_result) {
                System.out.println("increment error");
            } else {
                System.out.println("get after increment ...");
                System.out.println("key_4 -> " + "c2:" + tableClient.get((long)4)[0] + ", c3:" + tableClient.get((long)4)[1]);
            }

            // append
            String append_ret = tableClient.append((long)4, (String)"_append");
            if (append_ret == null) {
                System.out.println("append error");
            } else {
                System.out.println("get after append ...");
                System.out.println("key_4 -> " + "c2:" + tableClient.get((long)4)[0] + ", c3:" + tableClient.get((long)4)[1]);
            }

            // batch
            boolean batchOps = tableClient.batch();
            if (!batchOps) {
                System.out.println("batch ops error");
            } else {
                System.out.println("get after batch ops ...");
                System.out.println("key_5 -> " + "c2:" + tableClient.get((long)5)[0] + ", c3:" + tableClient.get((long)5)[1]);
                System.out.println("key_6 -> " + "c2:" + tableClient.get((long)6)[0] + ", c3:" + tableClient.get((long)6)[1]);
            }

            // scan
            boolean queryRet = tableClient.query();
            if (!queryRet) {
                System.out.println("query table error");
            } else {
                System.out.println("query table success.");
            }

        } finally {
            tableClient.close();
        }
    }
}

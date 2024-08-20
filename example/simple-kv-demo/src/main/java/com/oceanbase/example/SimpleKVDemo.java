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

public class SimpleKVDemo {

    public static void main(String[] args) {
        KVClient kvClient = new KVClient();
        try {
            if (!kvClient.initial()) {
                return;
            }
            // del
            kvClient.del((String)"key1");
            kvClient.del((String)"key2");
            kvClient.del((String)"key3");

            // get after del
            System.out.println("get after delete ...");
            System.out.println("key1 -> " + kvClient.get("key1"));
            System.out.println("key2 -> " + kvClient.get("key2"));
            System.out.println("key3 -> " + kvClient.get("key3"));

            // insert
            kvClient.put((String)"key1", "val1");
            kvClient.put((String)"key2", "val2");
            kvClient.put((String)"key3", "val3");

            // get after insert
            System.out.println("get after insert ...");
            System.out.println("key1 -> " + kvClient.get("key1"));
            System.out.println("key2 -> " + kvClient.get("key2"));
            System.out.println("key3 -> " + kvClient.get("key3"));

            // update
            boolean updateRet = kvClient.update((String)"key1",  (String)"update val1");
            if (!updateRet) {
                System.out.println("update error");
            } else {
                // get after update
                System.out.println("key1 -> " + kvClient.get("key1"));
            }

            // insert or update
            // insert new row
            boolean insertOrUpdateRet1 = kvClient.insertOrUpdate((String)"key4", (String)"insert val4");
            if (!insertOrUpdateRet1) {
                System.out.println("insertOrUpdate error");
            }
            // insert or update old row
            boolean insertOrUpdateRet2 = kvClient.insertOrUpdate((String)"key3", (String)"update old val3");
            if (!insertOrUpdateRet2) {
                System.out.println("insertOrUpdate error");
            } else {
                // get after insertOrUpdate
                System.out.println("get after insertOrUpdate ...");
                System.out.println("key3 -> " + kvClient.get("key3"));
                System.out.println("key4 -> " + kvClient.get("key4"));
            }

            // replace of row 2
            boolean replace_ret = kvClient.replace((String)"key2", (String)"replace val2");
            if (!replace_ret) {
                System.out.println("replace_ret error");
            } else {
                System.out.println("key2 -> " + kvClient.get("key2"));
            }

            // append
            String append_ret = kvClient.append((String)"key4", (String)"_append");
            if (append_ret == null) {
                System.out.println("append error");
            } else {
                System.out.println("get after append ...");
                System.out.println("key4 -> " + kvClient.get("key4"));
            }

            // batch
            boolean batchOps = kvClient.batch();
            if (!batchOps) {
                System.out.println("batch ops error");
            } else {
                System.out.println("get after batch ops ...");
                System.out.println("key5 -> " + kvClient.get("key5"));
                System.out.println("key6 -> " + kvClient.get("key6"));
            }
        } finally {
            kvClient.close();
        }
    }
}

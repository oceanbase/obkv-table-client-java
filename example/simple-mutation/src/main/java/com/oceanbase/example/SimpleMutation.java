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

import com.alipay.oceanbase.rpc.Mutation;

/* table schema:
CREATE TABLE IF NOT EXISTS `kv_table` (
    `key` varchar(20) NOT NULL,
    `val` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`key`)
);
*/

public class SimpleMutation {

    public static void main(String[] args) {
        MutationUtil mutationUtil = new MutationUtil();
        ObTableClient tableClient = null;
        try {
            if (!mutationUtil.initial()) {
                return;
            }
            tableClient = mutationUtil.getClient();

            // ColumnValue -> key-value pair
            ColumnValue columnValue = colval("key", "value");

            // Row -> multi-ColumnValue (row)
            Row row = row(columnValue, new ColumnValue("other_key", "other_value"));

            // insert
            Insert insert = tableClient.insert("kv_table")
                    .setRowKey(row(colVal("key", "key_0")))
                    .addMutateColVal(colVal("val", "val_0"));
            MutationResult result = insert.execute();
            System.out.println("insert " + (result.getAffectedRows()) + " rows");

            // update
            Update update = tableClient.update("kv_table")
                    .setRowKey(row(colVal("key", "key_0")))
                    .addMutateColVal(colVal("val", "val_1"))
                    .setFilter(compareVal(ObCompareOp.EQ, "val", "val_0")); // not available, coming soon
            MutationResult result = update.execute();
            System.out.println("update " + result.getAffectedRows() + " rows");

            // delete
            Delete delete = tableClient.delete("kv_table")
                    .setRowKey(row(colVal("key", "key_0")))
                    .setFilter(compareVal(ObCompareOp.EQ, "val", "val_0")); // not available, coming soon
            MutationResult result = delete.execute();
            System.out.println("delete " + (result.getAffectedRows()) + " rows");

            // insertOrUpdate
            InsertOrUpdate insertOrUpdate = tableClient.insertOrUpdate("kv_table")
                    .setRowKey(row(colVal("key", "key_0")))
                    .addMutateColVal(colVal("val", "val_0"));
            MutationResult result = insertOrUpdate.execute();
            System.out.println("insertOrUpdate " + (result.getAffectedRows()) + " rows");

            // replace
            Replace replace = tableClient.replace("kv_table")
                    .setRowKey(row(colVal("key", "key_0")))
                    .addMutateColVal(colVal("val", "val_0"));
            MutationResult result = replace.execute();
            System.out.println("replace " + (result.getAffectedRows()) + " rows");

            // increment
            Increment increment = tableClient.increment("kv_table")
                    .setRowKey(row(colVal("key", "key_0")))
                    .addMutateColVal(colVal("val", "val_1"))
                    .setFilter(compareVal(ObCompareOp.EQ, "val", "val_0")); // not available, coming soon
            MutationResult result = increment.execute();
            System.out.println("increment " + result.getAffectedRows() + " rows");

            // append
            Append append = tableClient.append("kv_table")
                    .setRowKey(row(colVal("key", "key_0")))
                    .addMutateColVal(colVal("val", "val_1"))
                    .setFilter(compareVal(ObCompareOp.EQ, "val", "val_0")); // not available, coming soon
            MutationResult result = append.execute();
            System.out.println("append " + result.getAffectedRows() + " rows");


            // batch operation
            // construct single mutation
            // filter could not be used in all operation in batchOperation
            Insert insert_0 = insert().setRowKey(colVal("key", "key_1"))
                    .addMutateColVal(colVal("val", "val_1"))

            Insert insert_1 = insert().setRowKey(colVal("key", "key_1"))
                    .addMutateColVal(colVal("val", "val_1"))

            Update update_0 = update().setRowKey(colVal("key", "key_1"))
                    .addMutateColVal(colVal("val", "val_2"))

            // construct batch operation
            BatchMutationResult batchResult = client.batchMutation("kv_table")
                    .addMutation(insert_0)
                    .addMutation(insert_1, update_0)
                    .execute();

            // print output
            for (int idx : batchResult.size()) {
                System.out.println(String.format("the %dth mutation affect %d rows", idx,
                        batchResult.get(idx).getAffectedRows()));
            }

        } finally {
            if (tableClient != null) {
                tableClient.close();
            }
        }
    }
}

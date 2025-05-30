/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.get.Get;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;
import static org.junit.Assert.assertEquals;

/**
 * create table test_json(k bigint, v json, primary key(k)) partition by key(k) partitions 97;
  create table test_json_not_null(k bigint, v json NOT NULL, primary key(k)) partition by key(k) partitions 97;
 * create table test_json_append_incre(k bigint, v json, id int, str varchar(50), primary key(k)) partition by key(k) partitions 97;
 */
class JsonTestUtils {
    public static String generateRandomString(int length) {
        Random rand = new Random();
        char[] chars = new char[length];
        for (int i = 0; i < length; i++) {
            chars[i] = (char) (rand.nextInt(94) + 33);
        }
        return new String(chars);
    }

    public static String Case1() {
        JSONObject obj = new JSONObject();
        for (int i = 0; i < 10; i++) {
            obj.put("k" + i, generateRandomString(90));
        }
        return obj.toJSONString();
    }

    public static String Case2() {
        JSONObject obj = new JSONObject();
        for (int i = 0; i < 100; i++) {
            obj.put("k"+i, generateRandomString(6));
        }
        return obj.toJSONString();
    }

    public static JSONObject recJson(int curL, int maxLevel) {
        JSONObject obj = new JSONObject();
        if (curL == maxLevel) {
            String big_str = generateRandomString(80);
            obj.put("key"+ curL, big_str);
        }

        if (curL < maxLevel) {
            obj.put("key" + curL, generateRandomString(80));
            obj.put("L" + curL, recJson(curL + 1, maxLevel));
        }
        return obj;
    }

    public static String Case3() {
        return recJson(1, 10).toJSONString();
    }

    public static boolean areJsonStringsEqual(String json1, String json2) {
        JSONObject obj1 = JSON.parseObject(json1);
        JSONObject obj2 = JSON.parseObject(json2);
        return obj1.equals(obj2);
    }

    // generate json whose depth can up to 100
    public static JSONObject generateLevel(int currentLevel, int maxLevel) {
        JSONObject obj = new JSONObject();
//        obj.put("level", currentLevel);

        if (currentLevel < maxLevel) {
            obj.put("next", generateLevel(currentLevel + 1, maxLevel));
        }

        return obj;
    }

    public static String generateJsonString(int maxLevel) {
        // 生成根节点
        JSONObject nestedJson = generateLevel(0, maxLevel);

        // 输出格式化后的 JSON 字符串
        return JSON.toJSONString(nestedJson, true);
    }
}

public class ObTableJsonTest {
    ObTableClient client;
    public static String tableName = "test_json";
    public static String tableNameNotNull = "test_json_not_null";
    public static String extraTable = "test_json_append_incre";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    /* test simple insert*/
    @Test
    public void testSimpleInsert() throws Exception {
        long key_bigint = 1;
        String val = "{\"key\":\"value\"}";
        String new_val = "{\"key\":\"new_value\"}";
        Row rowKey = row().add(colVal("k", key_bigint));
        Row prop = row().add(colVal("v", ""));
        // insert wrong type on column
        try {
            client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key_bigint)))
                    .addMutateColVal(colVal("v", key_bigint)) // err: v should be json
                    .execute();

            Assert.fail("Expected an exception to be thrown");
        } catch (Exception e) {
            System.out.println("insert mismatched type test succeed, val must be json "
                    + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("OB_KV_COLUMN_TYPE_NOT_MATCH"));
        }

        // insert byte[] on column
        try {
            byte[] bytearr = "{\"key\":\"new_value\"}".getBytes();
            client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key_bigint)))
                    .addMutateColVal(colVal("v", bytearr)) // err: v should be json
                    .execute();

            Map<String, Object> res = client.get(tableName).setRowKey(row(colVal("k", key_bigint)))
                    .select("v").execute();
            System.out.println(res.get("v").toString());
        } catch (Exception e) {
            System.out.println("insert mismatched type test succeed" + e.getMessage());
            Assert.assertTrue(e.getMessage().contains("OB_KV_COLUMN_TYPE_NOT_MATCH"));
        }

        // insert wrong type on key
        try {
            client.insertOrUpdate(tableName).setRowKey(row(colVal("k", val)))
                    .addMutateColVal(colVal("v", val)) // err: v should be json
                    .execute();

            Assert.fail("Expected an exception to be thrown");
        } catch (Exception e) {
            System.out.println("insert json on key test succeed, " + e.getMessage());
        }

        // simple insert(), insert empty value
        try {
            Row properties = row().add(colVal("v", ""));
            client.insert(tableName).setRowKey(rowKey).addMutateRow(properties).execute();
            Assert.fail("Expected a failure for trying to insert an empty json value");
        } catch (Exception e) {
            System.out.println("insert empty json test succeed, " + e.getMessage());
        }

        // simple insert(), insert the same key twice
        try {
            Row properties = row().add(colVal("v", val));
            client.insert(tableName).setRowKey(rowKey).addMutateRow(properties).execute();
            Map<String, Object> res = client.get(tableName).setRowKey(rowKey).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res.get("v").toString(), val));
            // should fail
            client.insert(tableName).setRowKey(rowKey).addMutateRow(properties).execute();
            Assert.fail("Expected a failure for insert an existed key, value");
        } catch (Exception e) {
            System.out.println("simple insert succeed, " + e.getMessage());
        } finally {
            client.delete(tableName).setRowKey(rowKey).execute();
        }

        // simple insert_or_update(), insert the same key twice with different val
        try {
            Row properties = row().add(colVal("v", val));
            Row new_properties = row().add(colVal("v", new_val));
            client.insertOrUpdate(tableName).setRowKey(rowKey).addMutateRow(properties).execute();
            Map<String, Object> res = client.get(tableName).setRowKey(rowKey).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res.get("v").toString(), val));
            // should succeed
            client.insertOrUpdate(tableName).setRowKey(rowKey).addMutateRow(new_properties)
                    .execute();
            res = client.get(tableName).setRowKey(rowKey).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res.get("v").toString(), new_val));
        } catch (Exception e) {
            System.out.println("simple insert_or_update fail, " + e.getMessage());
        } finally {
            client.delete(tableName).setRowKey(rowKey).execute();
        }

        // simple replace
        try {
            Row properties = row().add(colVal("v", val));
            Row new_properties = row().add(colVal("v", new_val));
            // replace first time, should insert new kv
            client.replace(tableName).setRowKey(rowKey).addMutateRow(properties).execute();
            Map<String, Object> res = client.get(tableName).setRowKey(rowKey).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res.get("v").toString(), val));
            // replace second time, should replace old_kv with new_kv
            client.replace(tableName).setRowKey(rowKey).addMutateRow(new_properties).execute();
            res = client.get(tableName).setRowKey(rowKey).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res.get("v").toString(), new_val));

        } catch (Exception e) {
            System.out.println("simple replace fail, " + e.getMessage());
        } finally {
            client.delete(tableName).setRowKey(rowKey).execute();
        }

        // simple put, should fail because jsontype has lob header
        try {
            Row properties = row().add(colVal("v", val));
            client.put(tableName).setRowKey(rowKey).addMutateRow(properties).execute();
            Assert.fail("table with lob column use put not supported");
        } catch (Exception e) {
            System.out.println("simple put succeed, " + e.getMessage());
        } finally {
            client.delete(tableName).setRowKey(rowKey).execute();
        }
    }

    // test batch insert
    @Test
    public void testBatchInsertJson() throws Exception {
        String json1 = "{\"name\":\"zhangsan\",\"age\":30}";
        String json2 = "{\"name\":\"lisi\",\"age\":25}";

        Row row1 = row(colVal("k", (long) 1));
        Row row2 = row(colVal("k", (long) 2));

        Row prop1 = row().add(colVal("v", json1));
        Row prop2 = row().add(colVal("v", json2));

        try {

            Insert insertOp1 = insert().setRowKey(row1).addMutateRow(prop1);
            Insert insertOp2 = insert().setRowKey(row2).addMutateRow(prop2);

            BatchOperationResult result = client.batchOperation(tableName)
                    .addOperation(insertOp1, insertOp2).execute();

            //            +---+---------------------------------+
            //            | k | v                               |
            //            +---+---------------------------------+
            //            | 2 | {"age": 25, "name": "lisi"}     |
            //            | 1 | {"age": 30, "name": "zhangsan"} |
            //            +---+---------------------------------+
            Map<String, Object> res = client.get(tableName).setRowKey(row1).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(json1, res.get("v").toString()));
            res = client.get(tableName).setRowKey(row2).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(json2, res.get("v").toString()));

            // batch update
            InsertOrUpdate insertUpOp1 = insertOrUpdate().setRowKey(row1).addMutateRow(prop2);
            InsertOrUpdate insertUpOp2 = insertOrUpdate().setRowKey(row2).addMutateRow(prop1);
            client.batchOperation(tableName).addOperation(insertUpOp1, insertUpOp2).execute();
            //            +---+---------------------------------+
            //            | k | v                               |
            //            +---+---------------------------------+
            //            | 2 | {"age": 30, "name": "zhangsan"} |
            //            | 1 | {"age": 25, "name": "lisi"}     |
            //            +---+---------------------------------+
            res = client.get(tableName).setRowKey(row1).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(json2, res.get("v").toString()));
            res = client.get(tableName).setRowKey(row2).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(json1, res.get("v").toString()));

            // batch replace
            String replacedJson1 = "{\"name\":\"replaced\",\"age\":99}";
            Replace replaceOp = replace().setRowKey(row1).addMutateColVal(
                    colVal("v", replacedJson1));
            client.batchOperation(tableName).addOperation(replaceOp).execute();
            //            +---+---------------------------------+
            //            | k | v                               |
            //            +---+---------------------------------+
            //            | 2 | {"age": 30, "name": "zhangsan"} |
            //            | 1 | {"age": 99, "name": "replaced"} |
            //            +---+---------------------------------+
            res = client.get(tableName).setRowKey(row1).select("v").execute();
            Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(replacedJson1, res.get("v")
                    .toString()));
        } catch (Exception e) {
            System.out.println("test Batch Insert Json fail, " + e.getMessage());
        } finally {
            client.delete(tableName).setRowKey(row1).execute();
            client.delete(tableName).setRowKey(row2).execute();
        }
    }

    /*test simple get*/
    @Test
    public void testSimpleGet() throws Exception {
        long key = 0;
        String json_str = new String(new char[1000]).replace('\0', 'A');
        System.out.println(json_str);
        Map<String, Object> res = client.get(tableName).setRowKey(row(colVal("k", key)))
                .select("k", "v").execute();
        Assert.assertTrue(res.isEmpty());
    }

    /*test batch mixed operations*/
    @Test
    public void testBatchMixedOps() throws Exception {
        String json1 = "{\"name\":\"zhangsan\",\"age\":30}";
        String json2 = "{\"name\":\"lisi\",\"age\":25}";

        Row row1 = row(colVal("k", (long) 1));
        Row row2 = row(colVal("k", (long) 2));

        Row prop1 = row().add(colVal("v", json1));
        Row prop2 = row().add(colVal("v", json2));

        // batch insert
        Insert insertOp1 = insert().setRowKey(row1).addMutateRow(prop1);
        Insert insertOp2 = insert().setRowKey(row2).addMutateRow(prop2);
        client.batchOperation(tableName).addOperation(insertOp1, insertOp2).execute();

        // batch get
        Get getOp1 = new Get().setRowKey(row1).select("v");
        Get getOp2 = new Get().setRowKey(row2).select("v");
        BatchOperationResult res = client.batchOperation(tableName).addOperation(getOp1, getOp2)
                .execute();
        Assert.assertEquals(2, res.size());
        System.out.println(res.size());
        System.out.println(res.get(0).getOperationRow().get("v"));
        System.out.println(res.get(1).getOperationRow().get("v"));

        // batch delete
        Delete delOp1 = delete().setRowKey(row1);
        Delete delOp2 = delete().setRowKey(row2);
        client.batchOperation(tableName).addOperation(delOp1, delOp2).execute();
        res = client.batchOperation(tableName).addOperation(getOp1, getOp2).execute();
        // get 2 null value
        Assert.assertNull(res.get(0).getOperationRow().get("v"));
        Assert.assertNull(res.get(1).getOperationRow().get("v"));
    }

    /* test query*/
    @Test
    public void testQuery() throws Exception {

        // insert
        long key1 = 1;
        long key2 = 2;
        client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key1)))
                .addMutateColVal(colVal("v", "{\"key\":\"value_1\"}")).execute();

        client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key2)))
                .addMutateColVal(colVal("v", "{\"key\":\"value_2\"}")).execute();

        TableQuery query = client.query(tableName);
        QueryResultSet res = query.setScanRangeColumns("k")
                .addScanRange(new Object[]{(long) 1}, new Object[]{(long) 2}).select("v")
                .execute();
        while (res.next()) {
            System.out.println(res.getRow().get("v"));
        }

        client.delete(tableName).setRowKey(row(colVal("k", key1))).execute();
        client.delete(tableName).setRowKey(row(colVal("k", key2))).execute();
    }

    @Test
    public void testInsertBigJson() throws Exception {
        //insert big json
        long key = 123;
        String big_json = JsonTestUtils.generateJsonString(100);
        System.out.println((big_json.getBytes().length));
        client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key)))
                .addMutateColVal((colVal("v", big_json))).execute();
        // get
        Map<String, Object> res = client.get(tableName).setRowKey(row(colVal("k", key)))
                .select("k", "v").execute();
        Assert.assertNotNull(res);
        Assert.assertEquals(key, res.get("k"));
        Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(big_json, res.get("v").toString()));
        client.delete(tableName).setRowKey(row(colVal("k", key))).execute();
    }

    /*test resorted json key*/
    @Test
    public void testResortedJsonKey() throws Exception {
        long key = 1;
        String val = "{\n" + "    \"zebra\": 1,\n" + "    \"apple\": 2,\n" + "    \"monkey\": 3,\n"
                + "    \"banana\": 4,\n" + "  \"name\": \"5\",\n" + "  \"a\": 6,\n"
                + "  \"age\": 7,\n" + "  \"city\": \"8\",\n" + "  \"occupation\": \"9\"" + "}";

        System.out.println(val);
        client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key)))
                .addMutateColVal((colVal("v", val))).execute();
        // get
        Map<String, Object> res = client.get(tableName).setRowKey(row(colVal("k", key)))
                .select("k", "v").execute();
        //        +---+-------------------------------------------------------------------------------------------------------------------+
        //        | k | v                                                                                                                 |
        //        +---+-------------------------------------------------------------------------------------------------------------------+
        //        | 1 | {"a": 6, "age": 7, "city": "8", "name": "5", "apple": 2, "zebra": 1, "banana": 4, "monkey": 3, "occupation": "9"} |
        //        +---+-------------------------------------------------------------------------------------------------------------------+
        Assert.assertNotNull(res);
        Assert.assertEquals(key, res.get("k"));
        Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(val, res.get("v").toString()));
        //        client.delete(tableName).setRowKey(row(colVal("k", key))).execute();
    }

    /* test update and get */
    @Test
    public void testUpdateAndGet() throws Exception {
        // insert
        long key = 1;
        String val1 = "{\"key\":\"value\"}";
        client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key)))
                .addMutateColVal(colVal("v", val1)).execute();

        Map<String, Object> res1 = client.get(tableName).setRowKey(row(colVal("k", key)))
                .select("k", "v").execute();

        Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res1.get("v").toString(), val1));
        // update
        String val2 = "{\"key\":\"new_value\"}";
        client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key)))
                .addMutateColVal(colVal("v", val2)).execute();

        // get
        Map<String, Object> res2 = client.get(tableName).setRowKey(row(colVal("k", key)))
                .select("k", "v").execute();
        Assert.assertEquals(key, res2.get("k"));
        Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res2.get("v").toString(), val2));
    }

    /* test append and increment*/
    @Test
    public void testAppendAndIncrement() throws Exception {
        // insert
        long key = 1;
        String val1 = "{\"key\":\"value\"}";
        Row rowKey = row(colVal("k", key));
        client.insertOrUpdate(extraTable).setRowKey(rowKey).addMutateColVal(colVal("v", val1))
                .addMutateColVal(colVal("id", 1)).addMutateColVal(colVal("str", "some chars"))
                .execute();

        Map<String, Object> res1 = client.get(extraTable).setRowKey(rowKey).select("k", "v")
                .execute();

        Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res1.get("v").toString(), val1));
        // increment
        Row properties = row().add(colVal("id", 1));
        client.increment(extraTable).setRowKey(rowKey).addMutateRow(properties).execute();
        res1 = client.get(extraTable).setRowKey(rowKey).select("k", "v", "id").execute();
        Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res1.get("v").toString(), val1));
        Assert.assertEquals(2, res1.get("id"));

        // append
        properties = row().add(colVal("str", " plus extra_chars"));
        client.append(extraTable).setRowKey(row(colVal("k", key))).addMutateRow(properties)
                .execute();

        res1 = client.get(extraTable).setRowKey(rowKey).select("k", "v", "str").execute();
        Assert.assertTrue(JsonTestUtils.areJsonStringsEqual(res1.get("v").toString(), val1));
        Assert.assertEquals("some chars plus extra_chars", res1.get("str"));

        client.delete(extraTable).setRowKey(rowKey);
    }

    /* test simple delete */
    @Test
    public void testSimpleDelete() throws Exception {
        // insert
        long key = 1;
        client.insertOrUpdate(tableName).setRowKey(row(colVal("k", key)))
                .addMutateColVal(colVal("v", "{\"key\":\"value\"}")).execute();

        // delete
        client.delete(tableName).setRowKey(row(colVal("k", key))).execute();

        // get
        Map<String, Object> res = client.get(tableName).setRowKey(row(colVal("k", key)))
                .select("k", "v").execute();
        Assert.assertNotNull(res);
        Assert.assertNull(res.get("k"));
        Assert.assertNull(res.get("v"));
    }

//    @Test
    public void testBigJsonInsert2() throws Exception {
        // insert
        String big_json = JsonTestUtils.Case2();
        System.out.println((big_json.getBytes().length));
        System.out.println(big_json);
        BatchOperation batch = client.batchOperation(tableName);
        for (int i = 0; i < 5000000; i++) {
            long key = i;
            Row rowkey = row(colVal("k", key));
            Row prop = row().add(colVal("v", big_json));
            Insert insertOp1 = insert().setRowKey(rowkey).addMutateRow(prop);
            batch.addOperation(insertOp1);

            if (i % 1000 == 0 && i != 0) {
                batch.execute();
                batch = client.batchOperation(tableName);
            }
//            client.insertOrUpdate(tableName).setRowKey()
//                    .addMutateColVal().execute();
        }
        batch.execute(); // 提交剩余的数据

        // delete
        for (int i = 0; i < 5000000; i++) {
            long key = i;
            Row rowkey = row(colVal("k", key));
            Delete del = delete().setRowKey(rowkey);
            client.batchOperation(tableName).addOperation(del);
//            client.delete(tableName).setRowKey(row(colVal("k", key))).execute();
        }
        client.batchOperation(tableName).execute();
    }

    @Test
    public void testReadWriteNull() throws Exception {
        // batchInsert or update
        Row rowKey0 = row().add(colVal("k", (long)0));
        String new_val0 = "{\"key_001\":\"new_value1\"}";
        Row prop0 = row().add(colVal("v", new_val0.getBytes()));
        InsertOrUpdate insertUpOp1 = insertOrUpdate().setRowKey(rowKey0).addMutateRow(prop0);
        BatchOperation batch = client.batchOperation(tableName);
        batch.addOperation(insertUpOp1).execute();
        client.delete(tableName).setRowKey(rowKey0).execute();

        Row rowKey1 = row().add(colVal("k", (long)1));
        try {
            Row prop = row().add(colVal("v",""));
            client.insertOrUpdate(tableNameNotNull).setRowKey(rowKey1)
                    .addMutateRow(prop)
                    .execute();
        } catch (ObTableException e) {
            assertEquals(ResultCodes.OB_ERR_INVALID_JSON_TEXT_IN_PARAM.errorCode, e.getErrorCode());
        } finally {
            client.delete(tableNameNotNull).setRowKey(rowKey1).execute();
        }

        // insert null on schema 'json not null'
        Row rowKey2 = row().add(colVal("k", (long)2));
        try {
            Row prop = row().add(colVal("v", null));
            client.insertOrUpdate(tableNameNotNull).setRowKey(rowKey2)
                    .addMutateRow(prop)
                    .execute();
        } catch (ObTableException e) {
            assertEquals(ResultCodes.OB_BAD_NULL_ERROR.errorCode, e.getErrorCode());
        } finally {
            client.delete(tableNameNotNull).setRowKey(rowKey2).execute();
        }

        // insert null on schema 'json'(can be null) and get
        Row prop = row().add(colVal("v", null));
        client.insertOrUpdate(tableName).setRowKey(rowKey2)
                .addMutateRow(prop)
                .execute();
        Map<String, Object> res = client.get(tableName).setRowKey(rowKey2)
                .select("k", "v").execute();
        client.delete(tableName).setRowKey(rowKey2).execute();
        Assert.assertNotNull(res);
        Assert.assertNull(res.get("v"));

    }
}
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

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.exception.ObTableDuplicateKeyException;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.table.api.TableBatchOps;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class ObAtomicBatchOperationTest {
    private static final int    dataSetSize = 4;
    private static final String successKey  = "abc-5";
    private static final String failedKey   = "abc-7";

    protected ObTableClient     obTableClient;

    @Before
    public void setup() throws Exception {

        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.setMetadataRefreshInterval(100);
        obTableClient.addProperty("connectTimeout", "100000");
        obTableClient.addProperty("socketTimeout", "100000");
        obTableClient.addProperty("table.connection.pool.size", "3");
        obTableClient.init();

        this.obTableClient = obTableClient;

        for (int i = 0; i < dataSetSize; i++) {
            String key = "abc-" + i;
            String val = "xyz-" + i;
            this.obTableClient.insert("test_varchar_table", key, new String[] { "c2" },
                new String[] { val });
        }
    }

    @After
    public void teardown() throws Exception {
        for (int i = 0; i < dataSetSize; i++) {
            String key = "abc-" + i;
            obTableClient.delete("test_varchar_table", key);
        }
        obTableClient.delete("test_varchar_table", successKey);
        obTableClient.close();
    }

    @Test
    public void testAtomic() {
        TableBatchOps batchOps = obTableClient.batch("test_varchar_table");
        // default: no atomic batch operation
        try {
            batchOps.clear();
            batchOps.insert("abc-1", new String[] { "c2" }, new String[] { "bar-1" });
            batchOps.get("abc-2", new String[] { "c2" });
            batchOps.insert("abc-3", new String[] { "c2" }, new String[] { "bar-3" });
            batchOps.insert(successKey, new String[] { "c2" }, new String[] { "bar-5" });
            List<Object> results = batchOps.execute();
            Assert.assertTrue(results.get(0) instanceof ObTableException);
            Assert.assertEquals(((Map) results.get(1)).get("c2"), "xyz-2");
            Assert.assertTrue(results.get(2) instanceof ObTableException);
            Assert.assertEquals(results.get(3), 1L);
        } catch (Exception ex) {
            Assert.fail("hit exception:" + ex);
        }

        // atomic batch operation
        try {
            batchOps.clear();
            batchOps.setAtomicOperation(true);
            batchOps.insert("abc-1", new String[] { "c2" }, new String[] { "bar-1" });
            batchOps.get("abc-2", new String[] { "c2" });
            batchOps.insert("abc-3", new String[] { "c2" }, new String[] { "bar-3" });
            batchOps.insert(failedKey, new String[] { "c2" }, new String[] { "bar-5" });
            batchOps.execute();
            // no support atomic batch
            // Assert.fail("expect duplicate key exception.");
        } catch (Exception ex) {
            Assert.assertTrue(ex instanceof ObTableException);
        }
    }

}

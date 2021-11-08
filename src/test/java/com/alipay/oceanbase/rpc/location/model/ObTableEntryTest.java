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

package com.alipay.oceanbase.rpc.location.model;

import com.alipay.oceanbase.rpc.constant.Constants;
import org.junit.Assert;
import org.junit.Test;

public class ObTableEntryTest {

    @Test
    public void testTableEntryKey() {
        TableEntryKey entryKey = new TableEntryKey();
        Assert.assertEquals(Constants.EMPTY_STRING, entryKey.getTableName());

        entryKey = TableEntryKey.getDummyEntryKey("testCluster", "testTenant");
        Assert.assertEquals(Constants.ALL_DUMMY_TABLE, entryKey.getTableName());
        Assert.assertFalse(entryKey.isSysAllDummy());
        Assert.assertTrue(entryKey.isAllDummy());
        Assert.assertTrue(entryKey.isValid());

        entryKey = TableEntryKey.getSysDummyEntryKey("testCluster");
        Assert.assertEquals(Constants.OCEANBASE_DATABASE, entryKey.getDatabaseName());
        Assert.assertTrue(entryKey.isSysAllDummy());
        Assert.assertTrue(entryKey.isAllDummy());
        Assert.assertTrue(entryKey.isValid());

        entryKey = new TableEntryKey();
        entryKey.setClusterName("testCluster");
        entryKey.setDatabaseName("testDB");
        entryKey.setTableName("testTable");
        entryKey.setTenantName("testTenant");
        Assert.assertEquals("testCluster", entryKey.getClusterName());
        Assert.assertEquals("testDB", entryKey.getDatabaseName());
        Assert.assertEquals("testTable", entryKey.getTableName());
        Assert.assertEquals("testTenant", entryKey.getTenantName());
        Assert.assertTrue(entryKey.toString().contains("testCluster"));
        Assert.assertNotEquals(0, entryKey.hashCode());

        TableEntryKey same = new TableEntryKey("testCluster", "testTenant", "testDB", "testTable");

        TableEntryKey other = new TableEntryKey("testCluster", "testTenant2", "testDB", "testTable");

        Assert.assertTrue(entryKey.equals(same));
        Assert.assertFalse(entryKey.equals(other));

    }

}

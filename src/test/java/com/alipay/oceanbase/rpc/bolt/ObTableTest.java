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

package com.alipay.oceanbase.rpc.bolt;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableClientType;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

public class ObTableTest extends ObTableClientTestBase {
    private ObTable obTable;

    @BeforeClass
    static public void beforeTest() throws Exception {
        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();

        assertFalse(obTableClient.isOdpMode());
    }

    @Before
    public void setup() throws Exception {
        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();

        if (obTableClient.isOdpMode()) {
            obTableClient.close();
            throw new ObTableException("ODP Mode does not support this test");
        } else {
            obTable = obTableClient.getTableParam("test_varchar_table", new Object[] { "abc" })
                .getObTable();
            client = obTable;
        }
    }

    @Test
    public void test_login() throws Exception {
        ObTableException obTableException = null;
        try {
            new ObTable.Builder(obTable.getIp(), obTable.getPort()) //
                .setLoginInfo(obTable.getTenantName(), obTable.getUserName(), "11", "test",
                    ObTableClientType.JAVA_TABLE_CLIENT) //
                .build();
        } catch (ObTableException ex) {
            obTableException = ex;
        }

        assertNotNull(obTableException);
        assertTrue(obTableException.getMessage().contains("login failed"));
        assertTrue(obTableException.getCause().getMessage().contains("OB_PASSWORD_WRONG"));

        obTableException = null;
        try {
            new ObTable.Builder(obTable.getIp(), obTable.getPort()) //
                .setLoginInfo(obTable.getTenantName(), "root1", "11", "test",
                    ObTableClientType.JAVA_TABLE_CLIENT) //
                .build();
            obTable.get("test_varchar_table", "1", new String[] { "c2" });
        } catch (ObTableException ex) {
            obTableException = ex;
        }

        assertNotNull(obTableException);
        assertTrue(obTableException.getMessage().contains("login failed"));
        assertTrue(obTableException.getCause().getMessage().contains("OB_ERR_USER_NOT_EXIST"));
    }

}

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

package com.alipay.oceanbase.rpc.hbase;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;

public class ObHTableTest {
    private ObTable client;

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
            client = obTableClient.getTable("test_varchar_table", new Object[] { "abc" }, true,
                true).getRight();
        }
    }

    @Test
    public void iud() throws Exception {
        /*
        CREATE TABLE `test_hbase$fn` (
                      `K` varbinary(1024) NOT NULL,
                      `Q` varbinary(256) NOT NULL,
                      `T` bigint(20) NOT NULL,
                      `V` varbinary(1024) DEFAULT NULL,
                      PRIMARY KEY (`K`, `Q`, `T`)
                )
         */
        ObHTableOperationRequest hTableOperationRequest = new ObHTableOperationRequest();
        hTableOperationRequest.setOperationType(ObTableOperationType.INSERT);
        hTableOperationRequest.setTableName("test_hbase");
        hTableOperationRequest.setFamilyName("fn");
        hTableOperationRequest.setRowKey("key".getBytes());
        hTableOperationRequest.setQualifierName("qualifierName2".getBytes());
        hTableOperationRequest.setValue("value".getBytes());

        Object obj = client.getRealClient().invokeSync(client.getConnection(),
            hTableOperationRequest.obTableOperationRequest(), 1000000);
        System.err.println(obj);

        hTableOperationRequest = new ObHTableOperationRequest();
        hTableOperationRequest.setOperationType(ObTableOperationType.GET);
        hTableOperationRequest.setTableName("test_hbase");
        hTableOperationRequest.setFamilyName("fn");
        hTableOperationRequest.setRowKey("key".getBytes());
        hTableOperationRequest.setQualifierName("qualifierName".getBytes());
        hTableOperationRequest.setValue("value".getBytes());
        client.getRealClient().invokeSync(client.getConnection(),
            hTableOperationRequest.obTableOperationRequest(), 1000000);

        client.delete("test_hbase$fn", new Object[] { "key".getBytes(),
                "qualifierName1".getBytes(), 12323121L });
    }

}

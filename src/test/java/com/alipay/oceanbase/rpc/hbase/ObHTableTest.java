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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertFalse;

public class ObHTableTest {
    private ObTable       client;
    private ObTableClient obTableClient;

    @BeforeClass
    static public void beforeTest() throws Exception {
        ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        obTableClient.setRunningMode(ObTableClient.RunningMode.HBASE);

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
            client = obTableClient.getTableParam("test_varchar_table", new Object[] { "abc" })
                .getObTable();
            this.obTableClient = obTableClient;
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

        client.delete("test_hbase$fn", new Object[] { "key".getBytes(),
                "qualifierName1".getBytes(), 12323121L });
    }

    public byte[][] extractFamilyFromQualifier(byte[] qualifier) throws Exception {
        int total_length = qualifier.length;
        int familyLen = -1;
        byte[][] familyAndQualifier = new byte[2][];

        for (int i = 0; i < total_length; i++) {
            if (qualifier[i] == '\0') {
                familyLen = i;
                break;
            }
        }

        byte[] family = new byte[familyLen];
        if (familyLen != -1) {
            for (int i = 0; i < familyLen; i++) {
                family[i] = qualifier[i];
            }
        } else {
            throw new RuntimeException("can not get family name");
        }
        familyAndQualifier[0] = family;
        int qualifierLen = total_length - familyLen - 1;
        byte[] newQualifier = new byte[qualifierLen];
        if (qualifierLen > 0) {
            for (int i = 0; i < qualifierLen; i++) {
                newQualifier[i] = qualifier[i + familyLen + 1];
            }
        } else {
            throw new RuntimeException("can not get qualifier name");
        }
        for (int i = 0; i < qualifierLen; i++) {
            newQualifier[i] = qualifier[i + familyLen + 1];
        }
        familyAndQualifier[1] = newQualifier;
        System.out.println(newQualifier);
        System.out.println(family);
        return familyAndQualifier;
    }

    public void getKeyValueFromResult(ObTableClientQueryStreamResult clientQueryStreamResult,
                                      boolean isTableGroup, byte[] family) throws Exception {
        for (List<ObObj> row : clientQueryStreamResult.getCacheRows()) {
            System.out.println(new String((byte[]) row.get(0).getValue())); //K
            // System.out.println(family); //family
            if (isTableGroup) {
                byte[][] familyAndQualifier = extractFamilyFromQualifier((byte[]) row.get(1)
                    .getValue());
                System.out.println(new String(familyAndQualifier[0])); //family
                System.out.println(new String(familyAndQualifier[1])); //qualifier
            } else {
                System.out.println(family); //family
                System.out.println(new String((byte[]) row.get(1).getValue())); //qualifier
            }

            System.out.println((Long) row.get(2).getValue());//T
            System.out.println(new String((byte[]) row.get(3).getValue()));//V
        }
    }

    @Test
    public void hbaseTableGroupTest() throws Exception {
        /*
        CREATE TABLEGROUP test SHARDING = 'ADAPTIVE';
        CREATE TABLE `test$family1` (
                      `K` varbinary(1024) NOT NULL,
                      `Q` varbinary(256) NOT NULL,
                      `T` bigint(20) NOT NULL,
                      `V` varbinary(1024) DEFAULT NULL,
                      PRIMARY KEY (`K`, `Q`, `T`)
                ) TABLEGROUP = test;
         */
        byte[] family = new byte[] {};
        ObHTableOperationRequest hTableOperationRequestGet = new ObHTableOperationRequest();
        hTableOperationRequestGet.setOperationType(ObTableOperationType.GET);
        hTableOperationRequestGet.setTableName("test");
        hTableOperationRequestGet.setRowKey("putKey".getBytes());

        ObTableQueryRequest requestGet = (ObTableQueryRequest) hTableOperationRequestGet
            .obTableGroupOperationRequest();
        ObTableClientQueryStreamResult clientQueryStreamResultGet = (ObTableClientQueryStreamResult) obTableClient
            .execute(requestGet);

        // Thread.currentThread().sleep(30000);
        ObHTableOperationRequest hTableOperationRequestScan = new ObHTableOperationRequest();
        hTableOperationRequestScan.setOperationType(ObTableOperationType.SCAN);
        hTableOperationRequestScan.setTableName("test");
        hTableOperationRequestScan.setRowKey("putKey".getBytes());

        ObTableQueryRequest requestScan = (ObTableQueryRequest) hTableOperationRequestScan
            .obTableGroupOperationRequest();
        ObTableClientQueryStreamResult clientQueryStreamResultScan = (ObTableClientQueryStreamResult) obTableClient
            .execute(requestScan);

    }

    @Test
    public void hbaseDiffTableGroupTest() throws Exception {
        /*
        CREATE TABLEGROUP test SHARDING = 'ADAPTIVE';
        CREATE TABLE `test$family1` (
                      `K` varbinary(1024) NOT NULL,
                      `Q` varbinary(256) NOT NULL,
                      `T` bigint(20) NOT NULL,
                      `V` varbinary(1024) DEFAULT NULL,
                      PRIMARY KEY (`K`, `Q`, `T`)
                ) TABLEGROUP = test;
        CREATE TABLEGROUP test2 SHARDING = 'ADAPTIVE';
        CREATE TABLE `test2$family1` (
                      `K` varbinary(1024) NOT NULL,
                      `Q` varbinary(256) NOT NULL,
                      `T` bigint(20) NOT NULL,
                      `V` varbinary(1024) DEFAULT NULL,
                      PRIMARY KEY (`K`, `Q`, `T`)
                ) TABLEGROUP = test2;
         */
        byte[] family = new byte[] {};
        ObHTableOperationRequest hTableOperationRequestGet = new ObHTableOperationRequest();
        hTableOperationRequestGet.setOperationType(ObTableOperationType.GET);
        hTableOperationRequestGet.setTableName("test");
        hTableOperationRequestGet.setRowKey("putKey".getBytes());

        ObTableQueryRequest requestGet = (ObTableQueryRequest) hTableOperationRequestGet
            .obTableGroupOperationRequest();
        ObTableClientQueryStreamResult clientQueryStreamResultGet = (ObTableClientQueryStreamResult) obTableClient
            .execute(requestGet);

        // Thread.currentThread().sleep(30000);
        ObHTableOperationRequest hTableOperationRequestScan = new ObHTableOperationRequest();
        hTableOperationRequestScan.setOperationType(ObTableOperationType.SCAN);
        hTableOperationRequestScan.setTableName("test2");
        hTableOperationRequestScan.setRowKey("putKey".getBytes());

        ObTableQueryRequest requestScan = (ObTableQueryRequest) hTableOperationRequestScan
            .obTableGroupOperationRequest();
        ObTableClientQueryStreamResult clientQueryStreamResultScan = (ObTableClientQueryStreamResult) obTableClient
            .execute(requestScan);
    }

}

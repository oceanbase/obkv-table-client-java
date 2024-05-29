/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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
import com.alipay.oceanbase.rpc.filter.ObCompareOp;
import com.alipay.oceanbase.rpc.filter.ObTableValueFilter;
import com.alipay.oceanbase.rpc.mutation.Append;
import com.alipay.oceanbase.rpc.mutation.BatchOperation;
import com.alipay.oceanbase.rpc.mutation.Insert;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateRequest;
import com.alipay.oceanbase.rpc.table.ObDirectLoadParameter;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableDirectLoad;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.JDBC_IP;
import static com.alipay.oceanbase.rpc.util.ObTableClientTestUtil.JDBC_PORT;
import static org.junit.Assert.*;

/*
CREATE TABLE IF NOT EXISTS `test_varchar_table` (
    `c1` varchar(20) NOT NULL,
    `c2` varchar(20) DEFAULT NULL,
    PRIMARY KEY (`c1`));
*/
public class ObTableFeatureCtrlTest {
    ObTableClient        client;
    public static String tableName = "test_varchar_table";
    private static String              tenantName           = "mysql";
    private static String              userName             = "root";
    private static String              password             = "";
    private static String              dbName               = "oceanbase";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { "c1" });
        disableKvFeature();
    }

    @After
    public void teardown() throws Exception {
        enableKvFeature();
    }

    private static void disableKvFeature() throws Exception {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        Connection connection = ObTableClientTestUtil.getSysConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter system set _enable_kv_feature=false;");
    }

    private static void enableKvFeature() throws Exception {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        Connection connection = ObTableClientTestUtil.getSysConnection();
        Statement statement = connection.createStatement();
        statement.execute("alter system set _enable_kv_feature=true;");
    }

    @Test
    public void testSingleOperation1() {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.insert(tableName).setRowKey(colVal("c1", "1"))
                            .addMutateColVal(colVal("c2", "hello"))
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][OBKV feature not supported]"));
    }

    @Test
    public void testSingleOperation2() throws Exception {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        try {
            enableKvFeature();
            client.insertOrUpdate(tableName).setRowKey(colVal("c1", "1"))
                    .addMutateColVal(colVal("c2", "hello"))
                    .execute();
        } finally {
            disableKvFeature();
        }
    }

    @Test
    public void testBatchOperation() {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    BatchOperation batchOps = client.batchOperation(tableName);
                    Insert ins1 = client.insert(tableName).setRowKey(colVal("c1", "1"))
                            .addMutateColVal(colVal("c2", "hello"));
                    Insert ins2 = client.insert(tableName).setRowKey(colVal("c1", "2"))
                            .addMutateColVal(colVal("c2", "hello"));
                    batchOps.addOperation(ins1, ins2).execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][OBKV feature not supported]"));
    }

    @Test
    public void testSyncQuery() {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    TableQuery tableQuery = client.query(tableName);
                    tableQuery.addScanRange(new Object[] { "1" }, new Object[] { "2" });
                    tableQuery.setScanRangeColumns("c1");
                    tableQuery.select("c1");
                    tableQuery.execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][OBKV feature not supported]"));
    }

    @Test
    public void testAsyncQuery() {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    TableQuery tableQuery = client.query(tableName);
                    tableQuery.addScanRange(new Object[] { "1" }, new Object[] { "2" });
                    tableQuery.setScanRangeColumns("c1");
                    tableQuery.select("c1");
                    tableQuery.asyncExecute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][OBKV feature not supported]"));
    }

    @Test
    public void testQueryAndMutate() {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    TableQuery tableQuery = client.query(tableName);
                    tableQuery.addScanRange(new Object[] { 0L }, new Object[] { 200L });
                    tableQuery.setScanRangeColumns("c1");
                    tableQuery.select("c1");
                    ObTableQueryAndMutateRequest req = client.obTableQueryAndAppend(tableQuery,
                            new String[] { "c2"}, new Object[] {"_append0" }, false);
                    client.execute(req);
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][OBKV feature not supported]"));
    }

    @Test
    public void testDirectLoad() {
        if (ObTableClientTestUtil.isOBVersionLessThan(ObTableClientTestUtil.obVsn4311)) {
            return;
        }
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    ObTable table = new ObTable.Builder(JDBC_IP, 41104).setLoginInfo(tenantName, userName, password, dbName).build();
                    ObDirectLoadParameter parameter = new ObDirectLoadParameter();
                    ObTableDirectLoad directLoad = new ObTableDirectLoad(table, tableName, parameter,true);
                    directLoad.begin();
                }
        );
        System.out.println(thrown.getMessage());
        assertFalse(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][OBKV feature not supported]"));
    }
}

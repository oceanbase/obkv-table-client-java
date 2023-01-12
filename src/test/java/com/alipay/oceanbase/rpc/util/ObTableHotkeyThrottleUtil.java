/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2022 OceanBase
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

package com.alipay.oceanbase.rpc.util;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.BatchOperationResult;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.property.Property;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.*;

public class ObTableHotkeyThrottleUtil extends Thread {
    int testNum = 100;

    public enum TestType {
        random, specifiedKey
    }

    public enum OperationType {
        insert, update, insertOrUpdate, query, queryAndMutate, batchOperation
    }

    TestType      testType;
    OperationType operationType;
    public Table  client      = null;
    Row           rowKey;
    int           throttleNum = 0;
    int           passNum     = 0;
    int           batchSize   = 64;

    public void init(TestType testType, OperationType operationType, Table client, int batchSize,
                     ColumnValue... rowKeyColumnValues) throws Exception {
        System.setProperty("ob_table_min_rslist_refresh_interval_millis", "1");

        switch (testType) {
            case random: {
                rowKey = null;
                this.testType = testType;
                break;
            }
            case specifiedKey: {
                if (rowKeyColumnValues != null) {
                    rowKey = row(rowKeyColumnValues);
                    this.testType = testType;
                } else {
                    throw new IllegalArgumentException("invalid row key pass into init");
                }
                break;
            }
            default:
                throw new IllegalArgumentException("invalid test type pass into init");
        }

        this.operationType = operationType;
        this.batchSize = batchSize;

        if (null == client) {
            final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
            obTableClient.setMetadataRefreshInterval(100);
            obTableClient.addProperty(Property.RPC_CONNECT_TIMEOUT.getKey(), "800");
            obTableClient.addProperty(Property.RPC_LOGIN_TIMEOUT.getKey(), "800");
            obTableClient.addProperty(Property.SERVER_CONNECTION_POOL_SIZE.getKey(), "1");
            obTableClient.addProperty(Property.RPC_EXECUTE_TIMEOUT.getKey(), "2000");
            obTableClient.init();

            this.client = obTableClient;
            syncRefreshMetaHelper(obTableClient);
        } else {
            this.client = client;
        }
        ((ObTableClient) this.client)
            .addRowKeyElement("test_throttle", new String[] { "c1", "c2" });
    }

    @Override
    public void run() {
        try {
            switch (testType) {
                case random:
                    runRandom();
                    break;
                case specifiedKey:
                    runSpecifiedKey();
                    break;
                default:
                    System.out.println(Thread.currentThread().getName()
                                       + " has no test type to run");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void runRandom() throws Exception {
        System.out.println(Thread.currentThread().getName() + " begin to run random test");
        for (int i = 0; i < testNum; ++i) {
            long randomNum = (long) (Math.random() * 2000000);
            String rowKeyString = "Test" + randomNum;
            Row rowKey = row(colVal("c1", randomNum), colVal("c2", rowKeyString));
            switch (operationType) {
                case insert:
                    insertTest(rowKey, colVal("c3", new byte[] { 1 }), colVal("c4", 0L));
                    break;
                case update:
                    updateTest(rowKey, colVal("c3", new byte[] { 1 }), colVal("c4", 0L));
                    break;
                case insertOrUpdate:
                    insertOrUpdateTest(rowKey, colVal("c3", new byte[] { 1 }), colVal("c4", 0L));
                    break;
                case query:
                    queryTest(rowKey);
                    break;
                case queryAndMutate:
                    queryAndMutateTest(rowKey, colVal("c3", new byte[] { 1 }), colVal("c4", 0L));
                    break;
                case batchOperation:
                    batchOperationTest();
                    break;
            }
        }
    }

    private void runSpecifiedKey() throws Exception {
        System.out.println(Thread.currentThread().getName() + " begin to run specified key test");
        for (int i = 0; i < testNum; ++i) {
            switch (operationType) {
                case insert:
                    insertTest(this.rowKey, colVal("c3", new byte[] { 1 }), colVal("c4", 0L));
                    break;
                case update:
                    updateTest(this.rowKey, colVal("c3", new byte[] { 1 }), colVal("c4", 0L));
                    break;
                case insertOrUpdate:
                    insertOrUpdateTest(this.rowKey, colVal("c3", new byte[] { 1 }),
                        colVal("c4", 0L));
                    break;
                case query:
                    queryTest(this.rowKey);
                    break;
                case queryAndMutate:
                    queryAndMutateTest(this.rowKey, colVal("c3", new byte[] { 1 }),
                        colVal("c4", 0L));
                    break;
            }
        }
    }

    private void insertTest(Row rowkey, ColumnValue... columnValues) throws Exception {
        MutationResult insertResult = client.insert("test_throttle").setRowKey(rowkey)
            .addMutateColVal(columnValues).execute();

    }

    private void updateTest(Row rowkey, ColumnValue... columnValues) throws Exception {
        MutationResult updateResult = client.update("test_throttle").setRowKey(rowKey)
            .addMutateColVal(columnValues).execute();
    }

    private void insertOrUpdateTest(Row rowkey, ColumnValue... columnValues) throws Exception {
        MutationResult insertOrUpdateResult = client.insertOrUpdate("test_throttle")
            .setRowKey(rowkey).addMutateColVal(columnValues).execute();
    }

    private void queryTest(Row rowkey) throws Exception {
        try {
            TableQuery tableQuery = client.query("test_throttle");
            tableQuery.addScanRange(rowkey.getValues(), rowkey.getValues());
            tableQuery.select("c1", "c2", "c3", "c4");
            QueryResultSet result_ = tableQuery.execute();
            ++passNum;
        } catch (Exception e) {
            if (e instanceof ObTableUnexpectedException) {
                if (((ObTableUnexpectedException) e).getErrorCode() == -4039) {
                    if (++throttleNum % 50 == 0) {
                        System.out.println(Thread.currentThread().getName() + " rowkey num is "
                                           + rowkey.get("c1") + " has pass " + passNum
                                           + " operations, and has throttle " + throttleNum
                                           + " operations");
                    }
                } else {
                    e.printStackTrace();
                    Assert.assertNull(e);
                }
            } else {
                e.printStackTrace();
                Assert.assertNull(e);
            }
        }
    }

    private void queryAndMutateTest(Row rowkey, ColumnValue... columnValues) throws Exception {
        MutationResult updateResult = client.update("test_throttle").setRowKey(rowkey)
            .addMutateColVal(columnValues).execute();
    }

    private List<Mutation> generateBatchOpertaionIoU() {
        List<Mutation> rowList = new ArrayList<>();
        for (int i = 0; i < batchSize; ++i) {
            long randomNum = (long) (Math.random() * 2000000);
            String rowKeyString = "Test" + randomNum;
            rowList.add(insertOrUpdate().setRowKey(colVal("c1", randomNum), colVal("c2", rowKeyString))
                    .addMutateColVal(colVal("c3", new byte[] { 1 }))
                    .addMutateColVal(colVal("c4", randomNum)));
        }
        return rowList;
    }

    private void batchOperationTest() throws Exception {
        try {
            BatchOperationResult batchResult = client.batchOperation("test_throttle")
                .addOperation(generateBatchOpertaionIoU()).execute();
            ++passNum;
        } catch (Exception e) {
            if (e instanceof ObTableUnexpectedException) {
                if (((ObTableUnexpectedException) e).getErrorCode() == -4039) {
                    if (++throttleNum % 50 == 0) {
                        System.out.println(Thread.currentThread().getName() + " has pass "
                                           + passNum + " batch operations, and has throttle "
                                           + throttleNum + " batch operations");
                    }
                } else {
                    e.printStackTrace();
                    Assert.assertNull(e);
                }
            } else {
                e.printStackTrace();
                Assert.assertNull(e);
            }
        }
    }

    public void syncRefreshMetaHelper(final ObTableClient obTableClient) {
        if (obTableClient.isOdpMode()) {
            // do noting
        } else {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for (int i = 0; i < 10; i++) {
                        try {
                            obTableClient.syncRefreshMetadata();
                        } catch (Exception e) {
                            e.printStackTrace();
                            Assert.fail();
                        }
                    }
                }
            }).start();
        }
    }
}

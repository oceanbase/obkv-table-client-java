/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

import com.alipay.oceanbase.rpc.mutation.*;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.api.TableQuery;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.row;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// NOTE: this test may not pass when sql audit eliminate frequently, increase your
//       observer's memory or sql_audit_memory_limit to alleviate the problem
public class ObTableSQLAuditTest {
    class AuditRows {
        int affected_rows = -1;
        int return_rows   = -1;
    }

    public ObTableClient client;

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
    }

    @BeforeClass
    public static void testVersion() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        if (ObGlobal.obVsnMajor() <= 0) {
            // ob version is invalid
            Assert.assertTrue(false);
        } else if (ObGlobal.obVsnMajor() != 3) {
            // todo: only support in 3.x currently
            Assert.assertTrue(false);
        }
    }

    // get affected_rows and return rows from sql_audit
    AuditRows fetch_audit_rows() throws Exception {
        AuditRows auditRows = new AuditRows();
        Connection connection = ObTableClientTestUtil.getConnection();
        Statement statement = connection.createStatement();
        statement
            .execute("select affected_rows, return_rows from oceanbase.gv$sql_audit where query_sql like 'table api%' order by request_time desc limit 1");
        ResultSet resultSet = statement.getResultSet();
        int rowCnt = 0;
        while (resultSet.next()) {
            auditRows.affected_rows = resultSet.getInt(1);
            auditRows.return_rows = resultSet.getInt(2);
            rowCnt++;
        }
        assertEquals(1, rowCnt);
        assertEquals(false, resultSet.next());
        return auditRows;
    }

    @Test
    public void testQuery() throws Exception {
        String tableName = "test_varchar_table";
        String startKey = "k1";
        String endKey = "k2";
        try {
            TableQuery query = client.query(tableName).addScanRange(startKey, endKey);
            QueryResultSet res = query.execute();
            Assert.assertEquals(0, res.cacheSize());
            AuditRows auditRows = fetch_audit_rows();
            assertEquals(0, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            MutationResult mutateRes = client.insertOrUpdate(tableName)
                .setRowKey(colVal("c1", startKey)).addMutateColVal(colVal("c2", "v1")).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            mutateRes = client.insertOrUpdate(tableName).setRowKey(colVal("c1", endKey))
                .addMutateColVal(colVal("c2", "v1")).execute();
            assertEquals(1, mutateRes.getAffectedRows());

            query = client.query(tableName).addScanRange(startKey, endKey);
            res = query.execute();
            Assert.assertEquals(2, res.cacheSize());
            auditRows = fetch_audit_rows();
            assertEquals(0, auditRows.affected_rows);
            assertEquals(2, auditRows.return_rows);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        } finally {
            client.delete(tableName).setRowKey(colVal("c1", startKey)).execute();
            client.delete(tableName).setRowKey(colVal("c1", endKey)).execute();
        }

    }

    @Test
    public void test_single_operation() throws Exception {

        String tableName = "test_varchar_table";
        ColumnValue rowKey = colVal("c1", "k1");
        try {
            // 1. get with empty result
            Map getRes = client.get(tableName, "not_exist_key", null);
            Assert.assertTrue(getRes.isEmpty());
            AuditRows auditRows = fetch_audit_rows();
            assertEquals(0, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            // 2. insert new record
            MutationResult mutateRes = client.insert(tableName).setRowKey(rowKey)
                .addMutateColVal(colVal("c2", "v1")).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            auditRows = fetch_audit_rows();
            assertEquals(1, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            // 3. insertOrUpdate
            mutateRes = client.insertOrUpdate(tableName).setRowKey(rowKey)
                .addMutateColVal(colVal("c2", "v2")).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            auditRows = fetch_audit_rows();
            assertEquals(1, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            // 4. update
            mutateRes = client.update(tableName).setRowKey(rowKey)
                .addMutateColVal(colVal("c2", "v3")).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            auditRows = fetch_audit_rows();
            assertEquals(1, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            // 5. replace
            mutateRes = client.replace(tableName).setRowKey(rowKey)
                .addMutateColVal(colVal("c2", "v4")).execute();
            assertEquals(2, mutateRes.getAffectedRows());
            auditRows = fetch_audit_rows();
            assertEquals(2, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            // 6. append
            mutateRes = client.append(tableName).setRowKey(rowKey)
                .addMutateColVal(colVal("c2", "v4")).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            auditRows = fetch_audit_rows();
            assertEquals(1, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            // 7. delete
            mutateRes = client.delete(tableName).setRowKey(rowKey).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            auditRows = fetch_audit_rows();
            assertEquals(1, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            // 8. replace
            mutateRes = client.replace(tableName).setRowKey(rowKey)
                .addMutateColVal(colVal("c2", "v4")).execute();
            assertEquals(1, mutateRes.getAffectedRows());
            auditRows = fetch_audit_rows();
            assertEquals(1, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        } finally {
            client.delete(tableName).setRowKey(rowKey).execute();
        }

    }

    @Test
    public void testBatchOperation() throws Exception {
        String tableName = "test_varchar_table";
        ColumnValue insUpKey = colVal("c1", "k1");
        ColumnValue deleteKey = colVal("c1", "k2");
        ColumnValue updateKey = colVal("c1", "k3");
        ColumnValue appendKey = colVal("c1", "k4");
        ColumnValue getKey = colVal("c2", "k5");
        ColumnValue updateVal = colVal("c2", "v1");
        try {
            BatchOperation batchOperation = client.batchOperation(tableName);
            InsertOrUpdate insertUp = client.insertOrUpdate(tableName).setRowKey(insUpKey)
                .addMutateColVal(updateVal);
            Delete delete = client.delete(tableName).setRowKey(deleteKey);
            Update update = client.update(tableName).setRowKey(updateKey)
                .addMutateColVal(updateVal);
            Append append = client.append(tableName).setRowKey(appendKey)
                .addMutateColVal(updateVal);
            ;
            TableQuery query = client.query(tableName).setRowKey(row(getKey));
            batchOperation.addOperation(insertUp, delete, update, append).addOperation(query);
            batchOperation.execute();

            AuditRows auditRows = fetch_audit_rows();
            assertEquals(2, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            client.insertOrUpdate(tableName).setRowKey(getKey).addMutateColVal(updateVal).execute();
            batchOperation.execute();

            auditRows = fetch_audit_rows();
            assertEquals(2, auditRows.affected_rows);
            assertEquals(1, auditRows.return_rows);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        } finally {
            client.delete(tableName).setRowKey(insUpKey).execute();
            client.delete(tableName).setRowKey(deleteKey).execute();
            client.delete(tableName).setRowKey(updateKey).execute();
            client.delete(tableName).setRowKey(appendKey).execute();
            client.delete(tableName).setRowKey(getKey).execute();
        }
    }

    @Test
    public void testQueryAndMutate() throws Exception {
        String tableName = "test_varchar_table";
        String startKey = "k1";
        String endKey = "k2";
        ColumnValue updateVal = colVal("c2", "v2");
        try {
            client.append(tableName).addScanRange(startKey, endKey).addMutateColVal(updateVal)
                .execute();
            AuditRows auditRows = fetch_audit_rows();
            assertEquals(0, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);

            client.insert(tableName).setRowKey(colVal("c1", startKey)).addMutateColVal(updateVal)
                .execute();
            client.insert(tableName).setRowKey(colVal("c2", endKey)).addMutateColVal(updateVal)
                .execute();

            client.update(tableName).addScanRange(startKey, endKey).addMutateColVal(updateVal)
                .execute();
            auditRows = fetch_audit_rows();
            assertEquals(2, auditRows.affected_rows);
            // TODO: ODP do not serialize return_affected_entity_ in ObTableQueryAndMutate
            //       which causes return_affected_entity_ always be true
            //       This bug will be fixed soon by ODP guys
            if (client.isOdpMode()) {
                assertEquals(2, auditRows.return_rows);
            } else {
                assertEquals(0, auditRows.return_rows);
            }

            client.delete(tableName).addScanRange(startKey, endKey).execute();
            auditRows = fetch_audit_rows();
            assertEquals(2, auditRows.affected_rows);
            // TODO: ODP do not serialize return_affected_entity_ in ObTableQueryAndMutate
            //       which causes return_affected_entity_ always be true
            //       This bug will be fixed soon by ODP guys
            if (client.isOdpMode()) {
                assertEquals(2, auditRows.return_rows);
            } else {
                assertEquals(0, auditRows.return_rows);
            }

            client.update(tableName).addScanRange(startKey, endKey).execute();
            auditRows = fetch_audit_rows();
            assertEquals(0, auditRows.affected_rows);
            assertEquals(0, auditRows.return_rows);
        } catch (Exception e) {
            e.printStackTrace();
            assertTrue(false);
        } finally {
            client.delete(tableName).addScanRange(startKey, endKey).execute();
        }
    }
}

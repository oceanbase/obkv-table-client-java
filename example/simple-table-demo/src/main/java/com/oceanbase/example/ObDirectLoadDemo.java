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

package com.oceanbase.example;

import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadConnection;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadManager;
import com.alipay.oceanbase.rpc.direct_load.ObDirectLoadStatement;
import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;
import com.alipay.oceanbase.rpc.direct_load.execution.ObDirectLoadStatementExecutionId;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObLoadDupActionType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class ObDirectLoadDemo {

    private static String              host       = "0.0.0.0";
    private static int                 port       = 0;
    private static int                 sqlPort    = 0;

    private static String              tenantName = "mysql";
    private static String              userName   = "root";
    private static String              password   = "";
    private static String              dbName     = "test";
    private static String              tableName  = "test1";

    // parameters of direct load
    private static int                 parallel   = 2;                          // Determines the number of server worker threads
    private static ObLoadDupActionType dupAction  = ObLoadDupActionType.REPLACE;
    private static long                timeout    = 1000L * 1000 * 1000;        // The overall timeout of the direct load task

    public static void main(String[] args) {
        SimpleTest.run();
        ParallelWriteTest.run();
        MultiNodeWriteTest.run();
    }

    private static void prepareTestTable() throws Exception {
        String url = String
            .format(
                "jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=utf-8&connectTimeout=%d&socketTimeout=%d",
                host + ":" + sqlPort, dbName, 10000, 10000);
        String user = String.format("%s@%s", userName, tenantName);
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection(url, user, password);
        Statement statement = connection.createStatement();
        String dropSql = "drop table " + tableName + ";";
        String tableDefinition = "create table " + tableName + " (c1 int, c2 varchar(255))";
        try {
            statement.execute(dropSql);
        } catch (Exception e) {
            // ignore drop error
        }
        statement.execute(tableDefinition);
        statement.close();
        connection.close();
    }

    private static void queryTestTable(int expectedRowCount) throws Exception {
        String url = String
            .format(
                "jdbc:mysql://%s/%s?useUnicode=true&characterEncoding=utf-8&connectTimeout=%d&socketTimeout=%d",
                host + ":" + sqlPort, dbName, 10000, 10000);
        String user = String.format("%s@%s", userName, tenantName);
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = DriverManager.getConnection(url, user, password);
        Statement statement = connection.createStatement();
        String querySql = "select count(*) from " + tableName + ";";
        ResultSet resultSet = statement.executeQuery(querySql);
        while (resultSet.next()) {
            int count = resultSet.getInt(1);
            if (count != expectedRowCount) {
                throw new RuntimeException("unexpected row count:" + count + ", expected:"
                                           + expectedRowCount);
            }
        }
        statement.close();
        connection.close();
    }

    private static ObDirectLoadConnection buildConnection(int writeThreadNum)
                                                                             throws ObDirectLoadException {
        return ObDirectLoadManager.getConnectionBuilder().setServerInfo(host, port)
            .setLoginInfo(tenantName, userName, password, dbName)
            .enableParallelWrite(writeThreadNum).build();
    }

    private static ObDirectLoadStatement buildStatement(ObDirectLoadConnection connection)
                                                                                          throws ObDirectLoadException {
        return connection.getStatementBuilder().setTableName(tableName).setDupAction(dupAction)
            .setParallel(parallel).setQueryTimeout(timeout).build();
    }

    private static class SimpleTest {

        public static void run() {
            System.out.println("SimpleTest start");
            ObDirectLoadConnection connection = null;
            ObDirectLoadStatement statement = null;
            try {
                prepareTestTable();

                connection = buildConnection(1);
                statement = buildStatement(connection);

                statement.begin();

                ObDirectLoadBucket bucket = new ObDirectLoadBucket();
                ObObj[] rowObjs = new ObObj[2];
                rowObjs[0] = new ObObj(ObObjType.ObInt32Type.getDefaultObjMeta(), 1);
                rowObjs[1] = new ObObj(ObObjType.ObInt32Type.getDefaultObjMeta(), 2);
                bucket.addRow(rowObjs);
                statement.write(bucket);

                statement.commit();

                queryTestTable(1);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (null != statement) {
                    statement.close();
                }
                if (null != connection) {
                    connection.close();
                }
            }
            System.out.println("SimpleTest successful");
        }

    };

    private static class ParallelWriteTest {

        private static class ParallelWriter implements Runnable {

            private final ObDirectLoadStatement statement;
            private final int                   id;

            ParallelWriter(ObDirectLoadStatement statement, int id) {
                this.statement = statement;
                this.id = id;
            }

            @Override
            public void run() {
                try {
                    ObDirectLoadBucket bucket = new ObDirectLoadBucket();
                    ObObj[] rowObjs = new ObObj[2];
                    rowObjs[0] = new ObObj(ObObjType.ObInt32Type.getDefaultObjMeta(), id);
                    rowObjs[1] = new ObObj(ObObjType.ObInt32Type.getDefaultObjMeta(), id);
                    bucket.addRow(rowObjs);
                    statement.write(bucket);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

        };

        public static void run() {
            System.out.println("ParallelWriteTest start");
            ObDirectLoadConnection connection = null;
            ObDirectLoadStatement statement = null;
            try {
                prepareTestTable();

                connection = buildConnection(parallel);
                statement = buildStatement(connection);

                statement.begin();

                Thread[] threads = new Thread[parallel];
                for (int i = 0; i < threads.length; ++i) {
                    ParallelWriter parallelWriter = new ParallelWriter(statement, i);
                    Thread thread = new Thread(parallelWriter);
                    thread.start();
                    threads[i] = thread;
                }
                for (int i = 0; i < threads.length; ++i) {
                    threads[i].join();
                }

                statement.commit();

                queryTestTable(2);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (null != statement) {
                    statement.close();
                }
                if (null != connection) {
                    connection.close();
                }
            }
            System.out.println("ParallelWriteTest successful");
        }

    };

    private static class MultiNodeWriteTest {

        private static class MultiNodeWriter implements Runnable {

            private final byte[] executionIdBytes;
            private final int    id;

            MultiNodeWriter(byte[] executionIdBytes, int id) {
                this.executionIdBytes = executionIdBytes;
                this.id = id;
            }

            @Override
            public void run() {
                ObDirectLoadConnection connection = null;
                ObDirectLoadStatement statement = null;
                try {
                    ObDirectLoadStatementExecutionId executionId = new ObDirectLoadStatementExecutionId();
                    executionId.decode(executionIdBytes);

                    connection = buildConnection(1);
                    statement = buildStatement(connection);

                    statement.resume(executionId);

                    ObDirectLoadBucket bucket = new ObDirectLoadBucket();
                    ObObj[] rowObjs = new ObObj[2];
                    rowObjs[0] = new ObObj(ObObjType.ObInt32Type.getDefaultObjMeta(), id);
                    rowObjs[1] = new ObObj(ObObjType.ObInt32Type.getDefaultObjMeta(), id);
                    bucket.addRow(rowObjs);
                    statement.write(bucket);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    if (null != statement) {
                        statement.close();
                    }
                    if (null != connection) {
                        connection.close();
                    }
                }
            }

        };

        public static void run() {
            System.out.println("MultiNodeWriteTest start");
            final int nodeNum = 10;
            ObDirectLoadConnection connection = null;
            ObDirectLoadStatement statement = null;
            try {
                prepareTestTable();

                connection = buildConnection(1);
                statement = buildStatement(connection);

                statement.begin();

                ObDirectLoadStatementExecutionId executionId = statement.getExecutionId();
                byte[] executionIdBytes = executionId.encode();

                Thread[] threads = new Thread[nodeNum];
                for (int i = 0; i < threads.length; ++i) {
                    MultiNodeWriter multiNodeWriter = new MultiNodeWriter(executionIdBytes, i);
                    Thread thread = new Thread(multiNodeWriter);
                    thread.start();
                    threads[i] = thread;
                }
                for (int i = 0; i < threads.length; ++i) {
                    threads[i].join();
                }

                statement.commit();

                queryTestTable(nodeNum);
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (null != statement) {
                    statement.close();
                }
                if (null != connection) {
                    connection.close();
                }
            }
            System.out.println("MultiNodeWriteTest successful");
        }

    };

}

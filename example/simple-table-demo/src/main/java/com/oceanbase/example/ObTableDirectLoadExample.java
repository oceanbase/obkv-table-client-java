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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObLoadDupActionType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.direct_load.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableDirectLoad;
import com.alipay.oceanbase.rpc.table.ObDirectLoadBucket;
import com.alipay.oceanbase.rpc.table.ObDirectLoadObjRow;
import com.alipay.oceanbase.rpc.table.ObDirectLoadParameter;
import com.alipay.oceanbase.rpc.util.ObBytesString;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

public class ObTableDirectLoadExample {

    private static String              host                 = "0.0.0.0";
    private static int                 port                 = 0;

    private static String              tenantName           = "mysql";
    private static String              userName             = "root";
    private static String              password             = "";
    private static String              dbName               = "test";
    private static String              tableName            = "lineitem";

    // parameters of direct load
    private static int                 parallel             = 2;                            // Determines the number of server worker threads
    private static long                maxErrorRowCount     = 0;
    private static ObLoadDupActionType dupAction            = ObLoadDupActionType.REPLACE;
    private static long                timeout              = 1000L * 1000 * 1000;          // The overall timeout of the direct load task
    private static long                heartBeatTimeout     = 30L * 1000 * 1000;          // The overall heartbeat timeout of the direct load task

    // load data from csv file
    private static String              CSVFilePath          = "xxx.csv";    // The path of the csv file
    private static byte                lineTermDelimByte    = '\n';         // The line separator
    private static byte                fieldTermDelimByte   = '|';          // The field separator

    // statistics parameter
    private static long                totalRow             = 0;
    private static long                readFileTime         = 0;
    private static long                createBucketTime     = 0;

    private static long                printProgressPerRows = 10 * 10000;

    /*
     * Create a ObObj value of ObBytesString.
     */
    private static ObObj createObj(ObBytesString value) {
        ObObj obj = new ObObj();
        obj.setMeta(ObObjType.defaultObjMeta(value.bytes));
        obj.setValue(value.bytes);
        return obj;
    }

    /*
     * Split string and create a obj row.
     */
    private static ObDirectLoadObjRow createObjRow(ObBytesString rowString) {
        ObBytesString[] fieldsString = rowString.split(fieldTermDelimByte);
        ObObj[] objs = new ObObj[fieldsString.length];
        for (int i = 0; i < fieldsString.length; ++i) {
            objs[i] = createObj(fieldsString[i]);
        }
        return new ObDirectLoadObjRow(objs);
    }

    /**
     * Create a bucket store a bath rows data.
     */
    private static ObDirectLoadBucket createBucket(ObBytesString[] rowsString, int limit) {
        if (rowsString == null)
            throw new NullPointerException();
        if (rowsString.length == 0 || limit > rowsString.length)
            throw new IllegalArgumentException(String.format(
                "invalid args, rowsString.length(%d), limit(%d)", rowsString.length, limit));
        long startTime = System.currentTimeMillis();
        ObDirectLoadBucket bucket = new ObDirectLoadBucket();
        for (int i = 0; i < limit; ++i) {
            bucket.addRow(createObjRow(rowsString[i]));
        }
        long endTime = System.currentTimeMillis();
        createBucketTime += (endTime - startTime);
        return bucket;
    }

    /**
     * Read row from csv file.
     */
    public static class FileReader {
        private RandomAccessFile raf;
        private byte[]           buffer;
        private int              readIndex  = 0;
        private int              writeIndex = 0;

        public FileReader(String filePath) throws FileNotFoundException {
            raf = new RandomAccessFile(filePath, "r");
            buffer = new byte[2 * 1024 * 1024];
        }

        public void close() throws IOException {
            raf.close();
        }

        private void squashBuffer() {
            if (readIndex > 0) {
                System.arraycopy(buffer, readIndex, buffer, 0, writeIndex - readIndex);
                writeIndex -= readIndex;
                readIndex = 0;
            }
        }

        /*
         * Read data from csv file to fill buffer.
         */
        private int fillBuffer() throws IOException {
            squashBuffer();
            long startTime = System.currentTimeMillis();
            int readLen = raf.read(buffer, writeIndex, buffer.length - writeIndex);
            long endTime = System.currentTimeMillis();
            readFileTime += (endTime - startTime);
            if (readLen != -1) {
                writeIndex += readLen;
            }
            return readLen;
        }

        /*
         * Read a row of data from buffer.
         */
        private ObBytesString readLineFromBuffer() {
            ObBytesString line = null;
            for (int i = readIndex; i < writeIndex; ++i) {
                if (buffer[i] == lineTermDelimByte) {
                    byte[] lineBytes = new byte[i - readIndex];
                    System.arraycopy(buffer, readIndex, lineBytes, 0, lineBytes.length);
                    line = new ObBytesString(lineBytes);
                    readIndex = i + 1;
                    break;
                }
            }
            return line;
        }

        /*
         * Read a row of data.
         */
        public ObBytesString readLine() throws IOException {
            ObBytesString line = readLineFromBuffer();
            if (line == null && fillBuffer() != -1) {
                line = readLineFromBuffer();
            }
            return line;
        }

        /**
         * Read several lines of data
         */
        public int readLines(ObBytesString[] lines) throws IOException {
            int count = 0;
            for (int i = 0; i < lines.length; ++i) {
                ObBytesString line = readLine();
                if (line != null) {
                    lines[count++] = line;
                } else {
                    break;
                }
            }
            return count;
        }
    }

    private static void printStatisticsInfo() {
        System.out.println(String.format("当前导入总行数：%d, 读文件耗时：%dms, 构造bucket耗时：%dms", totalRow,
            readFileTime, createBucketTime));
    }

    private static void printFinalStatisticsInfo() {
        System.out.println(String.format("导入总行数：%d, 读文件耗时：%dms, 构造bucket耗时：%dms", totalRow,
            readFileTime, createBucketTime));
    }

    public static void main(String[] args) {
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                printFinalStatisticsInfo();
            }
        });

        ObTable table = null;
        try {
            table = new ObTable.Builder(host, port).setLoginInfo(tenantName, userName, password,
                dbName).build();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        // create a direct load object for a table
        ObDirectLoadParameter parameter = new ObDirectLoadParameter();
        parameter.setParallel(parallel);
        parameter.setMaxErrorRowCount(maxErrorRowCount);
        parameter.setDupAction(dupAction);
        parameter.setTimeout(timeout);
        parameter.setHeartBeatTimeout(heartBeatTimeout);
        ObTableDirectLoad directLoad = new ObTableDirectLoad(table, tableName, parameter,true);

        try {
            // begin direct load for a table
            // is successful, table will be locked
            directLoad.begin();
            // specify the source data segment id to start a trans for load the source segment data
            FileReader fileReader = new FileReader(CSVFilePath);
            ObBytesString[] rowsString = new ObBytesString[100];
            int rowCount = 0;
            while ((rowCount = fileReader.readLines(rowsString)) > 0) {
                // construct a bucket
                ObDirectLoadBucket bucket = createBucket(rowsString, rowCount);
                // send the bucket to observer
                directLoad.insert(bucket);
                // update statistics info
                long prev = totalRow / printProgressPerRows;
                totalRow += bucket.getRowCount();
                long now = totalRow / printProgressPerRows;
                if (now > prev) {
                    printStatisticsInfo();
                }
            }
            // commit direct load
            directLoad.commit();

            // check commit complete
            while (true) {
                ObTableLoadClientStatus status = directLoad.getStatus();
                if (directLoad.isCommitting()) {
                    // wait
                } else if (directLoad.isCommit()) {
                    System.out.println("commit");
                    break;
                } else if (directLoad.isAbort()) {
                    System.out.println("abort");
                    break;
                } else {
                    System.out.println("error status:" + status.toString());
                    directLoad.abort();
                    break;
                }
                Thread.sleep(500);
            }
        } catch (Exception e) {
            e.printStackTrace();
            try {
                // unlock table and release direct load objects
                directLoad.abort();
            } catch (Exception e2) {
                throw new RuntimeException(e2);
            }
        } finally {
            table.close();
        }

    }

}

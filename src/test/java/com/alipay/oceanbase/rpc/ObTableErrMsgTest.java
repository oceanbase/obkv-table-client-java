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
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

import static com.alipay.oceanbase.rpc.mutation.MutationFactory.colVal;
import static org.junit.Assert.*;

public class ObTableErrMsgTest {
    ObTableClient        client;
    public static String tableName = "error_message_table";

    @Before
    public void setup() throws Exception {
        final ObTableClient obTableClient = ObTableClientTestUtil.newTestClient();
        obTableClient.init();
        this.client = obTableClient;
        client.addRowKeyElement(tableName, new String[] { "c1" });
    }

    /**
     CREATE TABLE IF NOT EXISTS  `error_message_table` (
     `c1` bigint(20) not null,
     `c2` varchar(5) not null,
     `c3` datetime default current_timestamp,
     `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
     `c5` double default 0,
     primary key (c1));
     **/
    @Test
    public void testTypeNotMatch() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.insert(tableName).setRowKey(colVal("c1", 1L))
                            .addMutateColVal(colVal("c2", 1L)) // c2 varchar(5) not null
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-10511][OB_KV_COLUMN_TYPE_NOT_MATCH][Column type for 'c2' not match, schema column type is 'VARCHAR', input column type is 'BIGINT']"));
    }

    /**
     CREATE TABLE IF NOT EXISTS  `error_message_table` (
     `c1` bigint(20) not null,
     `c2` varchar(5) not null,
     `c3` datetime default current_timestamp,
     `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
     `c5` double default 0,
     primary key (c1));
     **/
    @Test
    public void testColumnNotExist() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.insert(tableName).setRowKey(colVal("c1", 1L))
                            .addMutateColVal(colVal("cx", 1L)) // cx not exist
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-5217][OB_ERR_BAD_FIELD_ERROR][Unknown column 'cx' in 'error_message_table']"));
    }

    /**
     CREATE TABLE IF NOT EXISTS  `error_message_table` (
     `c1` bigint(20) not null,
     `c2` varchar(5) not null,
     `c3` datetime default current_timestamp,
     `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
     `c5` double default 0,
     primary key (c1));
     **/
    @Test
    public void testDataTooLong() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.insert(tableName).setRowKey(colVal("c1", 1L))
                            .addMutateColVal(colVal("c2", "123456")) // c2 varchar(5) not null
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-5167][OB_ERR_DATA_TOO_LONG][Data too long for column 'c2' at row 0]"));
    }

    /**
     CREATE TABLE IF NOT EXISTS  `error_message_table` (
     `c1` bigint(20) not null,
     `c2` varchar(5) not null,
     `c3` datetime default current_timestamp,
     `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
     `c5` double default 0,
     primary key (c1));
     **/
    @Test
    public void testDataOverflow() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    String dateString = "2023-04-05 14:30:00.123";
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                    Date date = sdf.parse(dateString);
                    client.insert(tableName).setRowKey(colVal("c1", 1L))
                            .addMutateColVal(colVal("c2", "a"))
                            .addMutateColVal(colVal("c3", date)) // `c3` datetime default current_timestamp,
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4157][OB_OPERATE_OVERFLOW][MYSQL_DATETIME value is out of range in 'c3']"));
    }

    /**
     CREATE TABLE IF NOT EXISTS  `error_message_table` (
     `c1` bigint(20) not null,
     `c2` varchar(5) not null,
     `c3` datetime default current_timestamp,
     `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
     `c5` double default 0,
     primary key (c1));
     **/
    @Test
    public void testRowKeyCountNotMatch() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.insert(tableName).setRowKey(colVal("c1", 1L), colVal("c2", 1L))
                            .addMutateColVal(colVal("c2", "a"))
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-10510][OB_KV_ROWKEY_COUNT_NOT_MATCH][Rowkey column count not match, schema rowkey count is '1', input rowkey count is '2']"));
    }

    /**
     CREATE TABLE IF NOT EXISTS  `error_message_table` (
     `c1` bigint(20) not null,
     `c2` varchar(5) not null,
     `c3` datetime default current_timestamp,
     `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
     `c5` double default 0,
     primary key (c1));
     **/
    @Test
    public void testMutateRowKeyNotSupport() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.insert(tableName, 1L, new String[] {"c1", "c2"}, new Object[]{2L, "a"});
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][mutate rowkey column not supported]"));
    }

    /**
     CREATE TABLE IF NOT EXISTS  `error_message_table` (
     `c1` bigint(20) not null,
     `c2` varchar(5) not null,
     `c3` datetime default current_timestamp,
     `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
     `c5` double default 0,
     primary key (c1));
     **/
    @Test
    public void testPrimaryKeyDuplicate() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.insert(tableName).setRowKey(colVal("c1", 1L))
                            .addMutateColVal(colVal("c2", "a"))
                            .execute();
                    client.insert(tableName).setRowKey(colVal("c1", 1L))
                            .addMutateColVal(colVal("c2", "a"))
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-5024][OB_ERR_PRIMARY_KEY_DUPLICATE][Duplicate entry '1' for key 'PRIMARY']"));
    }

    /**
     CREATE TABLE IF NOT EXISTS  `error_message_table` (
     `c1` bigint(20) not null,
     `c2` varchar(5) not null,
     `c3` datetime default current_timestamp,
     `c4` varchar(5) generated always as (SUBSTRING(c2, 1)),
     `c5` double default 0,
     primary key (c1));
     **/
    @Test
    public void testUpdateVirtualColumnNotSupport() {
        ObTableException thrown = assertThrows(
                ObTableException.class,
                () -> {
                    client.update(tableName).setRowKey(colVal("c1", 1L))
                            .addMutateColVal(colVal("c4", "a")) // c4 varchar(5) generated always as (substr(c2, 1)),
                            .execute();
                }
        );

        System.out.println(thrown.getMessage());
        assertTrue(thrown.getMessage().contains("[-4007][OB_NOT_SUPPORTED][The specified value for generated column not supported]"));
    }
}

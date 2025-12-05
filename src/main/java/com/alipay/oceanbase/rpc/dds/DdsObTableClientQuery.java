/*-
* #%L
 * * OceanBase Table Client Framework
 * *
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
 * *
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

package com.alipay.oceanbase.rpc.dds;

import com.alipay.oceanbase.rpc.ObPartKey;
import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.dds.rule.DatabaseAndTable;
import com.alipay.oceanbase.rpc.dds.util.VersionedConfigSnapshot;
import com.alipay.oceanbase.rpc.stream.QueryResultSet;
import com.alipay.oceanbase.rpc.table.AbstractTableQueryImpl;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableClientQueryImpl;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

/**
 * @author zhiqi.zzq
* @since 2021/7/14 下午7:20
*/
public class DdsObTableClientQuery extends AbstractTableQueryImpl {

    private final DdsObTableClient ddsObTableClient;
    private final String           tableName;
    private DatabaseAndTable       databaseAndTable = null;

    /**
     *
    * @param tableName
    * @param ddsObTableClient
    */
    public DdsObTableClientQuery(String tableName, DdsObTableClient ddsObTableClient) {
        this.tableName = tableName;
        this.ddsObTableClient = ddsObTableClient;
        this.tableQuery = new ObTableQuery();
    }

    public DdsObTableClientQuery(String tableName, ObTableQuery tableQuery) {
        this.tableName = tableName;
        this.ddsObTableClient = null;
        this.tableQuery = tableQuery;
    }

    public void SetDatabaseAndTable(int groupID, String tableName, String[] rowKeyColumns) {
        this.databaseAndTable = new DatabaseAndTable();
        this.databaseAndTable.setDatabaseShardValue(groupID);
        this.databaseAndTable.setTableName(tableName);
        this.databaseAndTable.setElasticIndexValue(-1);

        if (rowKeyColumns != null) {
            this.databaseAndTable.setRowKeyColumns(rowKeyColumns);
        }
    }

    /**
     *
    * @return
    */
    @Override
    public ObTableQuery getObTableQuery() {
        return tableQuery;
    }

    /**
     *
    * @return
    */
    @Override
    public String getTableName() {
        return tableName;
    }

    /**
     *
    * @return
    * @throws Exception
    */
    @Override
    public QueryResultSet execute() throws Exception {
        VersionedConfigSnapshot snapshot = ddsObTableClient.getCurrentConfigSnapshot();
        if (snapshot == null) {
            throw new IllegalStateException("Configuration snapshot is not available");
        }
        
        ObTableClient client = null;
        DatabaseAndTable dt = null;
        if (this.databaseAndTable == null) {
            dt = ddsObTableClient.calculateDatabaseAndTable(tableName, tableQuery, snapshot);
            client = ddsObTableClient.getObTableWithSnapshot(dt, snapshot);
        } else {
            dt = this.databaseAndTable;
            client = ddsObTableClient.getObTableWithSnapshot(databaseAndTable, snapshot);
        }

        return new ObTableClientQueryImpl(this.ddsObTableClient.getTargetTableName(dt
            .getTableName()), tableQuery, client).execute();
    }

    // /**
    //  *
    // * @param entry
    // * @return
    // */
    // @Override
    // public QueryResultSet executeInit(ObPair<ObPartKey, ObTable> entry) throws Exception {
    //     throw new IllegalArgumentException("not support executeInit");
    // }

    // @Override
    // public QueryResultSet executeNext(ObPair<ObPartKey, ObTable> entry) throws Exception {
    //     throw new IllegalArgumentException("not support executeNext");
    // }

    /**
     *
    * @param keys
    * @return
    */
    @Override
    public TableQuery setKeys(String... keys) {
        throw new IllegalArgumentException("Not needed");
    }

    /**
     *
    */
    @Override
    public void clear() {
        this.tableQuery = new ObTableQuery();
    }

    @Override
    public QueryResultSet asyncExecute() throws Exception {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'asyncExecute'");
    }
}

/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2024 OceanBase
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

package com.alipay.oceanbase.rpc.checkandmutate;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.filter.ObTableFilter;
import com.alipay.oceanbase.rpc.mutation.InsertOrUpdate;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.table.api.Table;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

import java.util.ArrayList;
import java.util.List;

public class CheckAndInsUp {
    private Table          client;
    private String         tableName;
    private ObTableFilter  filter;
    private InsertOrUpdate insUp;
    boolean                checkExists = true;

    public CheckAndInsUp(ObTableFilter filter, InsertOrUpdate insUp, boolean check_exists)
                                                                                          throws IllegalArgumentException {
        this.filter = filter;
        this.insUp = insUp;
        this.checkExists = check_exists;
        this.tableName = null;
        this.client = null;
    }

    public CheckAndInsUp(Table client, String tableName, ObTableFilter filter,
                         InsertOrUpdate insUp, boolean check_exists)
                                                                    throws IllegalArgumentException {
        this.client = client;
        this.tableName = tableName;
        this.filter = filter;
        this.insUp = insUp;
        this.checkExists = check_exists;
    }

    public Object[] getRowKey() {
        return insUp.getRowKey();
    }

    public ObTableFilter getFilter() {
        return filter;
    }

    public InsertOrUpdate getInsUp() {
        return insUp;
    }

    public boolean isCheckExists() {
        return checkExists;
    }

    public MutationResult execute() throws Exception {
        if (null == tableName) {
            throw new ObTableException("table name is null");
        } else if (null == client) {
            throw new ObTableException("client is null");
        } else if (!(client instanceof ObTableClient)) {
            throw new ObTableException("the client must be table clinet");
        }

        TableQuery query = client.query(tableName);
        query.setFilter(filter);
        Object[] rowKey = getRowKey();
        List<ObNewRange> ranges = new ArrayList<>();
        ObNewRange range = new ObNewRange();
        range.setStartKey(ObRowKey.getInstance(insUp.getRowKey()));
        range.setEndKey(ObRowKey.getInstance(insUp.getRowKey()));
        ranges.add(range);
        query.getObTableQuery().setKeyRanges(ranges);
        ObTableOperation operation = ObTableOperation.getInstance(ObTableOperationType.INSERT_OR_UPDATE,
                insUp.getRowKey(), insUp.getColumns(), insUp.getValues());

        return new MutationResult(((ObTableClient)client).mutationWithFilter(query, rowKey, ranges, operation, false, true, checkExists));
    }
}

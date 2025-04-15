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

package com.alipay.oceanbase.rpc.mutation;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.result.MutationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperation;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.table.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Delete extends Mutation<Delete> {
    /*
     * default constructor
     */
    public Delete() {
        super();
    }

    /*
     * construct with ObTableClient and String
     */
    public Delete(Table client, String tableName) {
        super(client, tableName);
    }

    /*
     * get operation type
     */
    public ObTableOperationType getOperationType() {
        return ObTableOperationType.DEL;
    }

    /*
     * execute
     */
    public MutationResult execute() throws Exception {
        if (null == getTableName() || getTableName().isEmpty()) {
            throw new ObTableException("table name is null");
        } else if (null == getClient()) {
            throw new ObTableException("client is null");
        }

        if (null == getQuery()) {
            // simple Insert, without filter
            return new MutationResult(((ObTableClient) getClient()).deleteWithResult(
                getTableName(), getRowKey()));
        } else {
            // QueryAndDelete
            ObTableOperation operation = ObTableOperation.getInstance(ObTableOperationType.DEL,
                new Object[] {}, null, null);
            return new MutationResult(((ObTableClient) getClient()).mutationWithFilter(getQuery(),
                getRowKey(), operation, false));
        }
    }
}

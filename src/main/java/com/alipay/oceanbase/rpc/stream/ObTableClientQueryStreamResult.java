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

package com.alipay.oceanbase.rpc.stream;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.bolt.transport.ObTableConnection;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class ObTableClientQueryStreamResult extends AbstractQueryStreamResult {
    private static final Logger logger = TableClientLoggerFactory
                                           .getLogger(ObTableClientQueryStreamResult.class);

    protected ObTableQueryResult referToNewPartition(ObPair<Long, ObTableParam> partIdWithObTable)
                                                                                                  throws Exception {
        ObTableQueryRequest request = new ObTableQueryRequest();
        request.setTableName(tableName);
        request.setTableQuery(tableQuery);
        request.setPartitionId(partIdWithObTable.getRight().getPartitionId());
        request.setTableId(partIdWithObTable.getRight().getTableId());
        request.setEntityType(entityType);
        if (operationTimeout > 0) {
            request.setTimeout(operationTimeout);
        } else {
            request.setTimeout(partIdWithObTable.getRight().getObTable()
                .getObTableOperationTimeout());
        }
        request.setConsistencyLevel(getReadConsistency().toObTableConsistencyLevel());
        return execute(partIdWithObTable, request);
    }

    @Override
    protected ObTableQueryResult execute(ObPair<Long, ObTableParam> partIdWithIndex,
                                         ObPayload request) throws Exception {
        // Construct connection reference (useless in sync query)
        AtomicReference<ObTableConnection> connectionRef = new AtomicReference<>();

        // execute request
        ObTableQueryResult result = (ObTableQueryResult) commonExecute(this.client, logger,
            partIdWithIndex, request, connectionRef);

        cacheStreamNext(partIdWithIndex, checkObTableQueryResult(result));

        return result;
    }

    @Override
    protected ObTableQueryAsyncResult executeAsync(ObPair<Long, ObTableParam> partIdWithObTable,
                                                   ObPayload streamRequest) throws Exception {
        throw new IllegalArgumentException("not support this execute");
    }

    @Override
    protected Map<Long, ObPair<Long, ObTableParam>> refreshPartition(ObTableQuery tableQuery,
                                                                     String tableName)
                                                                                      throws Exception {
        return buildPartitions(client, tableQuery, tableName);
    }
}

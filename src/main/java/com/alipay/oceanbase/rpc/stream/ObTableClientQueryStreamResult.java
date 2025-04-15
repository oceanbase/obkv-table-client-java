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
import com.alipay.oceanbase.rpc.exception.ObTableEntryRefreshException;
import com.alipay.oceanbase.rpc.exception.ObTableRetryExhaustedException;
import com.alipay.oceanbase.rpc.exception.ObTableServerCacheExpiredException;
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

import static com.alipay.oceanbase.rpc.protocol.payload.Constants.INVALID_TABLET_ID;

public class ObTableClientQueryStreamResult extends AbstractQueryStreamResult {
    private static final Logger logger = TableClientLoggerFactory
                                           .getLogger(ObTableClientQueryStreamResult.class);

    protected ObTableQueryResult referToNewPartition(ObPair<Long, ObTableParam> partIdWithObTable)
                                                                                                  throws Exception {
        long partitionId = client.getServerCapacity().isSupportDistributedExecute() ? INVALID_TABLET_ID
            : partIdWithObTable.getRight().getPartitionId();
        ObTableQueryRequest request = new ObTableQueryRequest();
        request.setTableName(tableName);
        request.setTableQuery(tableQuery);
        request.setPartitionId(partitionId);
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
        ObTableQueryResult result = null;
        int tryTimes = 0;
        long startExecute = System.currentTimeMillis();
        while (true) {
            long costMillis = System.currentTimeMillis() - startExecute;
            if (costMillis > client.getRuntimeMaxWait()) {
                logger.error("tableName: {} has tried " + tryTimes + " times and it has waited " + costMillis +
                        " ms which execeeds runtime max wait timeout " + client.getRuntimeMaxWait() + " ms", tableName);
                throw new ObTableRetryExhaustedException("query timeout and retried " + tryTimes + " times");
            }
            tryTimes++;
            try {
                result = (ObTableQueryResult) commonExecute(this.client, logger,
                        partIdWithIndex, request, connectionRef);
                break;
            } catch (ObTableServerCacheExpiredException e) {
                client.syncRefreshMetadata(false);
            }  catch (ObTableEntryRefreshException e) {
                if (e.isConnectInactive()) {
                    client.syncRefreshMetadata(true);
                } else {
                    throw e;
                }
            } catch (Throwable t) {
                throw t;
            }
        }
        if (result == null) {
            throw new ObTableRetryExhaustedException("query timeout and retried " + tryTimes + " times");
        }
        
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

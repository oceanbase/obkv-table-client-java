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
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableReplicaNotReadableException;
import com.alipay.oceanbase.rpc.exception.ObTableTimeoutExcetion;
import com.alipay.oceanbase.rpc.location.model.ObServerRoute;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class ObTableClientQueryStreamResult extends AbstractQueryStreamResult {

    private static final Logger logger = TableClientLoggerFactory
                                           .getLogger(ObTableClientQueryStreamResult.class);
    protected ObTableClient     client;

    protected ObTableQueryResult execute(ObPair<Long, ObTable> partIdWithIndex, ObPayload request)
                                                                                                  throws Exception {
        Object result;
        ObTable subObTable = partIdWithIndex.getRight();
        boolean needRefreshTableEntry = false;
        int tryTimes = 0;
        long startExecute = System.currentTimeMillis();
        Set<String> failedServerList = null;
        ObServerRoute route = null;
        while (true) {
            client.checkStatus();
            long currentExecute = System.currentTimeMillis();
            long costMillis = currentExecute - startExecute;
            if (costMillis > client.getRuntimeMaxWait()) {
                throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                                                 + " times and it has waited " + costMillis
                                                 + "/ms which exceeds response timeout "
                                                 + client.getRuntimeMaxWait() + "/ms");
            }
            tryTimes++;
            try {
                // 重试时重新 getTable
                if (tryTimes > 1) {
                    if (route == null) {
                        route = client.getReadRoute();
                    }
                    if (failedServerList != null) {
                        route.setBlackList(failedServerList);
                    }
                    subObTable = client.getTable(tableName, partIdWithIndex.getLeft(),
                        needRefreshTableEntry, client.isTableEntryRefreshIntervalWait(), route)
                        .getRight();
                }
                result = subObTable.execute(request);
                client.resetExecuteContinuousFailureCount(tableName);
                break;
            } catch (ObTableReplicaNotReadableException ex) {
                if ((tryTimes - 1) < client.getRuntimeRetryTimes()) {
                    logger.warn("retry when replica not readable: {}", ex.getMessage());
                    if (failedServerList == null) {
                        failedServerList = new HashSet<String>();
                    }
                    failedServerList.add(subObTable.getIp());
                } else {
                    logger.warn("exhaust retry when replica not readable: {}", ex.getMessage());
                    throw ex;
                }
            } catch (Exception e) {
                if (e instanceof ObTableException
                    && ((ObTableException) e).isNeedRefreshTableEntry()) {
                    needRefreshTableEntry = true;
                    logger
                        .warn(
                            "stream query refresh table while meet ObTableMasterChangeException, errorCode: {}",
                            ((ObTableException) e).getErrorCode());
                    if (client.isRetryOnChangeMasterTimes()
                        && (tryTimes - 1) < client.getRuntimeRetryTimes()) {
                        logger
                            .warn(
                                "stream query retry while meet ObTableMasterChangeException, errorCode: {} , retry times {}",
                                ((ObTableException) e).getErrorCode(), tryTimes);
                    } else {
                        client.calculateContinuousFailure(tableName, e.getMessage());
                        throw e;
                    }
                } else {
                    client.calculateContinuousFailure(tableName, e.getMessage());
                    throw e;
                }
            }
            Thread.sleep(client.getRuntimeRetryInterval());
        }

        cacheStreamNext(partIdWithIndex, checkObTableQueryResult(result));

        return (ObTableQueryResult) result;
    }

    /**
     * Get client.
     */
    public ObTableClient getClient() {
        return client;
    }

    /**
     * Set client.
     */
    public void setClient(ObTableClient client) {
        this.client = client;
    }
}

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
import com.alipay.oceanbase.rpc.exception.ObTableGlobalIndexRouteException;
import com.alipay.oceanbase.rpc.exception.ObTableReplicaNotReadableException;
import com.alipay.oceanbase.rpc.exception.ObTableTimeoutExcetion;
import com.alipay.oceanbase.rpc.location.model.ObServerRoute;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.util.HashSet;
import java.util.Set;

public class ObTableClientQueryStreamResult extends AbstractQueryStreamResult {

    private static final Logger logger = TableClientLoggerFactory
                                           .getLogger(ObTableClientQueryStreamResult.class);
    protected ObTableClient     client;

    protected ObTableQueryResult execute(ObPair<Long, ObTableParam> partIdWithIndex,
                                         ObPayload request) throws Exception {
        Object result;
        ObTable subObTable = partIdWithIndex.getRight().getObTable();
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
                long uniqueId = request.getUniqueId();
                long sequence = request.getSequence();
                String trace = String.format("Y%X-%016X", uniqueId, sequence);
                throw new ObTableTimeoutExcetion("[" + trace + "]" + " has tried " + tryTimes
                                                 + " times and it has waited " + costMillis
                                                 + "/ms which exceeds response timeout "
                                                 + client.getRuntimeMaxWait() + "/ms");
            }
            tryTimes++;
            try {
                // 重试时重新 getTable
                if (tryTimes > 1) {
                    if (client.isOdpMode()) {
                        subObTable = client.getOdpTable();
                    } else {
                        if (route == null) {
                            route = client.getReadRoute();
                        }
                        if (failedServerList != null) {
                            route.setBlackList(failedServerList);
                        }
                        subObTable = client
                            .getTable(indexTableName, partIdWithIndex.getLeft(),
                                needRefreshTableEntry, client.isTableEntryRefreshIntervalWait(),
                                route).getRight().getObTable();
                    }
                }
                result = subObTable.execute(request);
                client.resetExecuteContinuousFailureCount(indexTableName);
                break;
            } catch (Exception e) {
                if (client.isOdpMode()) {
                    if ((tryTimes - 1) < client.getRuntimeRetryTimes()) {
                        if (e instanceof ObTableException) {
                            logger
                                .warn(
                                    "tablename:{} stream query execute while meet Exception needing retry, errorCode: {}, errorMsg: {}, try times {}",
                                    indexTableName, ((ObTableException) e).getErrorCode(),
                                    e.getMessage(), tryTimes);
                        } else if (e instanceof IllegalArgumentException) {
                            logger
                                .warn(
                                    "tablename:{} stream query execute while meet Exception needing retry, try times {}, errorMsg: {}",
                                    indexTableName, tryTimes, e.getMessage());
                        } else {
                            logger
                                .warn(
                                    "tablename:{} stream query execute while meet Exception needing retry, try times {}",
                                    indexTableName, tryTimes, e);
                        }
                    } else {
                        throw e;
                    }
                } else {
                    if (e instanceof ObTableReplicaNotReadableException) {
                        if ((tryTimes - 1) < client.getRuntimeRetryTimes()) {
                            logger.warn(
                                "tablename:{} partition id:{} retry when replica not readable: {}",
                                indexTableName, partIdWithIndex.getLeft(), e.getMessage(), e);
                            if (failedServerList == null) {
                                failedServerList = new HashSet<String>();
                            }
                            failedServerList.add(subObTable.getIp());
                        } else {
                            logger
                                .warn(
                                    "tablename:{} partition id:{} exhaust retry when replica not readable: {}",
                                    indexTableName, partIdWithIndex.getLeft(), e.getMessage(), e);
                            throw e;
                        }
                    } else if (e instanceof ObTableException
                               && ((ObTableException) e).isNeedRefreshTableEntry()) {
                        needRefreshTableEntry = true;
                        logger
                            .warn(
                                "tablename:{} partition id:{} stream query refresh table while meet Exception needing refresh, errorCode: {}",
                                indexTableName, partIdWithIndex.getLeft(),
                                ((ObTableException) e).getErrorCode(), e);
                        if (client.isRetryOnChangeMasterTimes()
                            && (tryTimes - 1) < client.getRuntimeRetryTimes()) {
                            logger
                                .warn(
                                    "tablename:{} partition id:{} stream query retry while meet Exception needing refresh, errorCode: {} , retry times {}",
                                    indexTableName, partIdWithIndex.getLeft(),
                                    ((ObTableException) e).getErrorCode(), tryTimes, e);
                        } else {
                            client.calculateContinuousFailure(indexTableName, e.getMessage());
                            throw e;
                        }
                    } else if (e instanceof ObTableGlobalIndexRouteException) {
                        if ((tryTimes - 1) < client.getRuntimeRetryTimes()) {
                            logger
                                .warn(
                                    "meet global index route expcetion: indexTableName:{} partition id:{}, errorCode: {}, retry times {}",
                                    indexTableName, partIdWithIndex.getLeft(),
                                    ((ObTableException) e).getErrorCode(), tryTimes, e);
                            indexTableName = client.getIndexTableName(tableName,
                                tableQuery.getIndexName(), tableQuery.getScanRangeColumns(), true);
                        } else {
                            logger
                                .warn(
                                    "meet global index route expcetion: indexTableName:{} partition id:{}, errorCode: {}, reach max retry times {}",
                                    indexTableName, partIdWithIndex.getLeft(),
                                    ((ObTableException) e).getErrorCode(), tryTimes, e);
                            throw e;
                        }
                    } else {
                        client.calculateContinuousFailure(indexTableName, e.getMessage());
                        throw e;
                    }
                }
            }
            Thread.sleep(client.getRuntimeRetryInterval());
        }

        cacheStreamNext(partIdWithIndex, checkObTableQueryResult(result));

        return (ObTableQueryResult) result;
    }

    @Override
    protected ObTableQueryAsyncResult executeAsync(ObPair<Long, ObTableParam> partIdWithObTable,
                                                   ObPayload streamRequest) throws Exception {
        throw new IllegalArgumentException("not support this execute");
    }

    /**
     * Get client.
     * @return client
     */
    public ObTableClient getClient() {
        return client;
    }

    /*
     * Set client.
     */
    public void setClient(ObTableClient client) {
        this.client = client;
    }
}

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

package com.alipay.oceanbase.rpc.stream.async;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTableGlobalIndexRouteException;
import com.alipay.oceanbase.rpc.exception.ObTableTimeoutExcetion;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObTableClientQueryAsyncStreamResult extends AbstractQueryStreamResult {
    private static final Logger logger = LoggerFactory
                                           .getLogger(ObTableClientQueryStreamResult.class);
    protected ObTableClient     client;
    private boolean             isEnd  = true;
    private long                sessionId;
    private boolean             hasMore;

    @Override
    protected ObTableQueryResult execute(ObPair<Long, ObTableParam> partIdWithObTable,
                                         ObPayload streamRequest) throws Exception {
        throw new IllegalArgumentException("not support this execute");
    }

    @Override
    protected ObTableQueryAsyncResult executeAsync(ObPair<Long, ObTableParam> partIdWithObTable,
                                                   ObPayload streamRequest) throws Exception {
        Object result;
        ObTable subObTable = partIdWithObTable.getRight().getObTable();
        boolean needRefreshTableEntry = false;
        int tryTimes = 0;
        long startExecute = System.currentTimeMillis();

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
                if (needRefreshTableEntry) {
                    subObTable = client
                        .getTable(indexTableName, new Long[] { partIdWithObTable.getLeft() }, true,
                            client.isTableEntryRefreshIntervalWait()).getRight().getObTable();
                }
                result = subObTable.execute(streamRequest);
                client.resetExecuteContinuousFailureCount(indexTableName);
                break;
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
                        client.calculateContinuousFailure(indexTableName, e.getMessage());
                        throw e;
                    }
                } else if (e instanceof ObTableGlobalIndexRouteException) {
                    if ((tryTimes - 1) < client.getRuntimeRetryTimes()) {
                        logger
                            .warn(
                                "meet global index route expcetion: indexTableName:{} partition id:{}, errorCode: {}, retry times {}",
                                indexTableName, partIdWithObTable.getLeft(),
                                ((ObTableException) e).getErrorCode(), tryTimes, e);
                        indexTableName = client.getIndexTableName(tableName,
                            tableQuery.getIndexName(), tableQuery.getScanRangeColumns(), true);
                    } else {
                        logger
                            .warn(
                                "meet global index route expcetion: indexTableName:{} partition id:{}, errorCode: {}, reach max retry times {}",
                                indexTableName, partIdWithObTable.getLeft(),
                                ((ObTableException) e).getErrorCode(), tryTimes, e);
                        throw e;
                    }
                } else {
                    client.calculateContinuousFailure(indexTableName, e.getMessage());
                    throw e;
                }
            }
            Thread.sleep(client.getRuntimeRetryInterval());
        }

        cacheStreamNext(partIdWithObTable, checkObTableQueryAsyncResult(result));

        ObTableQueryAsyncResult obTableQueryAsyncResult = (ObTableQueryAsyncResult) result;
        if (obTableQueryAsyncResult.isEnd()) {
            isEnd = true;
        } else {
            isEnd = false;
        }
        sessionId = obTableQueryAsyncResult.getSessionId();
        return (ObTableQueryAsyncResult) result;
    }

    public ObTableClient getClient() {
        return client;
    }

    /**
     * Set client.
     * @param client client want to set
     */
    public void setClient(ObTableClient client) {
        this.client = client;
    }

    public boolean isEnd() {
        return isEnd;
    }

    public void setEnd(boolean end) {
        isEnd = end;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    public boolean hasMore() {
        return hasMore;
    }

    public void setHasMore(boolean hasMore) {
        this.hasMore = hasMore;
    }

}

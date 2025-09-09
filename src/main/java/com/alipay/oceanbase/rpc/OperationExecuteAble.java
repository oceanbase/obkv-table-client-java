/*-
 * #%L
 * OceanBase Table Client
 * %%
 * Copyright (C) 2016 - 2025 OceanBase
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

import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableAbstractOperationRequest;

import java.util.concurrent.ExecutorService;

public interface OperationExecuteAble {
    /**
     * ObTableAbstractOperationRequest execute
     * @param request ObTableAbstractOperationRequest
     * @return
     * @throws Exception
     */
    ObPayload execute(final ObTableAbstractOperationRequest request) throws Exception;

    /**
     * getOrRefreshTableEntry
     * @param tableName 表名
     * @param refresh 刷新
     * @param waitForRefresh 是否刷新
     * @return
     * @throws Exception
     */
    TableEntry getOrRefreshTableEntry(final String tableName, final boolean refresh,
                                      final boolean waitForRefresh) throws Exception;

    /**
     *
     * @param tableName
     * @param groupID
     * @param refresh
     * @param waitForRefresh
     * @return
     * @throws Exception
     */
    TableEntry getOrRefreshTableEntry(final String tableName, final Integer groupID,
                                      final boolean refresh, final boolean waitForRefresh)
                                                                                          throws Exception;

    /**
     *
     * @param runtimeBatchExecutor
     */
    void setRuntimeBatchExecutor(ExecutorService runtimeBatchExecutor);
}

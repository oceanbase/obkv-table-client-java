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

package com.alipay.oceanbase.rpc.mutation.result;

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateResult;

public class MutationResult {
    private final ObPayload result;

    /*
     * construct with ObPayload
     */
    public MutationResult(ObPayload result) {
        if (null == result) {
            throw new IllegalArgumentException("Invalid null result");
        }

        this.result = result;
    }

    /*
     * get operation type of result
     */
    public ObTableOperationType getOperationType() {
        return ((ObTableOperationResult) result).getOperationType();
    }

    /*
     * get the affected rows of mutation
     */
    public long getAffectedRows() {
        long affectedRows = 0;
        switch (result.getPcode()) {
            case Pcodes.OB_TABLE_API_EXECUTE:
                affectedRows = ((ObTableOperationResult) result).getAffectedRows();
                break;
            case Pcodes.OB_TABLE_API_QUERY_AND_MUTATE:
                affectedRows = ((ObTableQueryAndMutateResult) result).getAffectedRows();
                break;
            default:
                throw new ObTableException("unknown result type: " + result.getPcode());
        }
        return affectedRows;
    }
}

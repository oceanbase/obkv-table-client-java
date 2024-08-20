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
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableSingleOpResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.mutate.ObTableQueryAndMutateResult;

import java.util.Map;

public class MutationResult extends OperationResult {
    /*
     * construct with ObPayload
     */
    public MutationResult(ObPayload result) {
        super(result);
    }

    /*
     * get operation type of result
     */
    public ObTableOperationType getOperationType() {
        if (result instanceof ObTableSingleOpResult) {
            return ((ObTableSingleOpResult) result).getOperationType();
        }
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
            case Pcodes.OB_TABLE_API_LS_EXECUTE:
                affectedRows = ((ObTableSingleOpResult) result).getAffectedRows();
                break;
            default:
                throw new ObTableException("unknown result type: " + result.getPcode());
        }
        return affectedRows;
    }

    /*
     * get the result rows of operation
     */
    public Row getOperationRow() {
        Map<String, Object> rowsMap;
        switch (result.getPcode()) {
            case Pcodes.OB_TABLE_API_EXECUTE:
                rowsMap = ((ObTableOperationResult) result).getEntity().getSimpleProperties();
                break;
            case Pcodes.OB_TABLE_API_QUERY_AND_MUTATE:
                throw new ObTableException("could not get query and mutate result now"
                                           + result.getPcode());
            case Pcodes.OB_TABLE_API_LS_EXECUTE:
                rowsMap = ((ObTableSingleOpResult) result).getEntity().getSimpleProperties();
                break;
            default:
                throw new ObTableException("unknown result type: " + result.getPcode());
        }
        return new Row(rowsMap);
    }
}

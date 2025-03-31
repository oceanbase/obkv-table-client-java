/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2025 OceanBase
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

package com.alipay.oceanbase.rpc.get.result;

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.mutation.result.OperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableSingleOpResult;

import java.util.Map;

public class GetResult extends OperationResult {

    public GetResult(ObPayload result) {
        super(result);
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
            case Pcodes.OB_TABLE_API_LS_EXECUTE:
                rowsMap = ((ObTableSingleOpResult) result).getEntity().getSimpleProperties();
                break;
            default:
                throw new ObTableException("unknown result type: " + result.getPcode());
        }
        return new Row(rowsMap);
    }
}

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

import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.AbstractQueryStreamResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryResult;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.ObTableQueryAsyncResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;

public class ObTableQueryStreamResult extends AbstractQueryStreamResult {

    protected ObTableQueryResult execute(ObPair<Long, ObTableParam> partIdWithIndex,
                                         ObPayload request) throws Exception {
        Object result = partIdWithIndex.getRight().getObTable().execute(request);

        cacheStreamNext(partIdWithIndex, checkObTableQueryResult(result));

        return (ObTableQueryResult) result;
    }

    @Override
    protected ObTableQueryAsyncResult executeAsync(ObPair<Long, ObTableParam> partIdWithObTable,
                                                   ObPayload streamRequest) throws Exception {
        throw new IllegalArgumentException("not support this execute");
    }
}

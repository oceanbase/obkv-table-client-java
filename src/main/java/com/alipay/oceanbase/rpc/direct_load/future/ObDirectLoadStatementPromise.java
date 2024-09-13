/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.direct_load.future;

import com.alipay.oceanbase.rpc.direct_load.exception.ObDirectLoadException;

public interface ObDirectLoadStatementPromise extends ObDirectLoadStatementFuture {

    /**
     * 标记异步操作结果为成功
     */
    ObDirectLoadStatementPromise setSuccess();

    /**
     * 标记异步操作结果为失败
     */
    ObDirectLoadStatementPromise setFailure(ObDirectLoadException cause);

}

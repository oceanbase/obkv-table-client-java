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

package com.alipay.oceanbase.rpc.direct_load.exception;

import com.alipay.oceanbase.rpc.direct_load.protocol.payload.ObTableLoadClientStatus;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;

public class ObDirectLoadServerStatusException extends ObDirectLoadException {

    private final ObTableLoadClientStatus status;
    private final int                     errorCode;

    public ObDirectLoadServerStatusException(ObTableLoadClientStatus status, int errorCode) {
        super("status:" + status + ", errorCode:" + errorCode);
        this.status = status;
        this.errorCode = errorCode;
    }

    public ObDirectLoadServerStatusException(ObTableLoadClientStatus status, ResultCodes errorCode) {
        super("status:" + status + ", errorCode:" + errorCode);
        this.status = status;
        this.errorCode = errorCode.errorCode;
    }

    public ObTableLoadClientStatus getStatus() {
        return status;
    }

    public int getErrorCode() {
        return errorCode;
    }

}

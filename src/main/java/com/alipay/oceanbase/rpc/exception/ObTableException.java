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

package com.alipay.oceanbase.rpc.exception;

import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;

public class ObTableException extends RuntimeException {

    private int     errorCode             = -1;
    private boolean needRefreshTableEntry = false;

    /*
     * Ob table exception.
     */
    public ObTableException() {
    }

    /*
     * Ob table exception.
     */
    public ObTableException(int errorCode) {
        this("error code: " + errorCode, errorCode);
    }

    /*
     * Ob table exception.
     */
    public ObTableException(String message, int errorCode) {
        this(message);
        this.errorCode = errorCode;
    }

    /*
     * Ob table exception.
     */
    public ObTableException(String message) {
        super(message);
    }

    /*
     * Ob table exception.
     */
    public ObTableException(String message, Throwable cause) {
        super(message, cause);
        if (cause instanceof ObTableException) {
            errorCode = ((ObTableException) cause).getErrorCode();
            needRefreshTableEntry = ((ObTableException) cause).isNeedRefreshTableEntry();
        }
    }

    /*
     * Ob table exception.
     */
    public ObTableException(Throwable cause) {
        super(cause);
    }

    /*
     * Get error code.
     */
    public int getErrorCode() {
        return errorCode;
    }

    /*
     * Is need refresh table entry.
     */
    public boolean isNeedRefreshTableEntry() {
        return needRefreshTableEntry;
    }

    public boolean isNeedRetryServerError() {
        return errorCode == ResultCodes.OB_TRY_LOCK_ROW_CONFLICT.errorCode
               || errorCode == ResultCodes.OB_TRANSACTION_SET_VIOLATION.errorCode
               || errorCode == ResultCodes.OB_SCHEMA_EAGAIN.errorCode;
    }

}

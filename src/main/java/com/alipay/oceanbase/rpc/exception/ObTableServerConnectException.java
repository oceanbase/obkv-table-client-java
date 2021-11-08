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

public class ObTableServerConnectException extends ObTableException {

    /**
     * Ob table server connect exception.
     */
    public ObTableServerConnectException() {
    }

    /**
     * Ob table server connect exception.
     */
    public ObTableServerConnectException(int errorCode) {
        super(errorCode);
    }

    /**
     * Ob table server connect exception.
     */
    public ObTableServerConnectException(String message, int errorCode) {
        super(message, errorCode);
    }

    /**
     * Ob table server connect exception.
     */
    public ObTableServerConnectException(String message) {
        super(message);
    }

    /**
     * Ob table server connect exception.
     */
    public ObTableServerConnectException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Ob table server connect exception.
     */
    public ObTableServerConnectException(Throwable cause) {
        super(cause);
    }

    /**
     * Is need refresh table entry.
     */
    public boolean isNeedRefreshTableEntry() {
        return true;
    }
}

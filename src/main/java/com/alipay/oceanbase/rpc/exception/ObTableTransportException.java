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

import com.alipay.oceanbase.rpc.bolt.transport.TransportCodes;

public class ObTableTransportException extends ObTableException {

    /**
     * Ob table transport exception.
     */
    public ObTableTransportException() {
    }

    /**
     * Ob table transport exception.
     */
    public ObTableTransportException(int errorCode) {
        super(errorCode);
    }

    /**
     * Ob table transport exception.
     */
    public ObTableTransportException(String message, int errorCode) {
        super(message, errorCode);
    }

    /**
     * Ob table transport exception.
     */
    public ObTableTransportException(String message) {
        super(message);
    }

    /**
     * Ob table transport exception.
     */
    public ObTableTransportException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Ob table transport exception.
     */
    public ObTableTransportException(Throwable cause) {
        super(cause);
    }

    /**
     * Is need refresh table entry.
     */
    @Override
    public boolean isNeedRefreshTableEntry() {
        // send failed should refresh table entry
        return getErrorCode() == TransportCodes.BOLT_SEND_FAILED;
    }
}

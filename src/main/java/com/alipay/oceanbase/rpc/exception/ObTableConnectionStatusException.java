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

public class ObTableConnectionStatusException extends ObTableException {
    /*
     * Ob table connection status exception.
     */
    public ObTableConnectionStatusException() {
    }

    /*
     * Ob table connection status exception with error code.
     */
    public ObTableConnectionStatusException(int errorCode) {
        super(errorCode);
    }

    /*
     * Ob table connection status exception with message and error code.
     */
    public ObTableConnectionStatusException(String message, int errorCode) {
        super(message, errorCode);
    }

    /*
     * Ob table connection status exception with message.
     */
    public ObTableConnectionStatusException(String message) {
        super(message);
    }

    /*
     * Ob table connection status exception with message and cause.
     */
    public ObTableConnectionStatusException(String message, Throwable cause) {
        super(message, cause);
    }

    /*
     * Ob table connection status exception with cause.
     */
    public ObTableConnectionStatusException(Throwable cause) {
        super(cause);
    }
}

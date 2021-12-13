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

public class ObTableNotExistException extends ObTableEntryRefreshException {

    /*
     * Ob table not exist exception.
     */
    public ObTableNotExistException() {
    }

    /*
     * Ob table not exist exception.
     */
    public ObTableNotExistException(int errorCode) {
        super(errorCode);
    }

    /*
     * Ob table not exist exception.
     */
    public ObTableNotExistException(String message, int errorCode) {
        super(message, errorCode);
    }

    /*
     * Ob table not exist exception.
     */
    public ObTableNotExistException(String message) {
        super(message);
    }

    /*
     * Ob table not exist exception.
     */
    public ObTableNotExistException(String message, Throwable cause) {
        super(message, cause);
    }

    /*
     * Ob table not exist exception.
     */
    public ObTableNotExistException(Throwable cause) {
        super(cause);
    }
}

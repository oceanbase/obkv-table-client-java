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

package com.alipay.oceanbase.rpc.exception;

public class ObTableSchemaVersionMismatchException extends ObTableException {
    /*
     * Ob table schema version mismatch exception.
     */
    public ObTableSchemaVersionMismatchException() {
    }

    /*
     * Ob table schema version mismatch exception with error code.
     */
    public ObTableSchemaVersionMismatchException(int errorCode) {
        super(errorCode);
    }

    /*
     * Ob table schema version mismatch exception with message and error code.
     */
    public ObTableSchemaVersionMismatchException(String message, int errorCode) {
        super(message, errorCode);
    }

    /*
     * Ob table schema version mismatch exception with message.
     */
    public ObTableSchemaVersionMismatchException(String message) {
        super(message);
    }

    /*
     * Ob table schema version mismatch exception with message and cause.
     */
    public ObTableSchemaVersionMismatchException(String message, Throwable cause) {
        super(message, cause);
    }

    /*
     * Ob table schema version mismatch exception with case.
     */
    public ObTableSchemaVersionMismatchException(Throwable cause) {
        super(cause);
    }
}

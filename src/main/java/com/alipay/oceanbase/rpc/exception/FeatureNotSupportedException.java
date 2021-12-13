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

public class FeatureNotSupportedException extends RuntimeException {

    private int errorCode;

    /*
     * Feature not supported exception.
     */
    public FeatureNotSupportedException() {
    }

    /*
     * Feature not supported exception.
     */
    public FeatureNotSupportedException(int errorCode) {
        this("error code: " + errorCode, errorCode);
    }

    /*
     * Feature not supported exception.
     */
    public FeatureNotSupportedException(String message, int errorCode) {
        this(message);
        this.errorCode = errorCode;
    }

    /*
     * Feature not supported exception.
     */
    public FeatureNotSupportedException(String message) {
        super(message);
    }

    /*
     * Feature not supported exception.
     */
    public FeatureNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    /*
     * Feature not supported exception.
     */
    public FeatureNotSupportedException(Throwable cause) {
        super(cause);
    }

    /*
     * Get error code.
     */
    public int getErrorCode() {
        return errorCode;
    }

}

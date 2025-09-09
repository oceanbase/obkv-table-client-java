/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
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

/**
 * @author zhiqi.zzq
 * @since 2018/8/20 下午5:02
 */
public class DistributeDispatchException extends RuntimeException {

    private int errorCode;

    /**
     * Generate column parse exception.
     */
    public DistributeDispatchException() {
    }

    /**
     * Generate column parse exception.
     */
    public DistributeDispatchException(int errorCode) {
        this("error code: " + errorCode, errorCode);
    }

    /**
     * Generate column parse exception.
     */
    public DistributeDispatchException(String message, int errorCode) {
        this(message);
        this.errorCode = errorCode;
    }

    /**
     * Generate column parse exception.
     */
    public DistributeDispatchException(String message) {
        super(message);
    }

    /**
     * Generate column parse exception.
     */
    public DistributeDispatchException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Generate column parse exception.
     */
    public DistributeDispatchException(Throwable cause) {
        super(cause);
    }

    /**
     * Get error code.
     */
    public int getErrorCode() {
        return errorCode;
    }

}

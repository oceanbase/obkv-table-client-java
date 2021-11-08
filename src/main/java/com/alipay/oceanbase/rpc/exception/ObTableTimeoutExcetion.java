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

public class ObTableTimeoutExcetion extends ObTableException {
    /**
     * Ob table timeout excetion.
     */
    public ObTableTimeoutExcetion() {
    }

    /**
     * Ob table timeout excetion.
     */
    public ObTableTimeoutExcetion(int errorCode) {
        super(errorCode);
    }

    /**
     * Ob table timeout excetion.
     */
    public ObTableTimeoutExcetion(String message, int errorCode) {
        super(message, errorCode);
    }

    /**
     * Ob table timeout excetion.
     */
    public ObTableTimeoutExcetion(String message) {
        super(message);
    }

    /**
     * Ob table timeout excetion.
     */
    public ObTableTimeoutExcetion(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Ob table timeout excetion.
     */
    public ObTableTimeoutExcetion(Throwable cause) {
        super(cause);
    }
}

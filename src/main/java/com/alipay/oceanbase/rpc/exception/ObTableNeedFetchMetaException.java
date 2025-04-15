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

package com.alipay.oceanbase.rpc.exception;

import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;

public class ObTableNeedFetchMetaException extends ObTableException {
    /*
     * Ob table routing wrong exception.
     */
    public ObTableNeedFetchMetaException() {
    }

    /*
     * Ob table routing wrong exception with error code.
     */
    public ObTableNeedFetchMetaException(int errorCode) {
        super(errorCode);
    }

    /*
     * Ob table routing wrong exception with message and error code.
     */
    public ObTableNeedFetchMetaException(String message, int errorCode) {
        super(message, errorCode);
    }

    /*
     * Ob table routing wrong exception with message.
     */
    public ObTableNeedFetchMetaException(String message) {
        super(message);
    }

    /*
     * Ob table routing wrong exception with message and cause.
     */
    public ObTableNeedFetchMetaException(String message, Throwable cause) {
        super(message, cause);
    }

    /*
     * Ob table routing wrong exception with cause.
     */
    public ObTableNeedFetchMetaException(Throwable cause) {
        super(cause);
    }

    /*
     * Is need refresh table entry.
     */
    public boolean isNeedRefreshTableEntry() {
        return true;
    }

    public boolean isNeedRefreshMetaAndLocation() {
        return getErrorCode() == ResultCodes.OB_LS_NOT_EXIST.errorCode // need to refresh the whole tablets in this ls
               || getErrorCode() == ResultCodes.OB_SNAPSHOT_DISCARDED.errorCode // fetch a wrong ls tablets, need to refetch locations
               || getErrorCode() == ResultCodes.OB_TABLET_NOT_EXIST.errorCode
               || getErrorCode() == ResultCodes.OB_ERR_OPERATION_ON_RECYCLE_OBJECT.errorCode // table has been drop and recreated
               || getErrorCode() == ResultCodes.OB_MAPPING_BETWEEN_TABLET_AND_LS_NOT_EXIST.errorCode;
    }
}

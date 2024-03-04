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

package com.alipay.oceanbase.rpc.protocol.payload;

public interface Constants {

    long  OB_INVALID_ID      = -1;

    long  INVALID_TABLET_ID  = 0;

    short UNSIGNED_INT8_MAX  = (1 << 8) - 1;

    int   UNSIGNED_INT16_MAX = (1 << 16) - 1;

    int   UNSIGNED_INT24_MAX = (1 << 24) - 1;

    long  UNSIGNED_INT32_MAX = (1L << 32) - 1;

    long  INVALID_LS_ID = -1;

}

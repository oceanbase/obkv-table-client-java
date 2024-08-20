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

public interface Pcodes {

    int OB_TABLE_API_LOGIN              = 0x1101;
    int OB_TABLE_API_EXECUTE            = 0x1102;
    int OB_TABLE_API_BATCH_EXECUTE      = 0x1103;
    int OB_TABLE_API_EXECUTE_QUERY      = 0x1104;
    int OB_TABLE_API_QUERY_AND_MUTATE   = 0x1105;

    // INVALID REQUEST PCODE, no such rpc
    int OB_ERROR_PACKET                 = 0x010;

    int OB_TABLE_API_EXECUTE_QUERY_SYNC = 0x1106;

    int OB_TABLE_API_DIRECT_LOAD        = 0x1123;
    int OB_TABLE_API_MOVE               = 0x1124;
    int OB_TABLE_API_LS_EXECUTE         = 0x1125;
}

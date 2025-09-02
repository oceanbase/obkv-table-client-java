/*-
* #%L
 * * OceanBase Table Client Framework
 * *
 * %%
 * Copyright (C) 2016 - 2025 Ant Financial Services Group
 * *
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

/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2024 All Rights Reserved.
 */
package com.alipay.oceanbase.rpc;

/**
 * @author qiaoyunyao
 * @version 1: ObPartKey.java, v 0.1 2024年08月20日 20:51 qiaoyunyao Exp $
 */
public class ObPartKey {
    long tableId;
    long partId;

    public ObPartKey(long tableId, long partId) {
        this.partId = partId;
        this.tableId = tableId;
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public long getPartId() {
        return partId;
    }

    public void setPartId(long partId) {
        this.partId = partId;
    }
}

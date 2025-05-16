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

package com.alipay.oceanbase.rpc.meta;

// define rpc meta type enum
public enum ObTableRpcMetaType {
    INVALID(0), TABLE_PARTITION_INFO(1), // 分区信息, 用于路由刷新
    HTABLE_REGION_LOCATOR(2), // 分区上下界
    HTABLE_REGION_METRICS(3), // 分区统计信息
    HTABLE_CREATE_TABLE(4), // 建表
    HTABLE_DELETE_TABLE(5), // 删表
    HTABLE_TRUNCATE_TABLE(6), // 清空表
    HTABLE_EXISTS(7), // 检查表是否存在
    HTABLE_GET_DESC(8), // 获取表元信息
    HTABLE_META_MAX(255);
    private int type;

    ObTableRpcMetaType(int type) {
        this.type = type;
    }

    public int getType() {
        return type;
    }
}

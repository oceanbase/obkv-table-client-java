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

package com.alipay.oceanbase.rpc.mutation;

import com.alipay.oceanbase.rpc.table.api.Table;

/*
 * Put impl.
 * Need fill all columns when use put impl.
 * Server will do override when use put impl.
 */
public class Put extends InsertOrUpdate {
    /*
     * default constructor
     */
    public Put() {
        super();
        super.usePut();
    }

    /*
     * construct with ObTableClient and String
     */
    public Put(Table client, String tableName) {
        super(client, tableName);
        super.usePut();
    }
}

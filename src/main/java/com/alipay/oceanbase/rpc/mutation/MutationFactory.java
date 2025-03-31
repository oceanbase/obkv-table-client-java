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

import com.alipay.oceanbase.rpc.ObClusterTableQuery;
import com.alipay.oceanbase.rpc.table.ObTableClientQueryImpl;
import com.alipay.oceanbase.rpc.table.api.TableQuery;

public class MutationFactory {
    public static ColumnValue colVal(String columnName, Object value) {
        return new ColumnValue(columnName, value);
    }

    public static Row row(ColumnValue... columnValue) {
        return new Row(columnValue);
    }

    public static TableQuery query() {
        ObTableClientQueryImpl tableQuery = new ObTableClientQueryImpl();
        return new ObClusterTableQuery(tableQuery);
    }

    public static Insert insert() {
        return new Insert();
    }

    public static Delete delete() {
        return new Delete();
    }

    public static Update update() {
        return new Update();
    }

    public static InsertOrUpdate insertOrUpdate() {
        return new InsertOrUpdate();
    }

    public static Put put() {
        return new Put();
    }

    public static Replace replace() {
        return new Replace();
    }

    public static Increment increment() {
        return new Increment();
    }

    public static Append append() {
        return new Append();
    }

}

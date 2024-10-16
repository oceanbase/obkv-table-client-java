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

package com.alipay.oceanbase.rpc.direct_load;

import com.alipay.oceanbase.rpc.direct_load.protocol.ObDirectLoadProtocolFactory;

public class ObDirectLoadManager {

    private static final ObDirectLoadConnectionFactory connectionFactory = new ObDirectLoadConnectionFactory();

    private ObDirectLoadManager() {
    }

    /**
     * 检查observer版本是否支持旁路导入
     * @param obVersion
     * @return {@code true} 支持 {@code false} 不支持
     */
    public static boolean checkIsSupported(long obVersion) {
        return ObDirectLoadProtocolFactory.checkIsSupported(obVersion);
    }

    public static ObDirectLoadConnection.Builder getConnectionBuilder() {
        return new ObDirectLoadConnection.Builder(connectionFactory);
    }

}

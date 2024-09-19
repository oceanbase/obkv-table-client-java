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

import com.alipay.oceanbase.rpc.direct_load.exception.*;

public class ObDirectLoadConnectionFactory {

    private static final ObDirectLoadLogger logger = ObDirectLoadLogger.getLogger();

    public ObDirectLoadConnection createConnection() throws ObDirectLoadException {
        ObDirectLoadConnection connection = new ObDirectLoadConnection(this);
        logger.debug("create connection, id:" + connection.getTraceId());
        return connection;
    }

    public void closeConnection(ObDirectLoadConnection connection) {
        if (connection == null) {
            return;
        }
        logger.debug("close connection, id:" + connection.getTraceId());
    }

    public ObDirectLoadConnection buildConnection(ObDirectLoadConnection.Builder builder)
                                                                                         throws ObDirectLoadException {
        ObDirectLoadConnection connection = null;
        try {
            connection = createConnection();
            connection.init(builder);
        } catch (Exception e) {
            logger.warn("build connection failed, args:" + builder, e);
            throw e;
        }
        return connection;
    }

}

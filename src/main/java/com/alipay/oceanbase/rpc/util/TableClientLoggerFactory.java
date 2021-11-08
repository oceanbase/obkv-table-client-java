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

package com.alipay.oceanbase.rpc.util;

import com.alipay.sofa.common.code.LogCode2Description;
import org.slf4j.Logger;
import com.alipay.sofa.common.log.LoggerSpaceManager;

public class TableClientLoggerFactory {

    public static final String        OCEANBASE_TABLE_CLIENT_LOGGER_SPACE = "oceanbase-table-client";
    public static LogCode2Description LCD                                 = LogCode2Description
                                                                              .create(OCEANBASE_TABLE_CLIENT_LOGGER_SPACE);

    public static Logger getLogger(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }

        return LoggerSpaceManager.getLoggerBySpace(name, OCEANBASE_TABLE_CLIENT_LOGGER_SPACE);
    }

    public static Logger getLogger(Class<?> klass) {
        if (klass == null) {
            return null;
        }

        return getLogger(klass.getCanonicalName());
    }
}

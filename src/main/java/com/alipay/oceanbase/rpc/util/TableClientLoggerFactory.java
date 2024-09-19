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
import com.alipay.sofa.common.log.MultiAppLoggerSpaceManager;
import com.alipay.sofa.common.log.adapter.level.AdapterLevel;
import org.slf4j.Logger;
import org.slf4j.helpers.NOPLogger;

import java.util.Map;

public class TableClientLoggerFactory {

    public static final String        OCEANBASE_TABLE_CLIENT_LOGGER_SPACE = "oceanbase-table-client";
    public static final String        OCEANBASE_TABLE_CLIENT_BOOT         = "OBKV-BOOT";
    public static final String        OCEANBASE_TABLE_CLIENT_MONITOR      = "OBKV-MONITOR";
    public static final String        OCEANBASE_TABLE_CLIENT_RUNTIME      = "OBKV-RUNTIME";
    public static final String        OCEANBASE_TABLE_CLIENT_DIRECT       = "OBKV-DIRECT";
    public static LogCode2Description LCD                                 = LogCode2Description
                                                                              .create(OCEANBASE_TABLE_CLIENT_LOGGER_SPACE);

    public static Logger              BOOT                                = NOPLogger.NOP_LOGGER;
    public static Logger              MONITOR                             = NOPLogger.NOP_LOGGER;
    public static Logger              RUNTIME                             = NOPLogger.NOP_LOGGER;
    public static Logger              DIRECT                              = NOPLogger.NOP_LOGGER;

    static {
        BOOT = getBootLogger();
        MONITOR = getMonitorLogger();
        RUNTIME = getRUNTIMELogger();
        DIRECT = getDIRECTLogger();
    }

    public static Logger getLogger(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }

        if (!MultiAppLoggerSpaceManager.isSpaceInitialized(OCEANBASE_TABLE_CLIENT_LOGGER_SPACE)) {
            //initLogger();
            MultiAppLoggerSpaceManager.init(OCEANBASE_TABLE_CLIENT_LOGGER_SPACE, null);
        }

        return MultiAppLoggerSpaceManager.getLoggerBySpace(name,
            OCEANBASE_TABLE_CLIENT_LOGGER_SPACE);
    }

    public static Logger getLogger(Class<?> klass) {
        if (klass == null) {
            return null;
        }

        return getLogger(klass.getCanonicalName());
    }

    public static Logger getBootLogger() {
        if (BOOT == NOPLogger.NOP_LOGGER) {
            BOOT = new WrappedLogger(getLogger(OCEANBASE_TABLE_CLIENT_BOOT));
        }

        return BOOT;
    }

    public static Logger getMonitorLogger() {
        if (MONITOR == NOPLogger.NOP_LOGGER) {
            MONITOR = new WrappedLogger(getLogger(OCEANBASE_TABLE_CLIENT_MONITOR));
        }

        return MONITOR;
    }

    public static Logger getRUNTIMELogger() {
        if (RUNTIME == NOPLogger.NOP_LOGGER) {
            RUNTIME = new WrappedLogger(getLogger(OCEANBASE_TABLE_CLIENT_RUNTIME));
        }

        return RUNTIME;
    }

    public static Logger getDIRECTLogger() {
        if (DIRECT == NOPLogger.NOP_LOGGER) {
            DIRECT = new WrappedLogger(getLogger(OCEANBASE_TABLE_CLIENT_DIRECT));
        }

        return DIRECT;
    }

    public static void changeLevel(Map<String, String> levelMap) {
        for (Map.Entry<String, String> entry : levelMap.entrySet()) {
            changeLevel(entry.getKey(), entry.getValue());
        }
    }

    public static void changeLevel(String name, String level) {
        MultiAppLoggerSpaceManager.setLoggerLevel(name, OCEANBASE_TABLE_CLIENT_LOGGER_SPACE,
            AdapterLevel.getAdapterLevel(level));
    }
}

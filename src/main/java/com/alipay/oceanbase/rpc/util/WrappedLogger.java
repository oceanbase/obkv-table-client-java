/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2022 OceanBase
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

import org.slf4j.Logger;
import org.slf4j.Marker;
import com.alipay.remoting.util.StringUtils;

/**
 * <p> Date: 2016-08-17 Time: 10:22 </p>
 *
 * @author jiyong.jy
 */
public class WrappedLogger implements Logger {

    private Logger logger;

    public WrappedLogger(Logger logger) {
        this.logger = logger;
    }

    private String getLoggerString(String s) {
        StringBuilder stringBuilder = new StringBuilder();
        String traceId = getTraceId();
        if (StringUtils.isNotBlank(traceId)) {
            stringBuilder.append(traceId).append("-");
        }
        stringBuilder.append(s);
        return stringBuilder.toString();
    }

    private String getTraceId() {
        String tracerId = "";
        return tracerId;
    }

    @Override
    public String getName() {
        return logger.getName();
    }

    @Override
    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    @Override
    public void trace(String s) {
        logger.trace(getLoggerString(s));
    }

    @Override
    public void trace(String s, Object o) {
        logger.trace(getLoggerString(s), o);
    }

    @Override
    public void trace(String s, Object o, Object o1) {
        logger.trace(getLoggerString(s), o, o1);
    }

    @Override
    public void trace(String s, Object[] objects) {
        logger.trace(getLoggerString(s), objects);
    }

    @Override
    public void trace(String s, Throwable throwable) {
        logger.trace(getLoggerString(s), throwable);
    }

    @Override
    public boolean isTraceEnabled(Marker marker) {
        return logger.isTraceEnabled(marker);
    }

    @Override
    public void trace(Marker marker, String s) {
        logger.trace(marker, s);
    }

    @Override
    public void trace(Marker marker, String s, Object o) {
        logger.trace(marker, s, o);
    }

    @Override
    public void trace(Marker marker, String s, Object o, Object o1) {
        logger.trace(marker, s, o, o1);
    }

    @Override
    public void trace(Marker marker, String s, Object[] objects) {
        logger.trace(marker, s, objects);
    }

    @Override
    public void trace(Marker marker, String s, Throwable throwable) {
        logger.trace(marker, s, throwable);
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug(String s) {
        logger.debug(getLoggerString(s));
    }

    @Override
    public void debug(String s, Object o) {
        if (logger.isDebugEnabled()) {
            logger.debug(getLoggerString(s), o);
        }
    }

    @Override
    public void debug(String s, Object o, Object o1) {
        if (logger.isDebugEnabled()) {
            logger.debug(getLoggerString(s), o, o1);
        }
    }

    @Override
    public void debug(String s, Object[] objects) {
        if (logger.isDebugEnabled()) {
            logger.debug(getLoggerString(s), objects);
        }
    }

    @Override
    public void debug(String s, Throwable throwable) {
        if (logger.isDebugEnabled()) {
            logger.debug(getLoggerString(s), throwable);
        }
    }

    @Override
    public boolean isDebugEnabled(Marker marker) {
        return logger.isDebugEnabled(marker);
    }

    @Override
    public void debug(Marker marker, String s) {
        logger.debug(marker, s);
    }

    @Override
    public void debug(Marker marker, String s, Object o) {
        logger.debug(marker, s, o);
    }

    @Override
    public void debug(Marker marker, String s, Object o, Object o1) {
        logger.debug(marker, s, o, o1);
    }

    @Override
    public void debug(Marker marker, String s, Object[] objects) {
        logger.debug(marker, s, objects);
    }

    @Override
    public void debug(Marker marker, String s, Throwable throwable) {
        logger.debug(marker, s, throwable);
    }

    @Override
    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    @Override
    public void info(String s) {
        if (logger.isInfoEnabled()) {
            logger.info(getLoggerString(s));
        }
    }

    @Override
    public void info(String s, Object o) {
        if (logger.isInfoEnabled()) {
            logger.info(getLoggerString(s), o);
        }
    }

    @Override
    public void info(String s, Object o, Object o1) {
        if (logger.isInfoEnabled()) {
            logger.info(getLoggerString(s), o, o1);
        }
    }

    @Override
    public void info(String s, Object[] objects) {
        if (logger.isInfoEnabled()) {
            logger.info(getLoggerString(s), objects);
        }
    }

    @Override
    public void info(String s, Throwable throwable) {
        if (logger.isInfoEnabled()) {
            logger.info(getLoggerString(s), throwable);
        }
    }

    @Override
    public boolean isInfoEnabled(Marker marker) {
        return logger.isInfoEnabled(marker);
    }

    @Override
    public void info(Marker marker, String s) {
        logger.info(marker, s);
    }

    @Override
    public void info(Marker marker, String s, Object o) {
        logger.info(marker, s, o);
    }

    @Override
    public void info(Marker marker, String s, Object o, Object o1) {
        logger.info(marker, s, o, o1);
    }

    @Override
    public void info(Marker marker, String s, Object[] objects) {
        logger.info(marker, s, objects);
    }

    @Override
    public void info(Marker marker, String s, Throwable throwable) {
        logger.info(marker, s, throwable);
    }

    @Override
    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    @Override
    public void warn(String s) {
        logger.warn(getLoggerString(s));
    }

    @Override
    public void warn(String s, Object o) {
        logger.warn(getLoggerString(s), o);
    }

    @Override
    public void warn(String s, Object[] objects) {
        logger.warn(getLoggerString(s), objects);
    }

    @Override
    public void warn(String s, Object o, Object o1) {
        logger.warn(getLoggerString(s), o, o1);
    }

    @Override
    public void warn(String s, Throwable throwable) {
        logger.warn(getLoggerString(s), throwable);
    }

    @Override
    public boolean isWarnEnabled(Marker marker) {
        return logger.isWarnEnabled(marker);
    }

    @Override
    public void warn(Marker marker, String s) {
        logger.warn(marker, s);
    }

    @Override
    public void warn(Marker marker, String s, Object o) {
        logger.warn(marker, s, o);
    }

    @Override
    public void warn(Marker marker, String s, Object o, Object o1) {
        logger.warn(marker, s, o, o1);
    }

    @Override
    public void warn(Marker marker, String s, Object[] objects) {
        logger.warn(marker, s, objects);
    }

    @Override
    public void warn(Marker marker, String s, Throwable throwable) {
        logger.warn(marker, s, throwable);
    }

    @Override
    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    @Override
    public void error(String s) {
        logger.error(getLoggerString(s));
    }

    @Override
    public void error(String s, Object o) {
        logger.error(getLoggerString(s), o);
    }

    @Override
    public void error(String s, Object o, Object o1) {
        logger.error(getLoggerString(s), o, o1);
    }

    @Override
    public void error(String s, Object[] objects) {
        logger.error(getLoggerString(s), objects);
    }

    @Override
    public void error(String s, Throwable throwable) {
        logger.error(getLoggerString(s), throwable);
    }

    @Override
    public boolean isErrorEnabled(Marker marker) {
        return logger.isErrorEnabled(marker);
    }

    @Override
    public void error(Marker marker, String s) {
        logger.error(marker, s);
    }

    @Override
    public void error(Marker marker, String s, Object o) {
        logger.error(marker, s, o);
    }

    @Override
    public void error(Marker marker, String s, Object o, Object o1) {
        logger.error(marker, s, o, o1);
    }

    @Override
    public void error(Marker marker, String s, Object[] objects) {
        logger.error(marker, s, objects);
    }

    @Override
    public void error(Marker marker, String s, Throwable throwable) {
        logger.error(marker, s, throwable);
    }
}
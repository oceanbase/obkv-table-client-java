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

import org.slf4j.Logger;

import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;

public class ObDirectLoadLogger {
    private static final Logger             logger         = TableClientLoggerFactory
                                                               .getDIRECTLogger();
    private final ObDirectLoadTraceId       traceId;

    private static final ObDirectLoadLogger DEFAULT_LOGGER = new ObDirectLoadLogger(
                                                               ObDirectLoadTraceId.DEFAULT_TRACE_ID);

    public static ObDirectLoadLogger getLogger() {
        return DEFAULT_LOGGER;
    }

    public static ObDirectLoadLogger getLogger(ObDirectLoadTraceId traceId) {
        return new ObDirectLoadLogger(traceId);
    }

    public ObDirectLoadLogger(ObDirectLoadTraceId traceId) {
        this.traceId = traceId;
    }

    private String formatString(String msg) {
        return String.format("[%s] %s", this.traceId, msg);
    }

    // trace

    public void trace(String msg) {
        logger.trace(formatString(msg));
    }

    public void trace(String format, Object arg) {
        logger.trace(formatString(format), arg);
    }

    public void trace(String format, Object arg1, Object arg2) {
        logger.trace(formatString(format), arg1, arg2);
    }

    public void trace(String format, Object... arguments) {
        logger.trace(formatString(format), arguments);
    }

    public void trace(String msg, Throwable t) {
        logger.trace(formatString(msg), t);
    }

    // debug

    public void debug(String msg) {
        logger.debug(formatString(msg));
    }

    public void debug(String format, Object arg) {
        logger.debug(formatString(format), arg);
    }

    public void debug(String format, Object arg1, Object arg2) {
        logger.debug(formatString(format), arg1, arg2);
    }

    public void debug(String format, Object... arguments) {
        logger.debug(formatString(format), arguments);
    }

    public void debug(String msg, Throwable t) {
        logger.debug(formatString(msg), t);
    }

    // info

    public void info(String msg) {
        logger.info(formatString(msg));
    }

    public void info(String format, Object arg) {
        logger.info(formatString(format), arg);
    }

    public void info(String format, Object arg1, Object arg2) {
        logger.info(formatString(format), arg1, arg2);
    }

    public void info(String format, Object... arguments) {
        logger.info(formatString(format), arguments);
    }

    public void info(String msg, Throwable t) {
        logger.info(formatString(msg), t);
    }

    // warn

    public void warn(String msg) {
        logger.warn(formatString(msg));
    }

    public void warn(String format, Object arg) {
        logger.warn(formatString(format), arg);
    }

    public void warn(String format, Object arg1, Object arg2) {
        logger.warn(formatString(format), arg1, arg2);
    }

    public void warn(String format, Object... arguments) {
        logger.warn(formatString(format), arguments);
    }

    public void warn(String msg, Throwable t) {
        logger.warn(formatString(msg), t);
    }

    // error

    public void error(String msg) {
        logger.error(formatString(msg));
    }

    public void error(String format, Object arg) {
        logger.error(formatString(format), arg);
    }

    public void error(String format, Object arg1, Object arg2) {
        logger.error(formatString(format), arg1, arg2);
    }

    public void error(String format, Object... arguments) {
        logger.error(formatString(format), arguments);
    }

    public void error(String msg, Throwable t) {
        logger.error(formatString(msg), t);
    }

}

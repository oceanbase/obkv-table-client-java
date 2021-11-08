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

import org.slf4j.Logger;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

public class TimeUtils {
    private static final Logger logger = TableClientLoggerFactory.getLogger(TimeUtils.class);

    // currentTimeMillis是通过gettimeofday来实现的,
    // 是当前时间距离1970/01/01 08:00:00的毫秒数。
    // 依赖时钟时，应该使用currentTimeMillis。
    public static long getCurrentTimeMs() {
        return System.currentTimeMillis();
    }

    /**
     * Get current time us.
     */
    public static long getCurrentTimeUs() {
        return System.currentTimeMillis() * 1000L;
    }

    // nanoTime算出来的是一个相对的时间，相对于系统启动的时候的时间，
    // 主要用于衡量一个时间段，例如，统计代码段执行时间等。
    public static long getNanoTimeUs() {
        return System.nanoTime() / 1000;
    }

    /**
     * Get nano time ns.
     */
    public static long getNanoTimeNs() {
        return System.nanoTime();
    }

    /**
     * Sleep.
     */
    public static void sleep(long timeMs) {
        try {
            Thread.sleep(timeMs);
        } catch (InterruptedException e) {
            logger.error(e.toString());
        }
    }

    /**
     * Format time us to date.
     */
    public static String formatTimeUsToDate(long timeUS) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timeUS / 1000);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(c.getTime());
    }

    /**
     * Format time ms to date.
     */
    public static String formatTimeMsToDate(long timeMS) {
        Calendar c = Calendar.getInstance();
        c.setTimeInMillis(timeMS);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(c.getTime());
    }

    /**
     * Get time.
     */
    public static String getTime(String format) {
        Calendar c = Calendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(c.getTime());
    }

    /**
     * Str to timestamp.
     */
    public static Timestamp strToTimestamp(String str) {
        Timestamp ts = null;
        try {
            ts = Timestamp.valueOf(str);
        } catch (IllegalArgumentException e) {
            // maybe str format is yyyymmdd
            logger.warn(
                String.format("fail to convert str to timestamp, str=%s, errMsg=}%s", str,
                    e.getMessage()), e);

            java.util.Date date = strToUtilDate(str);
            if (null != date) {
                ts = new Timestamp(date.getTime());
            }
        }
        return ts;
    }

    /**
     * Str to date.
     */
    public static Date strToDate(String str) {
        Date sqlDate = null;
        java.util.Date utilDate = strToUtilDate(str);
        if (utilDate != null) {
            sqlDate = new Date(utilDate.getTime());
        } else {
            // maybe str format is yyyy-[m]m-[d]d hh:mm:ss[.f...]
            Timestamp ts = strToTimestamp(str);
            if (ts != null) {
                sqlDate = new Date(ts.getTime());
            }
        }
        return sqlDate;
    }

    private static java.util.Date strToUtilDate(String str) {
        java.util.Date utilDate = null;
        str = str.trim();
        try {
            // get date format
            DateFormat dateFormat = null;
            switch (str.length()) {
                case 8:
                    dateFormat = new SimpleDateFormat("yyyyMMdd");
                    break;
                case 10:
                    dateFormat = new SimpleDateFormat("yyyy-MM-dd");
                    break;
                case 14:
                    dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
                    break;
                default:
                    break;
            }

            // format date
            if (dateFormat != null) {
                utilDate = dateFormat.parse(str);
            }
        } catch (ParseException e) {
            logger
                .warn(String.format("failed to cast obj, str=%s, cause=%s", str, e.getCause()), e);
        }
        return utilDate;
    }
}

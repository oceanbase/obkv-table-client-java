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

import com.alipay.common.tracer.util.LoadTestUtil;
import com.alipay.routeclient.UidRange;
import com.alipay.zoneclient.util.ZoneClientUtil;
import org.slf4j.Logger;

import java.util.*;

/**
 * {@see com.alipay.sofa.service.api.ZoneClientHolder}
*
* Created by hongweiyi on 17/10/2016.
*/
public class ZoneUtil {

    private static final Logger LOGGER             = TableClientLoggerFactory.getRUNTIMELogger();

    private static final String ENV_ZONE_NAME      = "com.alipay.ldc.zone";
    private static final String ENV_APP_IDC        = "ALIPAY_APP_IDC";
    private static final String ENV_APP_LOCATION   = "ALIPAY_APP_LOCATION";
    private static String       DEFAULT_ZONE_VALUE = "GZ00A";
    private static String       DEFAULT_IDC_VALUE  = "dev";

    private static String       currentZone        = DEFAULT_ZONE_VALUE;
    private static String       currentAppIdc      = DEFAULT_ZONE_VALUE;
    private static String       currentAppLocation = DEFAULT_ZONE_VALUE;

    // 是否是zone模式
    private static boolean      zoneMode           = false;

    static {
        currentZone = System.getProperty(ENV_ZONE_NAME);
        currentAppIdc = System.getProperty(ENV_APP_IDC);
        currentAppLocation = System.getProperty(ENV_APP_LOCATION);
        boolean inBlackZone = true;
        if (currentZone != null && !currentZone.isEmpty()) {
            currentZone = currentZone.toUpperCase();
            String rlistraw = System.getProperty("rzone.list");
            if (rlistraw == null || rlistraw.isEmpty()) {
                //3.2.4.0版本增加了g和c,i
                rlistraw = "r,dr,g,c,i";
            }
            String[] rlist = rlistraw.split("\\,|\\，");
            for (int i = 0; i < rlist.length; i++) {
                String rz = rlist[i];
                if (rz != null && !rz.isEmpty()) {
                    rz = rz.trim().toUpperCase();
                    if (currentZone.startsWith(rz)) {
                        inBlackZone = false;
                        break;
                    }
                }
            }
        } else {
            currentZone = DEFAULT_ZONE_VALUE;
        }
        currentZone = currentZone.toUpperCase();

        String zone_mode = System.getProperty("zmode");
        if (zone_mode != null && !zone_mode.isEmpty()) {
            if (zone_mode.trim().equalsIgnoreCase("true")) {
                zoneMode = true;
            }
        } else {
            if (!inBlackZone) {
                zoneMode = true;
            }
        }
    }

    /**
     * Is zone mode.
    */
    public static boolean isZoneMode() {
        return zoneMode;
    }

    /**
     * 严格匹配是否是当前 zone
    * @param zone 目标 zone
    * @return 是否是当前zone
    */
    public static boolean isCurrentZone(String zone) {
        if (StringUtil.isEmpty(zone)) {
            return false;
        }

        // GZ00 GZ00A GZ00B
        zone = zone.toUpperCase();

        // zone 可能是不区分 A/B 的，如 GZ00，这种情况下，表示一套 DB，A/B 应用都会访问
        if ((currentZone.startsWith(zone) && currentZone.length() - 1 == zone.length()) // GZ00A.startsWith(GZ00)
            || currentZone.equals(zone)) { // GZ00A.equals(GZ00A)
            return true;
        } else {
            return false;
        }
    }

    /**
     * Is in current uid range.
    */
    public static boolean isInCurrentUidRange(int uid) {
        List<UidRange> uidRanges = ZoneClientUtil.getCurrZoneUidRange();
        for (UidRange uidRange : uidRanges) {
            if (uidRange.inRange(uid)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Get current zone.
    */
    public static String getCurrentZone() {
        return currentZone;
    }

    /**
     * Get current IDC.
    */
    public static String getCurrentIDC() {
        try {
            return ZoneClientUtil.getIdcByZone(currentZone);
        } catch (Throwable t) {
            LOGGER.warn("getIdcByZone failed", t);
        }
        return DEFAULT_IDC_VALUE;
    }

    public static Set<String> getCurrentIDCs() {
        Set<String> currentIDCs = new HashSet<String>();
        currentIDCs.add(getCurrentIDC());
        if (currentAppIdc != null && !currentAppIdc.isEmpty()) {
            currentIDCs.add(currentAppIdc.toUpperCase());
        }
        if (currentAppLocation != null && !currentAppLocation.isEmpty()) {
            currentIDCs.add(currentAppLocation.toUpperCase());
        }

        return currentIDCs;
    }

    public static void setCurrentAppIdc(String appIdc) {
        currentAppIdc = appIdc;
    }

    /**
     * @return 当前环境是否弹性机房
    */
    public static boolean isElastic() {
        if (isZoneMode()) {
            try {
                return ZoneClientUtil.queryZoneInfo().isElastic();
            } catch (Throwable t) {
                LOGGER.warn("queryZoneInfo().isElastic failed", t);
                return false;
            }
        }
        return false;
    }

    /**
     * @return 指定的 UID 是否是弹出状态
    */
    public static boolean isElasticUid(String uid) {
        if (isZoneMode()) {
            try {
                return ZoneClientUtil.isElastic(uid);
            } catch (Throwable t) {
                LOGGER.warn("ZoneClientUtil.isElastic(uid) failed", t);
                return false;
            }
        }
        return false;
    }

    private final static List<String> EMPTY_ARRAY = new ArrayList<String>(0);

    /**
     * @param isMark 是否是压测标识
    * @return 获得当前弹性位
    */
    public static List<String> getElasticValues(boolean isMark) {
        if (isZoneMode()) {
            try {
                List<String> ret = ZoneClientUtil.getElasticValues(isMark);
                return ret == null ? EMPTY_ARRAY : ret;
            } catch (Throwable t) {
                LOGGER.warn("getElasticValues() failed", t);
                return EMPTY_ARRAY;
            }
        }

        return EMPTY_ARRAY;
    }

    /**
     * @return 获得弹性位列表
    */
    public static List<String> getElasticList() {
        if (!isZoneMode()) {
            return new ArrayList<String>();
        }

        try {
            return ZoneClientUtil.getElasticValues(LoadTestUtil.isLoadTestMode());
        } catch (Throwable t) {
            LOGGER.warn("ZdalZoneUtil getElasticList meet exception: " + t.getMessage());
            return new ArrayList<String>();
        }
    }

}

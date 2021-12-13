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

package com.alipay.oceanbase.rpc.location.model;

import com.alipay.oceanbase.rpc.util.StringUtil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 * ObServerLdcLocation organizes server by IDC->Region order.
 *
 */
public class ObServerLdcLocation {
    enum RegionMatchType {
        MATCHED_BY_IDC, MATCHED_BY_ZONE_PREFIX, MATCHED_BY_URL
    }

    private String                           currentIDC;
    // 在某些OB部署场景，一个 IDC 可能归属到多个 Region.
    private Set<String>                      regionNames = new HashSet<String>();
    private RegionMatchType                  matchType;
    private boolean                          isLdcUsed;

    private List<ObServerLdcItem>            allServers;
    private HashMap<String, ObServerLdcItem> sameIDC     = new HashMap<String, ObServerLdcItem>();
    private HashMap<String, ObServerLdcItem> sameRegion  = new HashMap<String, ObServerLdcItem>();
    private HashMap<String, ObServerLdcItem> otherRegion = new HashMap<String, ObServerLdcItem>();

    /*
     * 构造 ObServerLdcLocation，根据当前 IDC 和 RegionMap 将服务器按 IDC->Region->OtherRegion的分类整理。
     *
     * @param allServers
     * @param currentIDC
     * @param regionFromOcp
     */
    public static ObServerLdcLocation buildLdcLocation(List<ObServerLdcItem> allServers,
                                                       String currentIDC, String regionFromOcp) {
        ObServerLdcLocation loc = new ObServerLdcLocation();
        loc.allServers = allServers;
        loc.currentIDC = currentIDC;

        if (StringUtil.isEmpty(currentIDC)) {
            loc.isLdcUsed = false;
        } else {
            // get Region names, refer to odp: ObLDCLocation::get_region_name
            // 1. 首先看 server 的 idc 是否匹配
            for (ObServerLdcItem ss : allServers) {
                if (ss.getIdc().equalsIgnoreCase(currentIDC)) {
                    loc.regionNames.add(ss.getRegion());
                    loc.matchType = RegionMatchType.MATCHED_BY_IDC;
                }
            }
            // 2. 若从 server idc 没匹配，则根据 zone 的前缀来匹配: case sensitive
            if (loc.regionNames.isEmpty()) {
                for (ObServerLdcItem ss : allServers) {
                    if (ss.getZone().startsWith(currentIDC)) {
                        loc.regionNames.add(ss.getRegion());
                        loc.matchType = RegionMatchType.MATCHED_BY_ZONE_PREFIX;
                    }
                }
            }
            // 3. 再次从 ocp 找 idc-> region 映射
            if (loc.regionNames.isEmpty() && StringUtil.isNotEmpty(regionFromOcp)) {
                loc.regionNames.add(regionFromOcp);
                loc.matchType = RegionMatchType.MATCHED_BY_URL;
            }

            if (!loc.regionNames.isEmpty()) {
                loc.isLdcUsed = true;
            }
        }

        if (loc.isLdcUsed) {
            // classify by idc->region->other
            for (ObServerLdcItem it : allServers) {
                if (it.getIdc().equalsIgnoreCase(currentIDC)) {
                    loc.sameIDC.put(it.getIp(), it);
                } else if (loc.regionNames.contains(it.getRegion())) {
                    loc.sameRegion.put(it.getIp(), it);
                } else {
                    loc.otherRegion.put(it.getIp(), it);
                }
            }
        }
        return loc;
    }

    /*
     * Whether the server in the same IDC.
     */
    public boolean inSameIDC(String svr_ip) {
        return sameIDC.containsKey(svr_ip);
    }

    /*
     * Whether the server in the same region.
     */
    public boolean inSameRegion(String ip) {
        return sameRegion.containsKey(ip);
    }

    /*
     * Whether the server in other region.
     */
    public boolean inOtherRegion(String ip) {
        return otherRegion.containsKey(ip);
    }

    /*
     * Whether the LDC routing is enabled.
     */
    public boolean isLdcUsed() {
        return isLdcUsed;
    }

    /*
     * Get current IDC.
     */
    public String getCurrentIDC() {
        return currentIDC;
    }

    /*
     * Get Region Match Type.
     */
    public RegionMatchType getMatchType() {
        return matchType;
    }

    /*
     * To String
     *
     * @return
     */
    @Override
    public String toString() {
        return "ObServerLdcLocation{" + "currentIDC='" + currentIDC + '\'' + ", isLdcUsed="
               + isLdcUsed + ", regionNames=" + regionNames + ", matchType=" + matchType
               + ", allServers=" + allServers + ", sameIDC=" + sameIDC + ", sameRegion="
               + sameRegion + ", otherRegion=" + otherRegion + '}';
    }
}

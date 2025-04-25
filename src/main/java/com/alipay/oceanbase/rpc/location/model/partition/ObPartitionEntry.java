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

package com.alipay.oceanbase.rpc.location.model.partition;

import com.alipay.oceanbase.rpc.location.model.ObServerLdcLocation;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.getLogger;


public class ObPartitionEntry {
    private static final Logger                              logger               = getLogger(ObPartitionEntry.class);

    private volatile  Long                                   lastRefreshAllTime   = 0L;

    private Map<Long, ObPartitionLocation>                   partitionLocation    = new HashMap<Long, ObPartitionLocation>();

    // mapping from tablet id to ls id, and the part id to tablet id mapping is in ObPartitionInfo
    private Map<Long, Long>                                  tabletLsIdMap        = new HashMap<>();
    
    // tabelt id -> (PartitionLocation, LsId)
    private ConcurrentHashMap<Long, ObPartitionLocationInfo> partitionInfos       = new ConcurrentHashMap<>();


    public ObPartitionLocationInfo getPartitionInfo(long tabletId) {
        return partitionInfos.computeIfAbsent(tabletId, id -> new ObPartitionLocationInfo());
    }

    public void removeNonExistentTablet(Map<Long, Long> partTabletMap) {
        List<Long> tablets = new ArrayList<>();
        for (Map.Entry<Long, ObPartitionLocationInfo> entry : partitionInfos.entrySet()) {
            if (!partTabletMap.containsValue(entry.getKey())) {
                tablets.add(entry.getKey());
                partitionInfos.remove(entry.getKey());
                tabletLsIdMap.remove(entry.getKey());
            }
        }
    }

    public void removePartitionLocationInfoByLsId(long lsId) {
        int cnt = 0;
        for (Map.Entry<Long, ObPartitionLocationInfo> entry : partitionInfos.entrySet()) {
            if (entry.getValue().getTabletLsId() == lsId) {
                ++cnt;
                partitionInfos.remove(entry.getKey());
            }
        }
    }

    public long getLastRefreshAllTime() {
        return lastRefreshAllTime;
    }

    public void setLastRefreshAllTime(long time) {
        lastRefreshAllTime = time;
    }
    
    public Map<Long, ObPartitionLocation> getPartitionLocation() {
        return partitionLocation;
    }

    /*
     * Set partition location.
     */
    public void setPartitionLocation(Map<Long, ObPartitionLocation> partitionLocation) {
        this.partitionLocation = partitionLocation;
    }

    public Map<Long, Long> getTabletLsIdMap() {
        return tabletLsIdMap;
    }

    public void setTabletLsIdMap(Map<Long, Long> tabletLsIdMap) {
        this.tabletLsIdMap = tabletLsIdMap;
    }

    public long getLsId(long tabletId) { return tabletLsIdMap.get(tabletId); }
    
    /*
     * Get partition location with part id.
     */
    public ObPartitionLocation getPartitionLocationWithPartId(long partId) {
        return partitionLocation.get(partId);
    }

    /*
     * Get partition location with tablet id.
     */
    public ObPartitionLocation getPartitionLocationWithTabletId(long tabletId) {
        return partitionLocation.get(tabletId);
    }

    /*
     * Put partition location with part id.
     */
    public ObPartitionLocation putPartitionLocationWithPartId(long partId,
                                                              ObPartitionLocation ObpartitionLocation) {
        return partitionLocation.put(partId, ObpartitionLocation);
    }

    /*
     * Put partition location with part id.
     */
    public ObPartitionLocation putPartitionLocationWithTabletId(long tabletId,
                                                                ObPartitionLocation ObpartitionLocation) {
        return partitionLocation.put(tabletId, ObpartitionLocation);
    }

    /*
     * Prepare for weak read.
     * @param ldcLocation
     */
    public void prepareForWeakRead(ObServerLdcLocation ldcLocation) {
        for (Map.Entry<Long, ObPartitionLocation> entry : partitionLocation.entrySet()) {
            entry.getValue().prepareForWeakRead(ldcLocation);
        }
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "ObPartitionEntry{" + "partitionLocation=" + partitionLocation + '}';
    }
}

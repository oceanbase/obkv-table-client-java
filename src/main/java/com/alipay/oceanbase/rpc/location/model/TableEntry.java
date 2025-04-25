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

import com.alipay.oceanbase.rpc.location.model.partition.ObPartIdCalculator;
import com.alipay.oceanbase.rpc.location.model.partition.ObPartitionEntry;
import com.alipay.oceanbase.rpc.location.model.partition.ObPartitionInfo;
import com.alipay.oceanbase.rpc.location.model.partition.ObPartitionLevel;
import com.alipay.oceanbase.rpc.protocol.payload.Constants;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class TableEntry {

    public static final Map<String, Integer> HBASE_ROW_KEY_ELEMENT = new LinkedHashMap<String, Integer>() {
                                                                       {
                                                                           put("K", 0);
                                                                           put("Q", 1);
                                                                           put("T", 2);
                                                                       }
                                                                   };

    // schema info
    private Long                             tableId               = Constants.OB_INVALID_ID;
    private Long                             partitionNum          = Constants.OB_INVALID_ID; //for dummy entry, it is one
    private Long                             replicaNum            = Constants.OB_INVALID_ID;
    private Long                             schemaVersion         = Constants.OB_INVALID_ID; // schema_version, to ensure atomicity of meta information in version above 4352
    private ObPartitionInfo                  partitionInfo         = null;
    // this create time is the creation time of this odp tableEntry in odp server
    private volatile long                    odpMetaCreateTimeMills;
    private volatile long                    refreshMetaTimeMills;
    private volatile long                    refreshPartLocationTimeMills;
    private Map<String, Integer>             rowKeyElement         = null;

    // table location
    private TableLocation                    tableLocation         = null;
    // partition location
    private TableEntryKey                    tableEntryKey         = null;
    private volatile ObPartitionEntry        partitionEntry        = null;

    /*
     * Is valid.
     */
    public boolean isValid() {
        return this.partitionNum > 0 && this.replicaNum > 0 && tableId > 0
               && ((null != tableLocation) && (tableLocation.getReplicaLocations().size() > 0)); // tableLocation.getReplicaLocations().size() 不一定等于 replicaNum，多个 partition 就有多个 replicaNum

    }

    /*
     * Get table id.
     */
    public Long getTableId() {
        return tableId;
    }

    /*
     * Set table id.
     */
    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    /*
     * Get partition num.
     */
    public Long getPartitionNum() {
        return partitionNum;
    }

    /*
     * Get schema version
     * */
    public Long getSchemaVersion() {
        return schemaVersion;
    }

    /*
     * Set partition num.
     */
    public void setPartitionNum(Long partitionNum) {
        this.partitionNum = partitionNum;
    }

    /*
    * Set schema version
    * */
    public void setSchemaVersion(Long schemaVersion) {
        this.schemaVersion = schemaVersion;
    }

    /*
     * Get replica num.
     */
    public Long getReplicaNum() {
        return replicaNum;
    }

    /*
     * Is partition table.
     */
    public boolean isPartitionTable() {
        return partitionNum > 1
               || (partitionInfo != null
                   && partitionInfo.getLevel().getIndex() > ObPartitionLevel.LEVEL_ZERO.getIndex() && partitionInfo
                   .getLevel().getIndex() < ObPartitionLevel.UNKNOWN.getIndex());
    }

    /*
     * Set replica num.
     */
    public void setReplicaNum(Long replicaNum) {
        this.replicaNum = replicaNum;
    }

    /*
     * Get table location.
     */
    public TableLocation getTableLocation() {
        return tableLocation;
    }

    /*
     * Set table location.
     */
    public void setTableLocation(TableLocation tableLocation) {
        this.tableLocation = tableLocation;
    }

    /*
     * Get partition info.
     */
    public ObPartitionInfo getPartitionInfo() {
        return partitionInfo;
    }

    /*
     * Set partition info.
     */
    public void setPartitionInfo(ObPartitionInfo partitionInfo) {
        this.partitionInfo = partitionInfo;
    }

    /*
     * Get refresh time mills.
     */
    public long getRefreshMetaTimeMills() {
        return refreshMetaTimeMills;
    }

    /*
     * Get refresh time mills.
     */
    public long getRefreshPartLocationTimeMills() {
        return refreshPartLocationTimeMills;
    }

    /*
     * Get odp creation time mills.
     */
    public long getODPMetaCreateTimeMills() {
        return odpMetaCreateTimeMills;
    }

    /*
     * Set refresh time mills.
     */
    public void setRefreshMetaTimeMills(long refreshMetaTimeMills) {
        this.refreshMetaTimeMills = refreshMetaTimeMills;
    }

    /*
     * Set refresh all time mills.
     */
    public void setRefreshPartLocationTimeMills(long refreshPartLocationTimeMills) {
        this.refreshPartLocationTimeMills = refreshPartLocationTimeMills;
    }

    /*
     * Set odp creation time mills.
     */
    public void setODPMetaCreateTimeMills(long odpMetaCreateTimeMills) {
        this.odpMetaCreateTimeMills = odpMetaCreateTimeMills;
    }

    public Map<String, Integer> getRowKeyElement() {
        return rowKeyElement;
    }

    /*
     * Set row key element.
     */
    public void setRowKeyElement(Map<String, Integer> rowKeyElement) {
        this.rowKeyElement = rowKeyElement;
        if (partitionInfo != null) {
            partitionInfo.setRowKeyElement(rowKeyElement);
        }
    }

    /*
     * Get table entry key.
     */
    public TableEntryKey getTableEntryKey() {
        return tableEntryKey;
    }

    /*
     * Set table entry key.
     */
    public void setTableEntryKey(TableEntryKey tableEntryKey) {
        this.tableEntryKey = tableEntryKey;
    }

    /*
     * Get partition entry.
     */
    public ObPartitionEntry getPartitionEntry() {
        return partitionEntry;
    }

    /*
     * Set partition entry.
     */
    public void setPartitionEntry(ObPartitionEntry partitionEntry) {
        this.partitionEntry = partitionEntry;
    }

    /*
     * Prepare.
     */
    public void prepare() throws IllegalArgumentException {
        if (isPartitionTable()) {
            checkArgument(partitionInfo != null, "partition table partition info is not ready. key"
                                                 + tableEntryKey);
            partitionInfo.prepare();
        }
    }

    /*
     * Get PartIdx from partId(logicId, partition id in 3.x)
     */
    public long getPartIdx(long partId) {
        long partIdx = partId;
        if (this.getPartitionInfo() != null
            && this.getPartitionInfo().getLevel() == ObPartitionLevel.LEVEL_TWO) {
            partIdx = ObPartIdCalculator.getPartIdx(partId, this.getPartitionInfo()
                .getSubPartDesc().getPartNum());
        }
        return partIdx;
    }

    /*
     * Prepare for weak read.
     * @param ldcLocation
     */
    public void prepareForWeakRead(ObServerLdcLocation ldcLocation) {
        if (partitionEntry != null) {
            partitionEntry.prepareForWeakRead(ldcLocation);
        }
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return "TableEntry{" + "tableId=" + tableId + ", partitionNum=" + partitionNum
               + ", replicaNum=" + replicaNum + ", partitionInfo=" + partitionInfo
               + ", refreshMetaTimeMills=" + refreshMetaTimeMills
               + ", refreshPartLocationTimeMills=" + refreshPartLocationTimeMills
               + ", rowKeyElement=" + rowKeyElement + ", tableLocation=" + tableLocation
               + ", tableEntryKey=" + tableEntryKey + ", partitionEntry=" + partitionEntry + '}';
    }
}

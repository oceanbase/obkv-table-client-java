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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class ObPartitionInfo {
    private ObPartitionLevel     level           = ObPartitionLevel.UNKNOWN;
    private ObPartDesc           firstPartDesc   = null;
    private ObPartDesc           subPartDesc     = null;
    private List<ObColumn>       partColumns     = new ArrayList<ObColumn>(1);
    // mapping from part id to tablet id, and the tablet id to ls id mapping is in ObPartitionInfo
    private Map<Long, Long>      partTabletIdMap = null;
    private Map<String, Long>    partNameIdMap   = null;
    private Map<String, Integer> rowKeyElement   = null;

    /*
     * Get level.
     */
    public ObPartitionLevel getLevel() {
        return level;
    }

    /*
     * Set level.
     */
    public void setLevel(ObPartitionLevel level) {
        this.level = level;
    }

    /*
     * Get first part desc.
     */
    public ObPartDesc getFirstPartDesc() {
        return firstPartDesc;
    }

    /*
     * Set first part desc.
     */
    public void setFirstPartDesc(ObPartDesc firstPartDesc) {
        this.firstPartDesc = firstPartDesc;
    }

    /*
     * Get sub part desc.
     */
    public ObPartDesc getSubPartDesc() {
        return subPartDesc;
    }

    /*
     * Set sub part desc.
     */
    public void setSubPartDesc(ObPartDesc subPartDesc) {
        this.subPartDesc = subPartDesc;
    }

    /*
     * Get part columns.
     */
    public List<ObColumn> getPartColumns() {
        return partColumns;
    }

    /*
     * Add column.
     */
    public void addColumn(ObColumn column) {
        this.partColumns.add(column);
    }

    public Map<String, Integer> getRowKeyElement() {
        return rowKeyElement;
    }

    /*
     * Set row key element.
     */
    public void setRowKeyElement(Map<String, Integer> rowKeyElement) {
        this.rowKeyElement = rowKeyElement;
        if (firstPartDesc != null) {
            firstPartDesc.setRowKeyElement(rowKeyElement);
        }
        if (subPartDesc != null) {
            subPartDesc.setRowKeyElement(rowKeyElement);
        }
    }

    /*
     * Prepare.
     */
    public void prepare() throws IllegalArgumentException {

        checkArgument(level != ObPartitionLevel.UNKNOWN, "unknown partition level");

        if (level.getIndex() >= ObPartitionLevel.LEVEL_ONE.getIndex()) {
            checkArgument(firstPartDesc != null,
                "firstPartDesc can not be null when level above level one");
            firstPartDesc.prepare();
        }

        if (level.getIndex() == ObPartitionLevel.LEVEL_TWO.getIndex()) {
            checkArgument(subPartDesc != null,
                "subPartDesc can not be null when level is level two");
            subPartDesc.prepare();
        }
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("ObPartitionInfo{");
        sb.append("level=").append(level);
        sb.append(", firstPartDesc=").append(firstPartDesc);
        sb.append(", subPartDesc=").append(subPartDesc);
        sb.append(", partColumns=").append(partColumns);
        sb.append('}');
        return sb.toString();
    }

    public Map<String, Long> getPartNameIdMap() {
        return this.partNameIdMap;
    }

    /*
     * Set part name id map.
     */
    public void setPartNameIdMap(Map<String, Long> partNameIdMap) {
        this.partNameIdMap = partNameIdMap;
    }

    public Map<Long, Long> getPartTabletIdMap() {
        return this.partTabletIdMap;
    }

    /*
     * Set part tablet id map.
     */
    public void setPartTabletIdMap(Map<Long, Long> partTabletIdMap) {
        this.partTabletIdMap = partTabletIdMap;
    }
}

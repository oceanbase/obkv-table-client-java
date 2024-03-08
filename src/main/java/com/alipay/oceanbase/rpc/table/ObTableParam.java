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

package com.alipay.oceanbase.rpc.table;

import com.alipay.oceanbase.rpc.ObTableClient;

import static com.alipay.oceanbase.rpc.protocol.payload.Constants.INVALID_TABLET_ID;
import static com.alipay.oceanbase.rpc.protocol.payload.Constants.OB_INVALID_ID;
import static com.alipay.oceanbase.rpc.protocol.payload.Constants.INVALID_LS_ID;

public class ObTableParam {
    private ObTable obTable;
    private long    tableId     = OB_INVALID_ID;
    private long    partitionId = INVALID_TABLET_ID; // partition id in 3.x aka tablet id in 4.x
    private long    partId      = INVALID_TABLET_ID; // logicId, partition id in 3.x, can be used when retry
    private long    lsId        = INVALID_LS_ID;

    /*
     * constructor
     */
    public ObTableParam(ObTable obTable, long tableId, long partitionId) {
        this.obTable = obTable;
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    /*
     * constructor
     */
    public ObTableParam(ObTable obTable) {
        this.obTable = obTable;
    }

    /*
     * Get ObTable.
     */
    public ObTable getObTable() {
        return obTable;
    }

    /*
     * Set ObTable.
     */
    public void setObTable(ObTable obTable) {
        this.obTable = obTable;
    }

    /*
     * Get tableId.
     */
    public long getTableId() {
        return tableId;
    }

    /*
     * Set tableId.
     */
    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    /*
     * Get tabletId.
     */
    public long getTabletId() {
        return partitionId;
    }

    /*
     * Set tabletId. partition ID is the same as tablet ID.
     */
    public void setTabletId(long tabletId) {
        this.partitionId = tabletId;
    }

    /*
     * Get partitionId.
     */
    public long getPartitionId() {
        return partitionId;
    }

    /*
     * Set partitionId.
     */
    public void setPartitionId(long partitionId) {
        this.partitionId = partitionId;
    }

    /*
     * Get partId (partition id in 3.x, originPartId in 4.x)
     */
    public long getPartId() {
        return this.partId;
    }

    /*
     * Set partId
     */
    public void setPartId(long partId) {
        this.partId = partId;
    }

    /*
     * Set lsId
     */
    public long getLsId() {
        return lsId;
    }

    /*
     * Get lsId
     */
    public void setLsId(long lsId) {
        this.lsId = lsId;
    }

}

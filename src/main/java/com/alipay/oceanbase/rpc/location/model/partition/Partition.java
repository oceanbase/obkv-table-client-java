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

package com.alipay.oceanbase.rpc.location.model.partition;

import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;

public class Partition {
    private final long partitionId;                      // partition id in 3.x aka tablet id in 4.x
    private final long partId;                           // logic id
    private long       tableId = Constants.OB_INVALID_ID;
    private String     ip      = "";
    private int        port    = -1;
    private long       lsId    = Constants.INVALID_LS_ID;

    public Partition(Long partitionId, Long partId) {

        this.partitionId = partitionId;
        this.partId = partId;
    }

    public Partition(long partitionId, long partId, long tableId, String ip, int port, long lsId) {
        this.partitionId = partitionId;
        this.partId = partId;
        this.tableId = tableId;
        this.ip = ip;
        this.port = port;
        this.lsId = lsId;
    }

    public Partition start() {
        return this;
    }

    public Partition end() {
        return this;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setTableId(Long tableId) {
        this.tableId = tableId;
    }

    public void setLsId(long lsId) {
        this.lsId = lsId;
    }

    public Long getPartitionId() {
        return this.partitionId;
    }

    public Long getPartId() {
        return this.partId;
    }

    public Long getTableId() {
        return this.tableId;
    }

    public String getIp() {
        return this.ip;
    }

    public int getPort() {
        return this.port;
    }

    public long getLsId() {
        return this.lsId;
    }

    public ObObj getStart() {
        return ObObj.getMin();
    }

    public ObObj getEnd() {
        return ObObj.getMax();
    }

    public String toString() {
        String msg = "The Partition info: partition_id = " + partitionId + ", part_id = " + partId
                + ", table_id = " + tableId + ", ls_id = " + lsId + ", ip = " + ip + ", port = " + port;
        return msg;
    }
}

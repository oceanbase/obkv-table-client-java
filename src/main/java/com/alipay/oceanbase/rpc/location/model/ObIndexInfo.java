/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2023 OceanBase
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

import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObIndexType;

public class ObIndexInfo {
    private Long        dataTableId  = Constants.OB_INVALID_ID;
    private Long        indexTableId = Constants.OB_INVALID_ID;
    private String      indexTableName;
    private ObIndexType indexType;

    public ObIndexInfo() {
        this.dataTableId = Constants.OB_INVALID_ID;
        this.indexTableId = Constants.OB_INVALID_ID;
        this.indexTableName = "";
        this.indexType = ObIndexType.IndexTypeIsNot;
    }

    public Long getDataTableId() {
        return dataTableId;
    }

    public void setDataTableId(Long dataTableId) {
        this.dataTableId = dataTableId;
    }

    public Long getIndexTableId() {
        return indexTableId;
    }

    public void setIndexTableId(Long indexTableId) {
        this.indexTableId = indexTableId;
    }

    public String getIndexTableName() {
        return indexTableName;
    }

    public void setIndexTableName(String indexTableName) {
        this.indexTableName = indexTableName;
    }

    public ObIndexType getIndexType() {
        return indexType;
    }

    public void setIndexType(ObIndexType indexType) {
        this.indexType = indexType;
    }

    @Override
    public String toString() {
        return "ObIndexInfo{" + "dataTableId=" + dataTableId + ", indexTableId=" + indexTableId
                + ", indexTableName=" + indexTableName + ", indexType=" + indexType + '}';
    }
}

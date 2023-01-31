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

package com.alipay.oceanbase.rpc.hbase;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableAbstractOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableEntityType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHTableFilter;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQuery;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObTableQueryRequest;

public class ObHTableOperationRequest {

    private static final String[] V_COLUMNS                  = new String[] { "V" };

    private static final String   TARGET_TABLE_NAME_TEMPLATE = "%s$%s";
    /**
     * create table {tableName}${familyName} (
     * K varbinary(1024),
     * Q varbinary(256),
     * T bigint,
     * V varbinary(1024) NOT NULL,
     * primary key(K, Q, T))
     * partition by key(K) partitions 16;
     */
    private String                tableName;                                        // should be visible character
    private String                familyName;                                       // should be visible character
    private byte[]                rowKey;                                           // K
    private byte[]                qualifierName;                                    // Q
    private long                  timestamp                  = -1;                  // T
    private byte[]                value;                                            // V

    private ObTableOperationType  operationType;

    public String getTargetTableName() {
        return String.format(TARGET_TABLE_NAME_TEMPLATE, tableName, familyName);
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFamilyName() {
        return familyName;
    }

    public void setFamilyName(String familyName) {
        this.familyName = familyName;
    }

    public byte[] getRowKey() {
        return rowKey;
    }

    public void setRowKey(byte[] rowKey) {
        this.rowKey = rowKey;
    }

    public byte[] getQualifierName() {
        return qualifierName;
    }

    public void setQualifierName(byte[] qualifierName) {
        this.qualifierName = qualifierName;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public ObTableOperationType getOperationType() {
        return operationType;
    }

    public void setOperationType(ObTableOperationType operationType) {
        this.operationType = operationType;
    }

    public ObTableAbstractOperationRequest obTableOperationRequest() {
        // TODO 这个类仅作为测试用，到时候要移到独立的 JAR 包内
        ObTableAbstractOperationRequest request = null;
        if (timestamp == -1) { // TODO 服务端暂时没有做自动生成时间戳，所以用了客户端的
            timestamp = System.currentTimeMillis();
        }
        switch (operationType) {
            case INSERT:
                request = ObTableOperationRequest.getInstance(getTargetTableName(),
                    ObTableOperationType.INSERT, new Object[] { rowKey, qualifierName, timestamp },
                    V_COLUMNS, new Object[] { value }, 10 * 1000);
                break;
            case DEL:
                request = ObTableOperationRequest.getInstance(getTargetTableName(),
                    ObTableOperationType.DEL, new Object[] { rowKey, qualifierName, timestamp },
                    null, null, 10 * 1000);
                break;
            case GET:
                request = new ObTableQueryRequest();
                ObTableQuery obTableQuery = new ObTableQuery();
                obTableQuery.setIndexName("PRIMARY");
                ObHTableFilter filter = new ObHTableFilter();
                filter.addSelectColumnQualifier("qualifierName");
                obTableQuery.sethTableFilter(filter);

                ObNewRange obNewRange = new ObNewRange();
                obNewRange
                    .setStartKey(ObRowKey.getInstance(rowKey, ObObj.getMin(), ObObj.getMin()));
                obNewRange.setEndKey(ObRowKey.getInstance(rowKey, ObObj.getMax(), ObObj.getMax()));
                obTableQuery.addKeyRange(obNewRange);
                obTableQuery.addSelectColumn("K");
                obTableQuery.addSelectColumn("Q");
                obTableQuery.addSelectColumn("T");
                obTableQuery.addSelectColumn("V");

                request.setTableName(getTargetTableName());
                ((ObTableQueryRequest) request).setTableQuery(obTableQuery);
                ((ObTableQueryRequest) request).setPartitionId(0);

                break;
            default:
                throw new RuntimeException("operationType invalid: " + operationType);
        }

        request.setEntityType(ObTableEntityType.HKV);

        return request;

    }

}

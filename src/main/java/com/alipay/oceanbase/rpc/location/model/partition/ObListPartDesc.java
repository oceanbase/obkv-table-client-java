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

import com.alipay.oceanbase.rpc.exception.ObTablePartitionConsistentException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.util.RandomUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

public class ObListPartDesc extends ObPartDesc {
    private static final Logger       logger              = TableClientLoggerFactory
                                                              .getLogger(ObListPartDesc.class);

    private List<ObColumn>            orderCompareColumns = null;
    private Map<ObPartitionKey, Long> sets                = null;

    /**
     * Ob list part desc.
     */
    public ObListPartDesc() {
        this.setPartFuncType(ObPartFuncType.LIST);
    }

    /**
     * Get order compare columns.
     */
    public List<ObColumn> getOrderCompareColumns() {
        return this.orderCompareColumns;
    }

    /**
     * Set order compare columns.
     */
    public void setOrderCompareColumns(List<ObColumn> columns) {
        this.orderCompareColumns = columns;
    }

    /**
     * Set sets.
     */
    public void setSets(Map<ObPartitionKey, Long> sets) {
        this.sets = sets;
    }

    public Map<ObPartitionKey, Long> getSets() {
        return this.sets;
    }

    /**
     * Get random part id.
     */
    @Override
    public Long getRandomPartId() {
        if (null == this.sets) {
            return null;
        }
        int randomIndex = RandomUtil.getRandomNum(0, this.sets.size());
        return (Long) this.sets.values().toArray()[randomIndex];
    }

    /**
     * Prepare.
     */
    @Override
    public void prepare() throws IllegalArgumentException {
        if (sets == null || sets.size() == 0) {
            throw new IllegalArgumentException(
                "prepare ObListPartDesc failed . partition set is empty " + sets);
        }

        if (orderCompareColumns == null || orderCompareColumns.size() == 0) {
            throw new IllegalArgumentException(
                "prepare ObListPartDesc failed . orderCompareColumns set is empty "
                        + orderCompareColumns);
        }
        super.prepare();
    }

    /**
     * Get part ids.
     */
    @Override
    public List<Long> getPartIds(Object[] start, boolean startInclusive, Object[] end,
                                 boolean endInclusive) {
        List<Object[]> rowKeys = new ArrayList<Object[]>();
        rowKeys.add(start);
        rowKeys.add(end);
        Long partId = getPartId(rowKeys, true);
        List<Long> partIds = new ArrayList<Long>();
        partIds.add(partId);
        return partIds;
    }

    /**
     * Get part id.
     */
    @Override
    public Long getPartId(Object... rowKey) {
        List<Object[]> rowKeys = new ArrayList<Object[]>();
        rowKeys.add(rowKey);
        return getPartId(rowKeys, false);
    }

    /**
     * Get part id.
     */
    @Override
    public Long getPartId(List<Object[]> rowKeys, boolean consistency) {
        if (rowKeys == null || rowKeys.size() == 0) {
            throw new IllegalArgumentException("invalid row keys :" + rowKeys);
        }

        try {
            Long partId = null;
            for (Object[] rowKey : rowKeys) {
                List<Object> currentRowKeyEvalValues = evalRowKeyValues(rowKey);
                List<Comparable> values = super.initComparableElementByTypes(
                    currentRowKeyEvalValues, this.orderCompareColumns);

                ObPartitionKey searchKey = ObPartitionKey.getInstance(orderCompareColumns, values);
                long currentPartId = this.sets.get(searchKey);

                if (partId == null) {
                    partId = currentPartId;
                }
                if (!consistency) {
                    break;
                }

                if (partId != currentPartId) {
                    throw new ObTablePartitionConsistentException(
                        "across partition operation may cause consistent problem " + rowKeys);
                }

            }
            return partId;
        } catch (IllegalArgumentException e) {
            logger.error(LCD.convert("01-00001"), e);
            throw new IllegalArgumentException(
                "ObListPartDesc get part id come across illegal params", e);
        }

    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("orderCompareColumns", this.orderCompareColumns)
            .append("sets", this.sets).append("partFuncType", this.getPartFuncType())
            .append("partExpr", this.getPartExpr()).toString();
    }
}

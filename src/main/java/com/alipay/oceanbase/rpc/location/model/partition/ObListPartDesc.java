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

import com.alipay.oceanbase.rpc.exception.ObTableException;
import com.alipay.oceanbase.rpc.exception.ObTablePartitionConsistentException;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.util.RandomUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

public class ObListPartDesc extends ObPartDesc {
    private static final Logger       logger              = TableClientLoggerFactory
                                                              .getLogger(ObListPartDesc.class);

    private List<ObColumn>            orderCompareColumns = null;
    private Map<ObPartitionKey, Long> sets                = null;

    /*
     * Ob list part desc.
     */
    public ObListPartDesc() {
        this.setPartFuncType(ObPartFuncType.LIST);
    }

    /*
     * Get order compare columns.
     */
    public List<ObColumn> getOrderCompareColumns() {
        return this.orderCompareColumns;
    }

    /*
     * Set order compare columns.
     */
    public void setOrderCompareColumns(List<ObColumn> columns) {
        this.orderCompareColumns = columns;
    }

    /*
     * Set sets.
     */
    public void setSets(Map<ObPartitionKey, Long> sets) {
        this.sets = sets;
    }

    public Map<ObPartitionKey, Long> getSets() {
        return this.sets;
    }

    /*
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

    /*
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

    /*
     * Get part ids.
     */
    @Override
    public List<Long> getPartIds(Object startRowObj, boolean startInclusive, Object endRowObj,
                                 boolean endInclusive) {
        throw new IllegalArgumentException("getPartIds for List partition is not supported");
    }
    /*
     * Get part id.
     */
    @Override
    public Long getPartId(Object... row) {
        List<Object> rows = new ArrayList<Object>();
        rows.addAll(Arrays.asList(row));
        return getPartId(rows, false);
    }

    /*
     * Get part id.
     */
    @Override
    public Long getPartId(List<Object> rows, boolean consistency) {
        if (rows == null || rows.size() == 0) {
            throw new IllegalArgumentException("invalid row keys :" + rows);
        }

        try {
            Long partId = null;
            for (Object rowObj : rows) {
                if (!(rowObj instanceof Row)) {
                    throw new ObTableException("invalid format of rowObj: " + rowObj);
                }
                Row row = (Row) rowObj;
                List<Object> currentRowKeyEvalValues = evalRowKeyValues(row);
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
                        "across partition operation may cause consistent problem " + rows);
                }

            }
            return partId;
        } catch (IllegalArgumentException e) {
            logger.error(LCD.convert("01-00001"), e);
            throw new IllegalArgumentException(
                "ObListPartDesc get part id come across illegal params", e);
        }

    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("orderCompareColumns", this.orderCompareColumns)
            .append("sets", this.sets).append("partFuncType", this.getPartFuncType())
            .append("partExpr", this.getPartExpr()).toString();
    }

    @Override
    public void setPartNum(int n) {
    }
}

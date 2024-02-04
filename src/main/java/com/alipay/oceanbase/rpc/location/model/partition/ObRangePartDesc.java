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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObSimpleColumn;
import com.alipay.oceanbase.rpc.util.RandomUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;

public class ObRangePartDesc extends ObPartDesc {
    private static final Logger logger = TableClientLoggerFactory.getLogger(ObRangePartDesc.class);

    /*
     * Ob range part desc.
     */
    public ObRangePartDesc() {
        setPartFuncType(ObPartFuncType.RANGE);
    }

    private int                                        partSpace                 = 0;
    private int                                        partNum                   = 0;
    private List<ObColumn>                             orderedCompareColumns     = null;
    private List<ObObjType>                            orderedCompareColumnTypes = null;
    private List<ObComparableKV<ObPartitionKey, Long>> bounds                    = null;

    /*
     * Get ordered compare column types.
     */
    public List<ObObjType> getOrderedCompareColumnTypes() {
        return orderedCompareColumnTypes;
    }

    /*
     * Set ordered compare column types.
     */
    public void setOrderedCompareColumnTypes(List<ObObjType> orderedPartColumnTypes) {

        this.orderedCompareColumnTypes = orderedPartColumnTypes;
    }

    /*
     * Set ordered compare columns.
     */
    public void setOrderedCompareColumns(List<ObColumn> orderedPartColumn) {
        this.orderedCompareColumns = orderedPartColumn;
    }

    /*
     * Get ordered compare columns.
     */
    public List<ObColumn> getOrderedCompareColumns() {
        return orderedCompareColumns;
    }

    /*
     * Set bounds.
     */
    public void setBounds(List<ObComparableKV<ObPartitionKey, Long>> bounds) {
        this.bounds = bounds;
    }

    public List<ObComparableKV<ObPartitionKey, Long>> getBounds() {
        return bounds;
    }

    /*
     * Get random part id.
     */
    @Override
    public Long getRandomPartId() {
        if (null == this.bounds) {
            return null;
        }
        int randomIndex = RandomUtil.getRandomNum(0, this.bounds.size());
        return this.bounds.get(randomIndex).value;
    }

    /*
     * Prepare.
     */
    @Override
    public void prepare() throws IllegalArgumentException {
        if (bounds == null || bounds.size() == 0) {
            throw new IllegalArgumentException(
                "prepare ObRangePartDesc failed. partition bounds is empty " + bounds);
        }

        if (orderedCompareColumnTypes == null || orderedCompareColumnTypes.size() == 0) {
            throw new IllegalArgumentException(
                "prepare ObRangePartDesc failed. partition orderedCompareColumnTypes is empty "
                        + orderedCompareColumnTypes);
        }

        if (orderedCompareColumns == null || orderedCompareColumns.size() == 0) {
            throw new IllegalArgumentException(
                "prepare ObRangePartDesc failed. partition orderedCompareColumns is empty "
                        + orderedCompareColumnTypes);
        }

        if (orderedCompareColumns.size() != orderedCompareColumnTypes.size()) {
            throw new IllegalArgumentException(
                "prepare ObRangePartDesc failed. the size of orderedCompareColumns  is as same as  the size of orderedCompareColumnTypes"
                        + orderedCompareColumnTypes);
        }
        // Range 类型的type比较特殊 在part_range_type中需要特殊转换一下
        List<ObColumn> orderedPartColumnTmp = new ArrayList<ObColumn>(orderedCompareColumns.size());
        for (int i = 0; i < orderedCompareColumns.size(); i++) {
            ObColumn obColumn = orderedCompareColumns.get(i);
            ObColumn convert;
            if (obColumn.getObGeneratedColumnSimpleFunc() != null) {
                convert = new ObGeneratedColumn(obColumn.getColumnName(), obColumn.getIndex(),
                    orderedCompareColumnTypes.get(i), obColumn.getObCollationType(),
                    obColumn.getObGeneratedColumnSimpleFunc());
            } else {
                convert = new ObSimpleColumn(obColumn.getColumnName(), obColumn.getIndex(),
                    orderedCompareColumnTypes.get(i), obColumn.getObCollationType());
            }
            orderedPartColumnTmp.add(convert);
        }
        this.orderedCompareColumns = orderedPartColumnTmp;

        if (orderedCompareColumnTypes.size() != orderedPartColumnNames.size()) {
            throw new IllegalArgumentException(
                "prepare ObRangePartDesc failed. the size of orderedCompareColumnTypes is not equal with the size of orderedCompareColumns. types "
                        + orderedCompareColumnTypes + " columns" + orderedPartColumnNames);
        }

        super.prepare();
    }

    /*
     * Get part space.
     */
    public int getPartSpace() {
        return partSpace;
    }

    /*
     * Set part space.
     */
    public void setPartSpace(int partSpace) {
        this.partSpace = partSpace;
    }

    /*
     * Get part num.
     */
    public int getPartNum() {
        return this.partNum;
    }

    /*
     * Set part num.
     */
    public void setPartNum(int partNum) {
        this.partNum = partNum;
    }

    /*
     * Get part ids.
     */
    @Override
    public List<Long> getPartIds(Object[] start, boolean startInclusive, Object[] end,
                                 boolean endInclusive) {

        // can not detail the border effect so that the range is magnified
        int startIdx = getBoundsIdx(start);
        int stopIdx = getBoundsIdx(end);
        List<Long> partIds = new ArrayList<Long>();
        for (int i = startIdx; i <= stopIdx; i++) {
            partIds.add(this.bounds.get(i).value);
        }
        return partIds;
    }

    /*
     * Get part id.
     */
    @Override
    public Long getPartId(Object... rowKey) {
        try {
            return this.bounds.get(getBoundsIdx(rowKey)).value;
        } catch (IllegalArgumentException e) {
            RUNTIME.error(LCD.convert("01-00025"), e);
            throw new IllegalArgumentException(
                "ObRangePartDesc get part id come across illegal params", e);
        }

    }

    public int getBoundsIdx(Object... rowKey) {
        if (rowKey.length != rowKeyElement.size()) {
            throw new IllegalArgumentException("row key is consist of " + rowKeyElement
                                               + "but found" + Arrays.toString(rowKey));
        }

        try {
            List<Object> evalParams = evalRowKeyValues(rowKey);
            List<Comparable> comparableElement = super.initComparableElementByTypes(evalParams,
                this.orderedCompareColumns);
            ObPartitionKey searchKey = ObPartitionKey.getInstance(orderedCompareColumns,
                comparableElement);

            int pos = upperBound(this.bounds, new ObComparableKV<ObPartitionKey, Long>(searchKey,
                (long) -1));
            if (pos >= this.bounds.size()) {
                throw new ArrayIndexOutOfBoundsException("Table has no partition for value in "
                                                         + this.getPartExpr());
            } else {
                return pos;
            }
        } catch (IllegalArgumentException e) {
            RUNTIME.error(LCD.convert("01-00025"), e);
            throw new IllegalArgumentException("ObRangePartDesc get getBoundsIdx error", e);
        }

    }

    /*
     * Get part id.
     */
    @Override
    public Long getPartId(List<Object[]> rowKeys, boolean consistency) {
        if (rowKeys == null || rowKeys.size() == 0) {
            throw new IllegalArgumentException("invalid row keys :" + rowKeys);
        }
        Long partId = null;

        for (Object[] rowKey : rowKeys) {
            long currentPartId = getPartId(rowKey);
            if (partId == null) {
                partId = currentPartId;
            }
            if (!consistency) {
                break;
            }

            if (!partId.equals(currentPartId)) {
                throw new ObTablePartitionConsistentException(
                    "across partition operation may cause consistent problem " + rowKeys);
            }
        }

        return partId;
    }

    private static <T extends Comparable<? super T>> int upperBound(List<T> list, T key) {
        int first = 0;
        int len = list.size();
        int half = 0;
        int middle = 0;
        while (len > 0) {
            half = len >> 1;
            middle = first + half;
            if (list.get(middle).compareTo(key) > 0) {
                // search left
                len = half;
            } else {
                // search right
                first = middle + 1;
                len = len - half - 1;
            }
        }
        return first;
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this)
            .append("orderedCompareColumnTypes", this.orderedCompareColumnTypes)
            .append("bounds", this.bounds).append("partFuncType", this.getPartFuncType())
            .append("partExpr", this.getPartExpr()).toString();
    }
}

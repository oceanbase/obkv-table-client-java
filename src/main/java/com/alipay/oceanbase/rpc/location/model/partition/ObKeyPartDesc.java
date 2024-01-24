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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.util.ObHashUtils;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.alipay.oceanbase.rpc.util.RandomUtil.getRandomNum;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

/*
 *  ------4----|----------28------------
 *  part space |     part id
 */
public class ObKeyPartDesc extends ObPartDesc {
    private static final Logger logger    = TableClientLoggerFactory.getLogger(ObKeyPartDesc.class);
    private List<Long>          completeWorks;

    private int                 partSpace = 0;
    private int                 partNum   = 0;

    /*
     * Ob key part desc.
     */
    public ObKeyPartDesc() {
        setPartFuncType(ObPartFuncType.KEY);
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
        List<Long> partIds = new ArrayList<Long>();
        for (long i = 0; i < partNum; i++) {
            partIds.add(i);
        }
        completeWorks = Collections.unmodifiableList(partIds);
    }

    /*
     * Get part ids.
     */
    @Override
    public List<Long> getPartIds(Object[] start, boolean startInclusive, Object[] end,
                                 boolean endInclusive) {
        try {
            // eval rowkey
            List<Object> startValues = evalRowKeyValues(start);
            List<Object> endValues = evalRowKeyValues(end);

            if (startValues == null || endValues == null) {
                throw new NumberFormatException("can not parseToComparable start value ["
                                                + startValues + "] or end value [" + endValues
                                                + "] to long");
            }

            if (startValues.equals(endValues)) {
                List<Object[]> rowKeys = new ArrayList<Object[]>();
                List<Long> partIds = new ArrayList<Long>();
                rowKeys.add(start);
                partIds.add(getPartId(rowKeys, false));
                return partIds;
            } else {
                // partition key is different in key partition
                return completeWorks;
            }
        } catch (IllegalArgumentException e) {
            logger.error(LCD.convert("01-00002"), e);
            throw new IllegalArgumentException(
                "ObKeyPartDesc get part id come across illegal params", e);
        }
    }

    /*
     * Get random part id.
     */
    @Override
    public Long getRandomPartId() {
        return (this.partNum > 0 ? (long) getRandomNum(0, this.partNum) : null);
    }

    /*
     * Get part id.
     */
    @Override
    public Long getPartId(Object... rowKey) throws IllegalArgumentException {
        List<Object[]> rowKeys = new ArrayList<Object[]>();
        rowKeys.add(rowKey);
        return getPartId(rowKeys, false);
    }

    /*
     * Get part id.
     */
    @Override
    public Long getPartId(List<Object[]> rowKeys, boolean consistency) {

        if (rowKeys == null || rowKeys.size() == 0) {
            throw new IllegalArgumentException("invalid row keys :" + rowKeys);
        }

        try {
            int partRefColumnSize = orderedPartRefColumnRowKeyRelations.size();
            List<Object> evalValues = null;

            for (Object[] rowKey : rowKeys) {
                List<Object> currentRowKeyEvalValues = evalRowKeyValues(rowKey);
                if (evalValues == null) {
                    evalValues = currentRowKeyEvalValues;
                }

                if (!consistency) {
                    break;
                }

                if (evalValues == currentRowKeyEvalValues) {
                    continue;
                }

                for (int i = 0; i < evalValues.size(); i++) {
                    if (!equalsWithCollationType(orderedPartRefColumnRowKeyRelations.get(i)
                        .getLeft().getObCollationType(), evalValues.get(i),
                        currentRowKeyEvalValues.get(i))) {
                        throw new ObTablePartitionConsistentException(
                            "across partition operation may cause consistent problem " + rowKeys);
                    }
                }
            }

            long hashValue = 0L;
            for (int i = 0; i < partRefColumnSize; i++) {
                hashValue = ObHashUtils.toHashcode(evalValues.get(i),
                    orderedPartRefColumnRowKeyRelations.get(i).getLeft(), hashValue,
                    this.getPartFuncType());
            }

            hashValue = (hashValue > 0 ? hashValue : -hashValue);
            return ((long) partSpace << ObPartConstants.OB_PART_IDS_BITNUM)
                   | (hashValue % this.partNum);
        } catch (IllegalArgumentException e) {
            logger.error(LCD.convert("01-00023"), e);
            throw new IllegalArgumentException(
                "ObKeyPartDesc get part id come across illegal params", e);
        }
    }

    private boolean equalsWithCollationType(ObCollationType collationType, Object s, Object t)
                                                                                              throws IllegalArgumentException {
        if (collationType == ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI) {
            if (!(s instanceof String && t instanceof String)) {
                throw new IllegalArgumentException(
                    "CS_TYPE_UTF8MB4_GENERAL_CI only allow string equal");
            }
            return ((String) s).equalsIgnoreCase((String) t);
        } else {
            return s.equals(t);
        }
    }

    /*
     * Set row key element.
     */
    @Override
    public void setRowKeyElement(Map<String, Integer> rowKeyElement) {
        if (rowKeyElement == null || rowKeyElement.size() == 0) {
            throw new IllegalArgumentException("ObKeyPartDesc rowKeyElement is empty");
        }
        super.setRowKeyElement(rowKeyElement);
    }

    /*
     * To string.
     */
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("partSpace", partSpace).append("partNum", partNum)
            .append("partFuncType", this.getPartFuncType()).append("partExpr", this.getPartExpr())
            .toString();
    }
}

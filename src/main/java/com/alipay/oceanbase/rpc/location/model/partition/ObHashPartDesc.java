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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.RandomUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;

/*
 *  ------4----|----------28------------
 *  part space |     part id
 */
public class ObHashPartDesc extends ObPartDesc {
    private static final Logger logger    = TableClientLoggerFactory
                                              .getLogger(ObHashPartDesc.class);

    private List<Long>          completeWorks;
    private int                 partSpace = 0;
    private int                 partNum   = 0;

    /*
     * Ob hash part desc.
     */
    public ObHashPartDesc() {
        setPartFuncType(ObPartFuncType.HASH);
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
     * Prepare.
     */
    @Override
    public void prepare() {
        //hash no prepare things
        super.prepare();
    }

    /*
     * Get part ids.
     */
    @Override
    public List<Long> getPartIds(Object[] start, boolean startInclusive, Object[] end,
                                 boolean endInclusive) {
        // close set
        try {
            List<Object> startValues = evalRowKeyValues(start);
            Object startValue = startValues.get(0);
            List<Object> endValues = evalRowKeyValues(end);
            Object endValue = endValues.get(0);

            Long startLongValue = ObObjType.parseToLongOrNull(startValue);
            Long endLongValue = ObObjType.parseToLongOrNull(endValue);

            if (startLongValue == null || endLongValue == null) {
                throw new NumberFormatException("can not parseToComparable start value ["
                                                + startValue + "] or end value [" + endValue
                                                + "] to long");
            }
            long startHashValue = startLongValue - (startInclusive ? 0 : -1);
            long endHashValue = endLongValue - (endInclusive ? 0 : 1);

            if (endHashValue - startHashValue + 1 >= partNum) {
                return completeWorks;
            } else {
                List<Long> partIds = new ArrayList<Long>();
                for (long i = startHashValue; i <= endHashValue; i++) {
                    partIds.add(innerHash(i));
                }
                return partIds;
            }
        } catch (IllegalArgumentException e) {
            logger.error(LCD.convert("01-00002"), e);
            throw new IllegalArgumentException(
                "ObHashPartDesc get part id come across illegal params", e);
        }
    }

    /*
     * Get random part id.
     */
    @Override
    public Long getRandomPartId() {
        return ((this.partNum > 0) ? (long) RandomUtil.getRandomNum(0, this.partNum) : null);
    }

    /*
     * Get part id.
     */
    @Override
    public Long getPartId(Object... rowKey) {
        List<Object[]> rowKeys = new ArrayList<Object[]>();
        rowKeys.add(rowKey);
        return this.getPartId(rowKeys, false);
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
        try {
            for (Object[] rowKey : rowKeys) {
                List<Object> evalValues = evalRowKeyValues(rowKey);
                Object value = evalValues.get(0);// the partition type of hash has one param at most
                Long longValue = ObObjType.parseToLongOrNull(value);

                if (longValue == null) {
                    throw new IllegalArgumentException("can not parseToComparable value [" + value
                                                       + "] to long");
                }

                long currentPartId = innerHash(longValue);
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
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                "ObHashPartDesc get part id come across illegal params", e);
        }
        return partId;
    }

    private Long innerHash(long hashValue) {
        hashValue = Math.abs(hashValue);
        return (partSpace << ObPartConstants.OB_PART_IDS_BITNUM) | (hashValue % partNum);
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

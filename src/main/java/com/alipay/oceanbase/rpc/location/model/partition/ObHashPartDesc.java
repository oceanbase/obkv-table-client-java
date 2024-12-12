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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.util.RandomUtil;
import com.alipay.oceanbase.rpc.util.Serialization;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.oceanbase.rpc.mutation.Row;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;

import java.util.*;

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
    public List<Long> getPartIds(Object startRowObj, boolean startInclusive, Object endRowObj,
                                 boolean endInclusive) {
        // close set
        try {
            // verify the type of parameters and convert to Row
            if (!(startRowObj instanceof Row) || !(endRowObj instanceof Row)) {
                throw new ObTableException("invalid format of rowObj: " + startRowObj + ", "
                                           + endRowObj);
            }
            Row startRow = (Row) startRowObj, endRow = (Row) endRowObj;
            // pre-check start and end
            // should remove after remove addRowkeyElement
            if (startRow.size() != endRow.size()) {
                throw new IllegalArgumentException("length of start key and end key is not equal");
            }

            if (startRow.size() == 1 && startRow.getValues()[0] instanceof ObObj
                && ((ObObj) startRow.getValues()[0]).isMinObj() && endRow.size() == 1
                && endRow.getValues()[0] instanceof ObObj
                && ((ObObj) endRow.getValues()[0]).isMaxObj()) {
                return completeWorks;
            }

            // check whether partition key is Min or Max, should refactor after remove addRowkeyElement
            for (ObColumn curObcolumn : partColumns) {
                for (int refIdx = 0; refIdx < curObcolumn.getRefColumnNames().size(); ++refIdx) {
                    String curObRefColumnName = curObcolumn.getRefColumnNames().get(refIdx);
                    if (startRow.size() <= refIdx) {
                        throw new IllegalArgumentException("rowkey length is " + startRow.size()
                                                           + ", which is shortest than " + refIdx);
                    }
                    Object startValue = null;
                    for (Map.Entry<String, Object> entry : startRow.getMap().entrySet()) {
                        if (entry.getKey().equalsIgnoreCase(curObRefColumnName)) {
                            startValue = entry.getValue();
                            break;
                        }
                    }
                    if (startValue == null) {
                        throw new IllegalArgumentException(
                            "Please include all partition key in start range. Currently missing key: { "
                                    + curObRefColumnName + " }");
                    }
                    if (startValue instanceof ObObj
                        && (((ObObj) startValue).isMinObj() || ((ObObj) startValue).isMaxObj())) {
                        return completeWorks;
                    }

                    Object endValue = null;
                    for (Map.Entry<String, Object> entry : endRow.getMap().entrySet()) {
                        if (entry.getKey().equalsIgnoreCase(curObRefColumnName)) {
                            endValue = entry.getValue();
                            break;
                        }
                    }
                    if (endValue == null) {
                        throw new IllegalArgumentException(
                            "Please include all partition key in end range. Currently missing key: { "
                                    + curObRefColumnName + " }");
                    }
                    if (endValue instanceof ObObj
                        && (((ObObj) endValue).isMinObj() || ((ObObj) endValue).isMaxObj())) {
                        return completeWorks;
                    }
                }
            }

            // eval partition key
            List<Object> startValues = evalRowKeyValues(startRow);
            Object startValue = startValues.get(0);
            List<Object> endValues = evalRowKeyValues(endRow);
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
    public Long getPartId(Object... row) {
        List<Object> rows = new ArrayList<Object>();
        rows.addAll(Arrays.asList(row));
        return this.getPartId(rows, false);
    }

    /*
     * Get part id.
     */
    @Override
    public Long getPartId(List<Object> rows, boolean consistency) {

        if (rows == null || rows.size() == 0) {
            throw new IllegalArgumentException("invalid row keys :" + rows);
        }

        Long partId = null;
        try {
            for (Object rowObj : rows) {
                if (!(rowObj instanceof Row)) {
                    throw new ObTableException("invalid format of rowObj: " + rowObj);
                }
                Row row = (Row) rowObj;
                List<Object> evalValues = evalRowKeyValues(row);
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
                        "across partition operation may cause consistent problem " + rows);
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

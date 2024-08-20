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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.util.ObHashUtils;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.slf4j.Logger;

import java.util.*;

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
    public List<Long> getPartIds(Object startRowObj, boolean startInclusive, Object endRowObj,
                                 boolean endInclusive) {
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

            if (startRow.size() == 1  && startRow.getValues()[0] instanceof ObObj && ((ObObj) startRow.getValues()[0]).isMinObj() &&
                    endRow.size() == 1  && endRow.getValues()[0] instanceof ObObj && ((ObObj) endRow.getValues()[0]).isMaxObj()) {
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
                    if (startRow.get(curObRefColumnName) instanceof ObObj
                        && (((ObObj) startRow.get(curObRefColumnName)).isMinObj() || ((ObObj) startRow
                            .get(curObRefColumnName)).isMaxObj())) {
                        return completeWorks;
                    }
                    if (endRow.get(curObRefColumnName) instanceof ObObj
                        && (((ObObj) endRow.get(curObRefColumnName)).isMinObj() || ((ObObj) endRow
                            .get(curObRefColumnName)).isMaxObj())) {
                        return completeWorks;
                    }
                }
            }

            // eval partition key
            List<Object> startValues = evalRowKeyValues(startRow);
            List<Object> endValues = evalRowKeyValues(endRow);

            if (startValues == null || endValues == null) {
                throw new NumberFormatException("can not parseToComparable start value ["
                                                + startValues + "] or end value [" + endValues
                                                + "] to long");
            }

            if (startValues.equals(endValues)) {
                List<Long> partIds = new ArrayList<Long>();
                partIds.add(calcPartId(startValues));
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
    public Long getPartId(Object... row) throws IllegalArgumentException {
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
            int partRefColumnSize = partColumns.size();
            List<Object> evalValues = null;

            for (Object rowObj : rows) {
                if (!(rowObj instanceof Row)) {
                    throw new ObTableException("invalid format of rowObj: " + rowObj);
                }
                Row row = (Row) rowObj;
                List<Object> currentRowKeyEvalValues = evalRowKeyValues(row);
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
                    if (!equalsWithCollationType(partColumns.get(i).getObCollationType(),
                        evalValues.get(i), currentRowKeyEvalValues.get(i))) {
                        throw new ObTablePartitionConsistentException(
                            "across partition operation may cause consistent problem " + rows);
                    }
                }
            }

            return calcPartId(evalValues);
        } catch (IllegalArgumentException e) {
            logger.error(LCD.convert("01-00023"), e);
            throw new IllegalArgumentException(
                "ObKeyPartDesc get part id come across illegal params", e);
        }
    }

    // calc partition id from eval values
    private Long calcPartId(List<Object> evalValues) {
        if (evalValues == null || evalValues.size() != partColumns.size()) {
            throw new IllegalArgumentException("invalid eval values :" + evalValues);
        }

        long hashValue = 0L;
        for (int i = 0; i < partColumns.size(); i++) {
            hashValue = ObHashUtils.toHashcode(evalValues.get(i),
                    partColumns.get(i), hashValue,
                    this.getPartFuncType());
        }

        hashValue = (hashValue > 0 ? hashValue : -hashValue);
        return ((long) partSpace << ObPartConstants.OB_PART_IDS_BITNUM)
                | (hashValue % this.partNum);
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

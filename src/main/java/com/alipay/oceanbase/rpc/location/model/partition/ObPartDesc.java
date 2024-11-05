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
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.util.StringUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.oceanbase.rpc.mutation.Row;
import org.slf4j.Logger;

import java.util.*;

import static com.alipay.oceanbase.rpc.constant.Constants.EMPTY_STRING;
import static com.alipay.oceanbase.rpc.location.model.partition.ObPartitionKey.MAX_PARTITION_ELEMENT;
import static com.alipay.oceanbase.rpc.location.model.partition.ObPartitionKey.MIN_PARTITION_ELEMENT;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObSimpleColumn.DEFAULT_UTF8MB4_GENERAL_CI;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;
import static java.util.Collections.*;

public abstract class ObPartDesc {

    private static final Logger                     logger                              = TableClientLoggerFactory
                                                                                            .getLogger(ObPartDesc.class);
    private ObPartFuncType                          partFuncType                        = ObPartFuncType.UNKNOWN;
    private String                                  partExpr                            = EMPTY_STRING;
    @SuppressWarnings("unchecked")
    protected List<String>                          orderedPartColumnNames              = EMPTY_LIST;
    @SuppressWarnings("unchecked")
    protected List<ObPair<ObColumn, List<Integer>>> orderedPartRefColumnRowKeyRelations = EMPTY_LIST;
    @SuppressWarnings("unchecked")
    protected List<ObColumn>                        partColumns                         = EMPTY_LIST;
    private Map<String, Long>                       partNameIdMap                       = null;
    public static final ObPartitionKey              DEFAULT_PART_KEY                    = ObPartitionKey
                                                                                            .getInstance(
                                                                                                Collections
                                                                                                    .singletonList(DEFAULT_UTF8MB4_GENERAL_CI),
                                                                                                "default");
    @SuppressWarnings("unchecked")
    protected Map<String, Integer>                  rowKeyElement                       = EMPTY_MAP;

    /*
     * Get part func type.
     */
    public ObPartFuncType getPartFuncType() {
        return partFuncType;
    }

    /*
     * Set part func type.
     */
    public void setPartFuncType(ObPartFuncType partFuncType) {
        this.partFuncType = partFuncType;
    }

    /*
     * Get part expr.
     */
    public String getPartExpr() {
        return partExpr;
    }

    /*
     * Set part expr.
     */
    public void setPartExpr(String partExpr) {
        if (StringUtil.isBlank(partExpr)) {
            throw new IllegalArgumentException("ObKeyPartDesc part express is blank");
        }
        this.partExpr = partExpr.replace(" ", "");
        this.orderedPartColumnNames = unmodifiableList(Arrays.asList(this.partExpr.split(",")));
    }

    /*
     * Get part num
     */
    public int getPartNum() {
        return -1;
    }

    /*
     * Get ordered part column names.
     */
    public List<String> getOrderedPartColumnNames() {
        return orderedPartColumnNames;
    }

    /*
     * Get part columns.
     */
    public List<ObColumn> getPartColumns() {
        return partColumns;
    }

    /*
     * Set part columns.
     */
    public void setPartColumns(List<ObColumn> partColumns) {
        this.partColumns = partColumns;
    }

    public Map<String, Long> getPartNameIdMap() {
        return this.partNameIdMap;
    }

    /*
     * Set part name id map.
     */
    public void setPartNameIdMap(Map<String, Long> partNameIdMap) {
        this.partNameIdMap = partNameIdMap;
    }

    public Map<String, Integer> getRowKeyElement() {
        return rowKeyElement;
    }

    /*
     * Set row key element.
     */
    public void setRowKeyElement(Map<String, Integer> rowKeyElement) {
        this.rowKeyElement = rowKeyElement;
    }

    protected List<Comparable> initComparableElementByTypes(List<Object> objects,
                                                            List<ObColumn> obColumns) {
        List<Comparable> comparableElement = new ArrayList<Comparable>(objects.size());
        try {
            for (int i = 0; i < objects.size(); i++) {
                ObColumn obColumn = obColumns.get(i);
                if (objects.get(i) instanceof ObObj) {
                    // deal with min / max
                    ObObj obj = (ObObj) objects.get(i);
                    if (obj.isMinObj()) {
                        comparableElement.add(MIN_PARTITION_ELEMENT);
                    } else if (obj.isMaxObj()) {
                        comparableElement.add(MAX_PARTITION_ELEMENT);
                    } else {
                        throw new IllegalArgumentException(String.format(
                            "failed to cast obj, obj=%s, types=%s", objects, obColumns));
                    }
                } else {
                    comparableElement.add(obColumn.getObObjType().parseToComparable(objects.get(i),
                        obColumn.getObCollationType()));
                }
            }
        } catch (Exception e) {
            logger.error(LCD.convert("01-00024"), objects, obColumns, e);
            throw new IllegalArgumentException(String.format(
                "failed to cast obj, obj=%s, types=%s", objects, obColumns), e);
        }
        return comparableElement;
    }

    //to prepare partition calculate resource
    //to check partition calculate is ready
    public void prepare() throws IllegalArgumentException { /* do nothing now */
    }

    /*
     * Eval row key values.
     */
    public List<Object> evalRowKeyValues(Row row) throws IllegalArgumentException {
        int partColumnSize = partColumns.size();
        List<Object> evalValues = new ArrayList<Object>(partColumnSize);
        Object[] rowValues = row.getValues();
        String[] rowColumnNames = row.getColumns();

        if (rowValues.length < partColumnSize) {
            throw new IllegalArgumentException("Input row key should at least include "
                                               + partColumns + "but found"
                                               + Arrays.toString(rowValues));
        }

        boolean needEval = true;

        // column or generate column
        for (int i = 0; i < partColumns.size(); ++i) {
            ObColumn curObColumn = partColumns.get(i);
            List<String> curObRefColumnNames = curObColumn.getRefColumnNames();
            Object[] evalParams = new Object[curObRefColumnNames.size()];
            for (int j = 0; j < curObRefColumnNames.size(); ++j) {
                for (int k = 0; k < rowColumnNames.length; ++k) {
                    if (rowColumnNames[k].equalsIgnoreCase(curObRefColumnNames.get(j))) {
                        if (curObRefColumnNames.size() == 1 && rowValues[k] instanceof ObObj) {
                            ObObj obj = (ObObj) rowValues[k];
                            if (obj.isMaxObj() || obj.isMinObj()) {
                                evalValues.add(obj);
                                needEval = false;
                                break;
                            }
                        }
                        evalParams[j] = rowValues[k];
                        break;
                    }
                }
            }
            if (needEval) {
                evalValues.add(curObColumn.evalValue(evalParams));
            }
        }

        return evalValues;
    }

    /*
     *
     * @param start the start row key
     * @param startInclusive the start row key inclusive
     * @param end   the end row key
     * @param endInclusive the end row key inclusive
     */
    public abstract List<Long> getPartIds(Object startRowObj, boolean startInclusive,
                                          Object endRowObj, boolean endInclusive)
                                                                                 throws IllegalArgumentException;

    public abstract Long getPartId(Object... row) throws IllegalArgumentException;

    public abstract Long getPartId(List<Object> row, boolean consistency)
                                                                         throws IllegalArgumentException,
                                                                         ObTablePartitionConsistentException;

    public abstract Long getRandomPartId();

    public abstract void setPartNum(int n);
}

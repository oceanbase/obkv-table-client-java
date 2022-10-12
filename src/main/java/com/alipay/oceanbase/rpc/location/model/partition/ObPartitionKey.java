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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;

import java.util.Arrays;
import java.util.List;

public class ObPartitionKey implements Comparable<ObPartitionKey> {

    public static final Comparable MAX_PARTITION_ELEMENT = new Comparable() {
                                                             /*
                                                              * Compare to.
                                                              */
                                                             @Override
                                                             public int compareTo(Object o) {
                                                                 if (o == this) {
                                                                     return 0;
                                                                 }
                                                                 return 1;
                                                             }
                                                         };

    public static final Comparable MIN_PARTITION_ELEMENT = new Comparable() {
                                                             /*
                                                              * Compare to.
                                                              */
                                                             @Override
                                                             public int compareTo(Object o) {
                                                                 if (o == this) {
                                                                     return 0;
                                                                 }
                                                                 return -1;
                                                             }
                                                         };

    private final List<Comparable> partitionElements;
    private final List<ObColumn>   orderPartColumns;

    public List<Comparable> getPartitionElements() {
        return partitionElements;
    }

    public List<ObColumn> getOrderPartColumns() {
        return orderPartColumns;
    }

    /*
     * Ob partition key.
     */
    public ObPartitionKey(List<ObColumn> orderPartColumns, List<Comparable> partitionElements)
                                                                                              throws IllegalArgumentException {
        if (orderPartColumns == null || partitionElements == null) {
            throw new IllegalArgumentException(
                "orderPartColumns is null or partitionElements is null");
        }
        if (orderPartColumns.size() != partitionElements.size()) {
            throw new IllegalArgumentException(
                "the size of orderPartColumns is not same as the size of partitionElements ");
        }
        this.orderPartColumns = orderPartColumns;
        this.partitionElements = partitionElements;
    }

    @Override
    @SuppressWarnings("unchecked")
    /*
     * Compare to.
     */
    public int compareTo(ObPartitionKey that) {
        if (this.partitionElements.size() != that.partitionElements.size()) {
            throw new ClassCastException("size not equal" + this.partitionElements
                                         + that.partitionElements);
        }

        int tmpRet;
        for (int i = 0; i < this.partitionElements.size(); ++i) {
            Comparable thisElement = this.partitionElements.get(i);
            Comparable thatElement = that.partitionElements.get(i);

            if (thisElement == thatElement) {
                continue;
            }

            if (thisElement == MAX_PARTITION_ELEMENT || thatElement == MIN_PARTITION_ELEMENT) {
                return 1;
            }

            if (thatElement == MAX_PARTITION_ELEMENT || thisElement == MIN_PARTITION_ELEMENT) {
                return -1;
            }

            if (orderPartColumns.get(i).getObCollationType() == ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI) {
                if (!(thisElement instanceof String && thatElement instanceof String)) {
                    throw new IllegalArgumentException(
                        "CS_TYPE_UTF8MB4_GENERAL_CI only allow string compare");
                }
                tmpRet = ((String) thisElement).toUpperCase().compareTo(
                    ((String) thatElement).toUpperCase());
            } else {
                tmpRet = thisElement.compareTo(thatElement);
            }

            if (0 != tmpRet) {
                // if not equal, we will return immediately
                return tmpRet;
            }
        }
        return 0;
    }

    /*
     * Get instance.
     */
    public static ObPartitionKey getInstance(List<ObColumn> orderPartColumns,
                                             Comparable... rowKeyElements) {
        return new ObPartitionKey(orderPartColumns, Arrays.asList(rowKeyElements));
    }

    /*
     * Get instance.
     */
    public static ObPartitionKey getInstance(List<ObColumn> orderPartColumns,
                                             List<Comparable> rowKeyElements) {
        return new ObPartitionKey(orderPartColumns, rowKeyElements);
    }

    /*
     * Equals.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        ObPartitionKey that = (ObPartitionKey) o;

        if (this.partitionElements.size() != that.partitionElements.size()) {
            return false;
        }

        for (int i = 0; i < this.partitionElements.size(); i++) {
            Comparable thisElement = this.partitionElements.get(i);
            Comparable thatElement = this.partitionElements.get(i);

            if (thisElement == thatElement) {
                continue;
            }

            if (orderPartColumns.get(i).getObCollationType() == ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI) {

                if (thisElement == MAX_PARTITION_ELEMENT || thisElement == MIN_PARTITION_ELEMENT
                    || thatElement == MAX_PARTITION_ELEMENT || thatElement == MIN_PARTITION_ELEMENT) {
                    return false;
                }
                if (!(thisElement instanceof String && thatElement instanceof String)) {
                    throw new IllegalArgumentException(
                        "CS_TYPE_UTF8MB4_GENERAL_CI only allow string compare");
                }
                if (!((String) thisElement).equalsIgnoreCase((String) thatElement)) {
                    return false;
                }
            }

            if (!thisElement.equals(thatElement)) {
                return false;
            }
        }

        return true;
    }

    /*
     * Hash code.
     */
    @Override
    public int hashCode() {
        int hashCode = 1;
        for (int i = 0; i < partitionElements.size(); i++) {
            Comparable thisElement = this.partitionElements.get(i);

            if (thisElement == MAX_PARTITION_ELEMENT || thisElement == MIN_PARTITION_ELEMENT) {
                hashCode = 31 * hashCode + thisElement.hashCode();
                continue;
            }

            if (orderPartColumns.get(i).getObCollationType() == ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI) {
                if (!(thisElement instanceof String)) {
                    throw new IllegalArgumentException(
                        "CS_TYPE_UTF8MB4_GENERAL_CI only allow string compare");
                }
                hashCode = 31 * hashCode + ((String) thisElement).toUpperCase().hashCode();
            }
        }
        return hashCode;
    }
}

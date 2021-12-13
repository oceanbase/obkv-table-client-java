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

import static com.alipay.oceanbase.rpc.location.model.partition.ObPartConstants.PART_ID_BITNUM;
import static com.alipay.oceanbase.rpc.location.model.partition.ObPartConstants.PART_ID_SHIFT;

public class ObPartIdCalculator {
    private static final Long MASK = (1L << PART_ID_BITNUM)
                                     | 1L << (PART_ID_BITNUM + PART_ID_SHIFT);

    /*
     * Generate part id.
     */
    public static Long generatePartId(Long partId1, Long partId2) {

        if (null == partId1) {
            return null;
        }

        if (null == partId2) {
            return partId1;
        }

        return (partId1 << PART_ID_SHIFT) | partId2 | MASK;
    }

}

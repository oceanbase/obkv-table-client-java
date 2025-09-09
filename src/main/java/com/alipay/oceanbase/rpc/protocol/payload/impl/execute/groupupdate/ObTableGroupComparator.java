/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2022 Ant Financial Services Group
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.groupupdate;

import java.util.ArrayList;
import java.util.List;

/**
 * @author xuanchao.xc
 * @since  2022-12-28
 */
public class ObTableGroupComparator {
    private List<String> cmpList = new ArrayList<>();

    /**
     * @desc 添加列比较关系，要求提前在表中定义列关系，否则无法生效
     * @param lcol 比较列列名
     * @param rcol 被比较列列名
     * @param val  比较值
     * @return
     */
    public ObTableGroupComparator addColumnCompare(String lcol, String rcol, long val) {
        String s = lcol + ":" + rcol + ":" + val;
        cmpList.add(s);
        return this;
    }

    /**
     * @desc 添加常量比较关系
     * @param lcol 比较列列名
     * @param val  比较值
     * @param op   比较关系
     * @return
     */
    public ObTableGroupComparator addConstantCompare(String lcol, long val, ObTableGroupCompareOp op) {
        String s = lcol + ":" + val + ":" + op.getValue();
        cmpList.add(s);

        return  this;
    }

    @Override
    public String toString() {
        char c = ',';
        StringBuilder sb = new StringBuilder(32);

        if (cmpList.size() > 0) {
            sb.append(cmpList.get(0));
        }

        for (int i = 1; i < cmpList.size(); i++) {
            sb.append(c);
            sb.append(cmpList.get(i));
        }

        return sb.toString();
    }
}

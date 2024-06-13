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

package com.alipay.oceanbase.rpc.protocol.payload.impl.column;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObExprGrammerVisitorImpl;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.gen.ObExprGrammerParser;

import java.util.List;
import java.util.Map;

public class ObGeneratedColumnSimpleFunc {

    ObExprGrammerParser.ExprContext ctx;
    public ObGeneratedColumnSimpleFunc(ObExprGrammerParser.ExprContext ctx) {
        this.ctx = ctx;
    }

    void setParameters(List<Object> parameters) { return; }

    int getMinParameters() { return 0; }

    int getMaxParameters() { return 0; }

    //    List<String> getRefColumnNames();

//    Object evalValue(ObCollationType collationTypeList, Object... refs)
//                                                                       throws IllegalArgumentException
//    {
//        throw new IllegalArgumentException("not supported");
//    }

    public Object evalValue(ObCollationType collationType, Map<String, Object> rowMap) {
        ObExprGrammerVisitorImpl visitor = new ObExprGrammerVisitorImpl(collationType, rowMap);
        return visitor.visit(ctx);
    }

}

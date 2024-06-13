/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnSubStrFunc;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.gen.ObExprGrammerBaseVisitor;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.gen.ObExprGrammerParser;

public class ObExprGrammerVisitorImpl<Object> extends ObExprGrammerBaseVisitor<Object> {

    public ObExprGrammerVisitorImpl(ObCollationType collationType, Map<String, Object> rowMap) {
        super();
        this.collationType = collationType;
        this.rowMap = rowMap;
    }

    private Map<String, Object> rowMap;
    ObCollationType collationType;

    @Override
    public Object visitObExprSubstr(ObExprGrammerParser.ObExprSubstrContext ctx) {
        Object params = visit(ctx.exprs_with_comma());
        if (!(params instanceof ArrayList)) {
            throw new IllegalArgumentException("invalid params type, params: " + params);
        }

        ObGeneratedColumnSubStrFunc subStrFunc = new ObGeneratedColumnSubStrFunc();
        return (Object) subStrFunc.evalValue(collationType, (ArrayList)params);
    }

    public Object visitObExprColumnRefExpr(ObExprGrammerParser.ObExprColumnRefExprContext ctx) {
        return rowMap.get(ctx.ID().getText());
    }

    @Override
    public Object visitObExprIntExpr(ObExprGrammerParser.ObExprIntExprContext ctx) {
        String s = ctx.getText();
        return (Object) Integer.valueOf(s);
    }

    @Override
    public Object visitExprs_with_comma(ObExprGrammerParser.Exprs_with_commaContext ctx) {
        ArrayList<Object> res = new ArrayList<>();
        res.add(visit(ctx.expr()));
        if (ctx.exprs_with_comma() != null) {
            res.addAll((List<Object>)visit(ctx.exprs_with_comma()));
        }
        return (Object) res;
    }

    @Override public Object visitObExprSubStringIndex(ObExprGrammerParser.ObExprSubStringIndexContext ctx) {
        Object params = visit(ctx.exprs_with_comma());
        if (!(params instanceof ArrayList)) {
            throw new IllegalArgumentException("invalid params type, params: " + params);
        }

        ObGeneratedColumnSubStrIndexFunc subStrIndexFunc = new ObGeneratedColumnSubStrIndexFunc();
        return (Object) subStrIndexFunc.evalValue(collationType, (ArrayList)params);
    }
}


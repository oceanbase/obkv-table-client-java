grammar ObExprGrammer;

@header {
package com.alipay.oceanbase.rpc.protocol.payload.impl.parser.gen;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.*;
}

import ObExprLexer;

// parser
expr
    : column_ref
    | const_expr
    | func_expr
;

column_ref
    : ID # ObExprColumnRefExpr
;

const_expr
    : INT_LITERAL # ObExprIntExpr
;

func_expr
    : substr_or_substring LBRAC exprs_with_comma RBRAC # ObExprSubstr
    | SUBSTRING_INDEX LBRAC exprs_with_comma RBRAC # ObExprSubStringIndex
;

substr_or_substring: SUBSTR | SUBSTRING;

exprs_with_comma
    : expr
    | expr COMMA exprs_with_comma
;

//substring_index_expr : SUBSTRING_INDEX LBRAC substring_index_params RBRAC ;
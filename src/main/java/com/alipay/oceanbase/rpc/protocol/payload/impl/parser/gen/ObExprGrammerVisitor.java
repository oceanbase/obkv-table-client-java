package com.alipay.oceanbase.rpc.protocol.payload.impl.parser.gen;


import org.antlr.v4.runtime.tree.ParseTreeVisitor;

/**
 * This interface defines a complete generic visitor for a parse tree produced
 * by {@link ObExprGrammerParser}.
 *
 * @param <T> The return type of the visit operation. Use {@link Void} for
 * operations with no return type.
 */
public interface ObExprGrammerVisitor<T> extends ParseTreeVisitor<T> {
	/**
	 * Visit a parse tree produced by {@link ObExprGrammerParser#expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExpr(ObExprGrammerParser.ExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ObExprColumnRefExpr}
	 * labeled alternative in {@link ObExprGrammerParser#column_ref}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitObExprColumnRefExpr(ObExprGrammerParser.ObExprColumnRefExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ObExprIntExpr}
	 * labeled alternative in {@link ObExprGrammerParser#const_expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitObExprIntExpr(ObExprGrammerParser.ObExprIntExprContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ObExprSubstr}
	 * labeled alternative in {@link ObExprGrammerParser#func_expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitObExprSubstr(ObExprGrammerParser.ObExprSubstrContext ctx);
	/**
	 * Visit a parse tree produced by the {@code ObExprSubStringIndex}
	 * labeled alternative in {@link ObExprGrammerParser#func_expr}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitObExprSubStringIndex(ObExprGrammerParser.ObExprSubStringIndexContext ctx);
	/**
	 * Visit a parse tree produced by {@link ObExprGrammerParser#substr_or_substring}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitSubstr_or_substring(ObExprGrammerParser.Substr_or_substringContext ctx);
	/**
	 * Visit a parse tree produced by {@link ObExprGrammerParser#exprs_with_comma}.
	 * @param ctx the parse tree
	 * @return the visitor result
	 */
	T visitExprs_with_comma(ObExprGrammerParser.Exprs_with_commaContext ctx);
}
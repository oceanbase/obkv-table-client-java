package com.alipay.oceanbase.rpc.protocol.payload.impl.parser.gen;

import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.tree.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast", "CheckReturnValue"})
public class ObExprGrammerParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.13.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		SUBSTR=1, SUBSTRING=2, SUBSTRING_INDEX=3, LBRAC=4, RBRAC=5, COMMA=6, ID=7, 
		WS=8, DECIMAL_LITERAL=9;
	public static final int
		RULE_expr = 0, RULE_column_ref = 1, RULE_const_expr = 2, RULE_func_expr = 3, 
		RULE_substr_or_substring = 4, RULE_exprs_with_comma = 5;
	private static String[] makeRuleNames() {
		return new String[] {
			"expr", "column_ref", "const_expr", "func_expr", "substr_or_substring", 
			"exprs_with_comma"
		};
	}
	public static final String[] ruleNames = makeRuleNames();

	private static String[] makeLiteralNames() {
		return new String[] {
			null, "'substr'", "'substring'", "'substring_index'", "'('", "')'", "','"
		};
	}
	private static final String[] _LITERAL_NAMES = makeLiteralNames();
	private static String[] makeSymbolicNames() {
		return new String[] {
			null, "SUBSTR", "SUBSTRING", "SUBSTRING_INDEX", "LBRAC", "RBRAC", "COMMA", 
			"ID", "WS", "DECIMAL_LITERAL"
		};
	}
	private static final String[] _SYMBOLIC_NAMES = makeSymbolicNames();
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "ObExprGrammer.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public ObExprGrammerParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@SuppressWarnings("CheckReturnValue")
	public static class ExprContext extends ParserRuleContext {
		public Column_refContext column_ref() {
			return getRuleContext(Column_refContext.class,0);
		}
		public Const_exprContext const_expr() {
			return getRuleContext(Const_exprContext.class,0);
		}
		public Func_exprContext func_expr() {
			return getRuleContext(Func_exprContext.class,0);
		}
		public ExprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expr; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ObExprGrammerVisitor ) return ((ObExprGrammerVisitor<? extends T>)visitor).visitExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExprContext expr() throws RecognitionException {
		ExprContext _localctx = new ExprContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_expr);
		try {
			setState(15);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case ID:
				enterOuterAlt(_localctx, 1);
				{
				setState(12);
				column_ref();
				}
				break;
			case DECIMAL_LITERAL:
				enterOuterAlt(_localctx, 2);
				{
				setState(13);
				const_expr();
				}
				break;
			case SUBSTR:
			case SUBSTRING:
			case SUBSTRING_INDEX:
				enterOuterAlt(_localctx, 3);
				{
				setState(14);
				func_expr();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Column_refContext extends ParserRuleContext {
		public Column_refContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_column_ref; }
	 
		public Column_refContext() { }
		public void copyFrom(Column_refContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ObExprColumnRefExprContext extends Column_refContext {
		public TerminalNode ID() { return getToken(ObExprGrammerParser.ID, 0); }
		public ObExprColumnRefExprContext(Column_refContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ObExprGrammerVisitor ) return ((ObExprGrammerVisitor<? extends T>)visitor).visitObExprColumnRefExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Column_refContext column_ref() throws RecognitionException {
		Column_refContext _localctx = new Column_refContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_column_ref);
		try {
			_localctx = new ObExprColumnRefExprContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(17);
			match(ID);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Const_exprContext extends ParserRuleContext {
		public Const_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_const_expr; }
	 
		public Const_exprContext() { }
		public void copyFrom(Const_exprContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ObExprIntExprContext extends Const_exprContext {
		public TerminalNode DECIMAL_LITERAL() { return getToken(ObExprGrammerParser.DECIMAL_LITERAL, 0); }
		public ObExprIntExprContext(Const_exprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ObExprGrammerVisitor ) return ((ObExprGrammerVisitor<? extends T>)visitor).visitObExprIntExpr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Const_exprContext const_expr() throws RecognitionException {
		Const_exprContext _localctx = new Const_exprContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_const_expr);
		try {
			_localctx = new ObExprIntExprContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(19);
			match(DECIMAL_LITERAL);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Func_exprContext extends ParserRuleContext {
		public Func_exprContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_func_expr; }
	 
		public Func_exprContext() { }
		public void copyFrom(Func_exprContext ctx) {
			super.copyFrom(ctx);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ObExprSubstrContext extends Func_exprContext {
		public Substr_or_substringContext substr_or_substring() {
			return getRuleContext(Substr_or_substringContext.class,0);
		}
		public TerminalNode LBRAC() { return getToken(ObExprGrammerParser.LBRAC, 0); }
		public Exprs_with_commaContext exprs_with_comma() {
			return getRuleContext(Exprs_with_commaContext.class,0);
		}
		public TerminalNode RBRAC() { return getToken(ObExprGrammerParser.RBRAC, 0); }
		public ObExprSubstrContext(Func_exprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ObExprGrammerVisitor ) return ((ObExprGrammerVisitor<? extends T>)visitor).visitObExprSubstr(this);
			else return visitor.visitChildren(this);
		}
	}
	@SuppressWarnings("CheckReturnValue")
	public static class ObExprSubStringIndexContext extends Func_exprContext {
		public TerminalNode SUBSTRING_INDEX() { return getToken(ObExprGrammerParser.SUBSTRING_INDEX, 0); }
		public TerminalNode LBRAC() { return getToken(ObExprGrammerParser.LBRAC, 0); }
		public Exprs_with_commaContext exprs_with_comma() {
			return getRuleContext(Exprs_with_commaContext.class,0);
		}
		public TerminalNode RBRAC() { return getToken(ObExprGrammerParser.RBRAC, 0); }
		public ObExprSubStringIndexContext(Func_exprContext ctx) { copyFrom(ctx); }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ObExprGrammerVisitor ) return ((ObExprGrammerVisitor<? extends T>)visitor).visitObExprSubStringIndex(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Func_exprContext func_expr() throws RecognitionException {
		Func_exprContext _localctx = new Func_exprContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_func_expr);
		try {
			setState(31);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case SUBSTR:
			case SUBSTRING:
				_localctx = new ObExprSubstrContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(21);
				substr_or_substring();
				setState(22);
				match(LBRAC);
				setState(23);
				exprs_with_comma();
				setState(24);
				match(RBRAC);
				}
				break;
			case SUBSTRING_INDEX:
				_localctx = new ObExprSubStringIndexContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(26);
				match(SUBSTRING_INDEX);
				setState(27);
				match(LBRAC);
				setState(28);
				exprs_with_comma();
				setState(29);
				match(RBRAC);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Substr_or_substringContext extends ParserRuleContext {
		public TerminalNode SUBSTR() { return getToken(ObExprGrammerParser.SUBSTR, 0); }
		public TerminalNode SUBSTRING() { return getToken(ObExprGrammerParser.SUBSTRING, 0); }
		public Substr_or_substringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_substr_or_substring; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ObExprGrammerVisitor ) return ((ObExprGrammerVisitor<? extends T>)visitor).visitSubstr_or_substring(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Substr_or_substringContext substr_or_substring() throws RecognitionException {
		Substr_or_substringContext _localctx = new Substr_or_substringContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_substr_or_substring);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(33);
			_la = _input.LA(1);
			if ( !(_la==SUBSTR || _la==SUBSTRING) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	@SuppressWarnings("CheckReturnValue")
	public static class Exprs_with_commaContext extends ParserRuleContext {
		public ExprContext expr() {
			return getRuleContext(ExprContext.class,0);
		}
		public TerminalNode COMMA() { return getToken(ObExprGrammerParser.COMMA, 0); }
		public Exprs_with_commaContext exprs_with_comma() {
			return getRuleContext(Exprs_with_commaContext.class,0);
		}
		public Exprs_with_commaContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_exprs_with_comma; }
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof ObExprGrammerVisitor ) return ((ObExprGrammerVisitor<? extends T>)visitor).visitExprs_with_comma(this);
			else return visitor.visitChildren(this);
		}
	}

	public final Exprs_with_commaContext exprs_with_comma() throws RecognitionException {
		Exprs_with_commaContext _localctx = new Exprs_with_commaContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_exprs_with_comma);
		try {
			setState(40);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(35);
				expr();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(36);
				expr();
				setState(37);
				match(COMMA);
				setState(38);
				exprs_with_comma();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\u0004\u0001\t+\u0002\u0000\u0007\u0000\u0002\u0001\u0007\u0001\u0002"+
		"\u0002\u0007\u0002\u0002\u0003\u0007\u0003\u0002\u0004\u0007\u0004\u0002"+
		"\u0005\u0007\u0005\u0001\u0000\u0001\u0000\u0001\u0000\u0003\u0000\u0010"+
		"\b\u0000\u0001\u0001\u0001\u0001\u0001\u0002\u0001\u0002\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001\u0003\u0001"+
		"\u0003\u0001\u0003\u0001\u0003\u0003\u0003 \b\u0003\u0001\u0004\u0001"+
		"\u0004\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0001\u0005\u0003"+
		"\u0005)\b\u0005\u0001\u0005\u0000\u0000\u0006\u0000\u0002\u0004\u0006"+
		"\b\n\u0000\u0001\u0001\u0000\u0001\u0002(\u0000\u000f\u0001\u0000\u0000"+
		"\u0000\u0002\u0011\u0001\u0000\u0000\u0000\u0004\u0013\u0001\u0000\u0000"+
		"\u0000\u0006\u001f\u0001\u0000\u0000\u0000\b!\u0001\u0000\u0000\u0000"+
		"\n(\u0001\u0000\u0000\u0000\f\u0010\u0003\u0002\u0001\u0000\r\u0010\u0003"+
		"\u0004\u0002\u0000\u000e\u0010\u0003\u0006\u0003\u0000\u000f\f\u0001\u0000"+
		"\u0000\u0000\u000f\r\u0001\u0000\u0000\u0000\u000f\u000e\u0001\u0000\u0000"+
		"\u0000\u0010\u0001\u0001\u0000\u0000\u0000\u0011\u0012\u0005\u0007\u0000"+
		"\u0000\u0012\u0003\u0001\u0000\u0000\u0000\u0013\u0014\u0005\t\u0000\u0000"+
		"\u0014\u0005\u0001\u0000\u0000\u0000\u0015\u0016\u0003\b\u0004\u0000\u0016"+
		"\u0017\u0005\u0004\u0000\u0000\u0017\u0018\u0003\n\u0005\u0000\u0018\u0019"+
		"\u0005\u0005\u0000\u0000\u0019 \u0001\u0000\u0000\u0000\u001a\u001b\u0005"+
		"\u0003\u0000\u0000\u001b\u001c\u0005\u0004\u0000\u0000\u001c\u001d\u0003"+
		"\n\u0005\u0000\u001d\u001e\u0005\u0005\u0000\u0000\u001e \u0001\u0000"+
		"\u0000\u0000\u001f\u0015\u0001\u0000\u0000\u0000\u001f\u001a\u0001\u0000"+
		"\u0000\u0000 \u0007\u0001\u0000\u0000\u0000!\"\u0007\u0000\u0000\u0000"+
		"\"\t\u0001\u0000\u0000\u0000#)\u0003\u0000\u0000\u0000$%\u0003\u0000\u0000"+
		"\u0000%&\u0005\u0006\u0000\u0000&\'\u0003\n\u0005\u0000\')\u0001\u0000"+
		"\u0000\u0000(#\u0001\u0000\u0000\u0000($\u0001\u0000\u0000\u0000)\u000b"+
		"\u0001\u0000\u0000\u0000\u0003\u000f\u001f(";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}
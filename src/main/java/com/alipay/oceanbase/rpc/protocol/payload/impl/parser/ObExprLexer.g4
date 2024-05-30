lexer grammar ObExprLexer;

@header{}
// lexer
SUBSTR: 'substr';
SUBSTRING: 'substring';
SUBSTRING_INDEX: 'substring_index';

LBRAC: '(';
RBRAC: ')';
COMMA: ',';
ID: IDEN_LITERAL;

WS:                               [ \t\r\n]+    -> channel(HIDDEN);
INT_LITERAL               : DIGIT+;

fragment IDEN_LITERAL: (LETTER | '_') (LETTER | DIGIT | '_')*;
fragment DIGIT: [0-9];
fragment LETTER : [a-zA-Z];
fragment EXPONENT_NUM_PART : 'E' [-+]? DIGIT+;

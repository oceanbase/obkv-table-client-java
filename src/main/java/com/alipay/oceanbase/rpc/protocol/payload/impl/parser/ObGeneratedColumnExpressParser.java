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

package com.alipay.oceanbase.rpc.protocol.payload.impl.parser;

import com.alipay.oceanbase.rpc.exception.GenerateColumnParseException;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnNegateFunc;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnReferFunc;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnSimpleFunc;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnSubStrFunc;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnFuncName.funcToToken;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnSimpleToken.*;

public class ObGeneratedColumnExpressParser {

    private final ObGeneratedColumnLexer lexer;

    /*
     * Ob generated column express parser.
     */
    public ObGeneratedColumnExpressParser(String text) {
        this.lexer = new ObGeneratedColumnLexer(text);
    }

    /*
     * Parse.
     */
    public ObGeneratedColumnSimpleFunc parse() throws GenerateColumnParseException {
        switch (lexer.token()) {
            case IDENTIFIER:
                String stringVal = lexer.stringVal();
                lexer.nextToken();
                switch (lexer.token()) {
                    case EOF:
                        return new ObGeneratedColumnReferFunc(stringVal);
                    case LPAREN:
                        ObGeneratedColumnFuncName funcName = funcToToken(stringVal.toLowerCase());
                        switch (funcName) {
                            case SUB_STR:
                                ObGeneratedColumnSubStrFunc subStr = new ObGeneratedColumnSubStrFunc();
                                lexer.nextToken();
                                listParameters(subStr);
                                return subStr;
                            default:
                                throw new GenerateColumnParseException("");
                        }
                    default:
                        throw new GenerateColumnParseException("");
                }
            case SUB:
                // -(T)
                // If the first token is a '-' sign, then generate the opposite number expression.
                lexer.nextToken();
                if (lexer.token().equals(LPAREN)) {
                    lexer.nextToken();
                }
                String refColumn = lexer.stringVal();
                return new ObGeneratedColumnNegateFunc(refColumn);
            default:
                throw new GenerateColumnParseException("ERROR. token :  " + lexer.token()
                                                       + ", pos : " + lexer.pos());
        }
    }

    protected void listParameters(ObGeneratedColumnSimpleFunc func) {
        List<Object> args = new ArrayList<Object>();
        args.add(primaryParameters());
        while (lexer.token() == COMMA) {
            lexer.nextToken();
            args.add(primaryParameters());
        }

        accept(RPAREN);

        if (func.getMinParameters() > args.size() || func.getMaxParameters() < args.size()) {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < args.size(); i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(args.get(i));
            }
            sb.append(")");
            throw new GenerateColumnParseException("illegal func args. expect size  "
                                                   + func.getMinParameters() + "~"
                                                   + func.getMaxParameters() + " but found"
                                                   + sb.toString());
        }
        try {
            func.setParameters(args);
        } catch (IllegalArgumentException e) {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            for (int i = 0; i < args.size(); i++) {
                if (i != 0) {
                    sb.append(",");
                }
                sb.append(args.get(i));
            }
            sb.append(")");
            throw new GenerateColumnParseException("illegal func args :" + sb.toString(), e);
        }
    }

    protected Object primaryParameters() {
        final ObGeneratedColumnSimpleToken tok = lexer.token();
        switch (tok) {
            case IDENTIFIER:
                String stringVal = lexer.stringVal();
                lexer.nextToken();
                return stringVal;
            case LITERAL_HEX:
                String hexStr = lexer.addSymbol();
                lexer.nextToken();
                return Long.parseLong(hexStr, 16);
            case LITERAL_INT:
                Number integerValue = lexer.integerValue();
                lexer.nextToken();
                return integerValue.longValue();
            case SUB:
                lexer.nextToken();
                switch (lexer.token()) {
                    case LITERAL_INT:
                        integerValue = lexer.integerValue();
                        if (integerValue instanceof Integer) {
                            int intVal = integerValue.intValue();
                            if (intVal == Integer.MIN_VALUE) {
                                integerValue = ((long) intVal) * -1;
                            } else {
                                integerValue = intVal * -1;
                            }
                        } else if (integerValue instanceof Long) {
                            long longVal = (Long) integerValue;
                            if (longVal == 2147483648L) {
                                integerValue = (int) (((long) longVal) * -1);
                            } else {
                                integerValue = longVal * -1;
                            }
                        } else {
                            integerValue = ((BigInteger) integerValue).negate();
                        }
                        lexer.nextToken();
                        return integerValue.longValue();
                    case LITERAL_HEX:
                        hexStr = lexer.addSymbol();
                        lexer.nextToken();
                        return Long.parseLong(hexStr, 16) * -1;
                    default:
                        throw new GenerateColumnParseException("unsupported  token : " + tok.name
                                                               + ", after - pos : " + lexer.pos());
                }
            default:
                throw new GenerateColumnParseException("ERROR. token : " + tok + ", pos : "
                                                       + lexer.pos());
        }
    }

    /*
     * Accept.
     */
    public void accept(ObGeneratedColumnSimpleToken token) {
        if (lexer.token() == token) {
            lexer.nextToken();
        } else {
            throw new GenerateColumnParseException("syntax error, expect " + token + ", actual "
                                                   + lexer.token() + " " + lexer.stringVal());
        }
    }
}

/*-
 * #%L
 * OceanBase Table Client Framework
 * %%
 * Copyright (C) 2016 - 2018 Ant Financial Services Group
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

package com.alipay.oceanbase.rpc.dds.parser;

import com.alipay.oceanbase.rpc.exception.DistributeRuleParseException;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.dds.parser.DistributeRuleFuncName.funcToToken;
import static com.alipay.oceanbase.rpc.dds.parser.DistributeRuleSimpleToken.COMMA;
import static com.alipay.oceanbase.rpc.dds.parser.DistributeRuleSimpleToken.RPAREN;

public class DistributeRuleExpressParser {

    private final DistributeRuleLexer lexer;

    /**
     * Ob generated column express parser.
     */
    public DistributeRuleExpressParser(String text) {
        this.lexer = new DistributeRuleLexer(text);
    }

    /**
     * Parse.
     */
    public DistributeRuleSimpleFunc parse() throws DistributeRuleParseException {
        switch (lexer.token()) {
            case IDENTIFIER:
                String stringVal = lexer.stringVal();
                lexer.nextToken();
                switch (lexer.token()) {
                    case EOF:
                        return new DistributeRuleReferFunc(stringVal);
                    case LPAREN:
                        DistributeRuleFuncName funcName = funcToToken(stringVal.toLowerCase());
                        switch (funcName) {
                            case SUB_STR:
                                DistributeRuleSubStrFunc subStr = new DistributeRuleSubStrFunc();
                                lexer.nextToken();
                                listParameters(subStr);
                                return subStr;
                            case SUB_STR_LOAD_TEST:
                                DistributeRuleSubStrLoadTestFunc subStrLoadTest = new DistributeRuleSubStrLoadTestFunc();
                                lexer.nextToken();
                                listParameters(subStrLoadTest);
                                return subStrLoadTest;
                            case SUB_STR_REVERSE:
                                DistributeRuleSubStrReverseFunc subStrRerverse = new DistributeRuleSubStrReverseFunc();
                                lexer.nextToken();
                                listParameters(subStrRerverse);
                                return subStrRerverse;
                            case SUB_STR_REVERSE_LOAD_TEST:
                                DistributeRuleSubStrReverseLoadTestFunc subStrRerverseLoadTest = new DistributeRuleSubStrReverseLoadTestFunc();
                                lexer.nextToken();
                                listParameters(subStrRerverseLoadTest);
                                return subStrRerverseLoadTest;
                            case HASH_STR:
                                DistributeRuleHashStrFunc hashStr = new DistributeRuleHashStrFunc();
                                lexer.nextToken();
                                listParameters(hashStr);
                                return hashStr;
                            case HASH_DIVISION_STR:
                                DistributeRuleHashDivisionStrFunc hashDivisionStr = new DistributeRuleHashDivisionStrFunc();
                                lexer.nextToken();
                                listParameters(hashDivisionStr);
                                return hashDivisionStr;
                            case HASH_SALT:
                                DistributeRuleHashSaltFunc hashSalt = new DistributeRuleHashSaltFunc();
                                lexer.nextToken();
                                listParameters(hashSalt);
                                return hashSalt;
                            case HASH_SALT_DIVISION:
                                DistributeRuleHashSaltDivisionFunc hashSaltDivision = new DistributeRuleHashSaltDivisionFunc();
                                lexer.nextToken();
                                listParameters(hashSaltDivision);
                                return hashSaltDivision;
                            case HASH_SUB_STR:
                                DistributeRuleHashSubStrFunc hashSubStrFunc = new DistributeRuleHashSubStrFunc();
                                lexer.nextToken();
                                listParameters(hashSubStrFunc);
                                return hashSubStrFunc;
                            case HASH_MULTIPLE_FIELD:
                                DistributeRuleHashMultipleFieldStrFunc hashMultipleFieldStrFunc = new DistributeRuleHashMultipleFieldStrFunc();
                                lexer.nextToken();
                                listParameters(hashMultipleFieldStrFunc);
                                return hashMultipleFieldStrFunc;
                            case TO_INT:
                                DistributeRuleToIntFunc toIntFunc = new DistributeRuleToIntFunc();
                                lexer.nextToken();
                                listParameters(toIntFunc);
                                return toIntFunc;
                            default:
                                throw new DistributeRuleParseException("");
                        }
                    default:
                        throw new DistributeRuleParseException("");
                }
            default:
                throw new DistributeRuleParseException("ERROR. token :  " + lexer.token()
                                                       + ", pos : " + lexer.pos());
        }
    }

    protected void listParameters(DistributeRuleSimpleFunc func) {
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
            throw new DistributeRuleParseException("illegal func args. expect size  "
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
            throw new DistributeRuleParseException("illegal func args :" + sb.toString(), e);
        }
    }

    protected Object primaryParameters() {
        final DistributeRuleSimpleToken tok = lexer.token();
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
                        throw new DistributeRuleParseException("unsupported  token : " + tok.name
                                                               + ", after - pos : " + lexer.pos());
                }
            default:
                throw new DistributeRuleParseException("ERROR. token : " + tok + ", pos : "
                                                       + lexer.pos());
        }
    }

    /**
     * Accept.
     */
    public void accept(DistributeRuleSimpleToken token) {
        if (lexer.token() == token) {
            lexer.nextToken();
        } else {
            throw new DistributeRuleParseException("syntax error, expect " + token + ", actual "
                                                   + lexer.token() + " " + lexer.stringVal());
        }
    }
}

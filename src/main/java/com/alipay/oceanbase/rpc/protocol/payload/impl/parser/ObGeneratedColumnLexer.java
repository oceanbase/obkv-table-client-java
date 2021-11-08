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

import java.math.BigInteger;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnCharType.*;
import static com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnSimpleToken.*;

public class ObGeneratedColumnLexer {

    private final String                   text;
    protected char                         ch;
    private int                            pos;
    private int                            mark;
    protected ObGeneratedColumnSimpleToken token;
    protected char[]                       buf;
    private int                            bufPos;
    protected String                       stringVal;

    /**
     * Ob generated column lexer.
     */
    public ObGeneratedColumnLexer(String text) {
        this.text = text;
        this.pos = -1;
        scanChar();
        nextToken();
    }

    protected void nextToken() {
        bufPos = 0;
        for (;;) {
            if (isWhitespace(ch)) {
                scanChar();
                continue;
            }

            if (isFirstIdentifierChar(ch)) {
                scanIdentifier();
                return;
            }

            switch (ch) {
                case '0':
                    if (charAt(pos + 1) == 'x') {
                        scanChar();
                        scanChar();
                        scanHexaDecimal();
                    } else {
                        scanNumber();
                    }
                    return;
                case '1':
                case '2':
                case '3':
                case '4':
                case '5':
                case '6':
                case '7':
                case '8':
                case '9':
                    scanNumber();
                    return;
                case ',':
                case '，':
                    scanChar();
                    token = COMMA;
                    return;
                case '(':
                    scanChar();
                    token = LPAREN;
                    return;
                case ')':
                    scanChar();
                    token = RPAREN;
                    return;
                case '-':
                    scanOperator();
                    return;
                default:
                    if (isEOF()) { // JLS
                        token = EOF;
                    } else {
                        lexError("illegal.char", String.valueOf((int) ch));
                        scanChar();
                    }
                    return;
            }
        }

    }

    protected void lexError(String key, Object... args) {
        token = ERROR;
    }

    protected final void scanChar() {
        ch = charAt(++pos);
    }

    public final char charAt(int index) {
        if (index >= text.length()) {
            return LayoutCharacters.EOI;
        }

        return text.charAt(index);
    }

    protected void skipWhitespace() {
        for (;;) {
            if (isEOF()) {
                return;
            }
            if (isWhitespace(ch)) {
                scanChar();
            } else {
                break;
            }
        }
    }

    /**
     * Scan identifier.
     */
    public void scanIdentifier() {
        skipWhitespace();
        final char first = ch;

        final boolean firstFlag = isFirstIdentifierChar(first);
        if (!firstFlag) {
            throw new GenerateColumnParseException("illegal identifier");
        }

        mark = pos;
        bufPos = 1;
        char ch;
        for (;;) {
            ch = charAt(++pos);

            if (!isIdentifierChar(ch)) {
                break;
            }

            bufPos++;
        }

        this.ch = charAt(pos);

        stringVal = addSymbol();

        token = IDENTIFIER;
    }

    /**
     * Scan hexa decimal.
     */
    public void scanHexaDecimal() {
        mark = pos;

        if (ch == '-') {
            bufPos++;
            ch = charAt(++pos);
        }

        for (;;) {
            if (ObGeneratedColumnCharType.isHex(ch)) {
                bufPos++;
            } else {
                break;
            }
            ch = charAt(++pos);
        }

        token = LITERAL_HEX;
    }

    /**
     * Scan number.
     */
    public void scanNumber() {
        mark = pos;
        if (ch == '-') {
            bufPos++;
            ch = charAt(++pos);
        }

        for (;;) {
            if (ch >= '0' && ch <= '9') {
                bufPos++;
            } else {
                break;
            }
            ch = charAt(++pos);
        }

        boolean isDouble = false;

        if (ch == '.') {
            if (charAt(pos + 1) == '.') {
                token = LITERAL_INT;
                return;
            }
            bufPos++;
            ch = charAt(++pos);
            isDouble = true;

            for (;;) {
                if (ch >= '0' && ch <= '9') {
                    bufPos++;
                } else {
                    break;
                }
                ch = charAt(++pos);
            }
        }

        if (ch == 'e' || ch == 'E') {
            bufPos++;
            ch = charAt(++pos);

            if (ch == '+' || ch == '-') {
                bufPos++;
                ch = charAt(++pos);
            }

            for (;;) {
                if (ch >= '0' && ch <= '9') {
                    bufPos++;
                } else {
                    break;
                }
                ch = charAt(++pos);
            }

            isDouble = true;
        }

        if (isDouble) {
            token = LITERAL_FLOAT;
        } else {
            token = LITERAL_INT;
        }
    }

    private void scanOperator() {
        switch (ch) {
            case '+':
                scanChar();
                token = PLUS;
                break;
            case '-':
                scanChar();
                token = SUB;
                break;
            default:
                //TODO
                token = ERROR;
        }
    }

    /**
     * Is e o f.
     */
    public boolean isEOF() {
        return pos >= text.length();
    }

    public final String addSymbol() {
        return subString(mark, bufPos);
    }

    public final String subString(int offset, int count) {
        return text.substring(offset, offset + count);
    }

    private static final long  MULTMIN_RADIX_TEN   = Long.MIN_VALUE / 10;
    private static final long  N_MULTMAX_RADIX_TEN = -Long.MAX_VALUE / 10;

    private final static int[] digits              = new int[(int) '9' + 1];

    static {
        for (int i = '0'; i <= '9'; ++i) {
            digits[i] = i - '0';
        }
    }

    /**
     * Integer value.
     */
    public Number integerValue() {
        long result = 0;
        boolean negative = false;
        int i = mark, max = mark + bufPos;
        long limit;
        long multmin;
        int digit;

        if (charAt(mark) == '-') {
            negative = true;
            limit = Long.MIN_VALUE;
            i++;
        } else {
            limit = -Long.MAX_VALUE;
        }
        multmin = negative ? MULTMIN_RADIX_TEN : N_MULTMAX_RADIX_TEN;
        if (i < max) {
            digit = digits[charAt(i++)];
            result = -digit;
        }
        while (i < max) {
            // Accumulating negatively avoids surprises near MAX_VALUE
            digit = digits[charAt(i++)];
            if (result < multmin) {
                return new BigInteger(addSymbol());
            }
            result *= 10;
            if (result < limit + digit) {
                return new BigInteger(addSymbol());
            }
            result -= digit;
        }

        if (negative) {
            if (i > mark + 1) {
                if (result >= Integer.MIN_VALUE) {
                    return (int) result;
                }
                return result;
            } else { /* Only got "-" */
                throw new NumberFormatException(addSymbol());
            }
        } else {
            result = -result;
            if (result <= Integer.MAX_VALUE) {
                return (int) result;
            }
            return result;
        }
    }

    /**
     * Token.
     */
    public ObGeneratedColumnSimpleToken token() {
        return token;
    }

    /**
     * String val.
     */
    public String stringVal() {
        return stringVal;
    }

    /**
     * Pos.
     */
    public int pos() {
        return pos;
    }
}

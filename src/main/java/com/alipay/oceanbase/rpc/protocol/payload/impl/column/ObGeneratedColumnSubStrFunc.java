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
import com.alipay.oceanbase.rpc.util.Serialization;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Charsets.UTF_8;

public class ObGeneratedColumnSubStrFunc implements ObGeneratedColumnSimpleFunc {

    private List<String> refColumnNames = new ArrayList<String>();
    private int          pos            = 0;
    private int          len            = Integer.MIN_VALUE;

    /**
     * Get pos.
     */
    public int getPos() {
        return pos;
    }

    /**
     * Set pos.
     */
    public void setPos(int pos) {
        this.pos = pos;
    }

    /**
     * Get len.
     */
    public int getLen() {
        return len;
    }

    /**
     * Set len.
     */
    public void setLen(int len) {
        this.len = len;
    }

    /**
     * Set parameters.
     */
    @Override
    public void setParameters(List<Object> parameters) throws IllegalArgumentException {
        Object parameter1 = parameters.get(0);

        if (!(parameter1 instanceof String)) {
            throw new IllegalArgumentException("substr first argument must be column or string "
                                               + parameter1);
        }
        String ref = (String) parameters.get(0);
        refColumnNames.add(ref);

        Object parameter2 = parameters.get(1);

        if (!((parameter2 instanceof Long || (parameter2 instanceof Integer)))) {
            throw new IllegalArgumentException("substr second argument pos must be int "
                                               + parameter2);
        }

        Long pos = ((Number) parameters.get(1)).longValue();

        if (pos == 0 || pos > Integer.MAX_VALUE || pos < Integer.MIN_VALUE) {
            throw new IllegalArgumentException(
                "substr second argument pos must be int exclude zero");
        }

        this.pos = pos.intValue();

        if (parameters.size() == 3) {

            Object parameter3 = parameters.get(2);

            if (!((parameter3 instanceof Long) || (parameter3 instanceof Integer))) {
                throw new IllegalArgumentException("substr third argument len must be int "
                                                   + parameter3);
            }

            Long len = ((Number) parameters.get(2)).longValue();

            if (len <= 0 || len > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("substr third argument len must be positive int");
            }
            this.len = len.intValue();
        }
    }

    /**
     * Get min parameters.
     */
    @Override
    public int getMinParameters() {
        return 2;
    }

    /**
     * Get max parameters.
     */
    @Override
    public int getMaxParameters() {
        return 3;
    }

    /**
     * Get ref column names.
     */
    @Override
    public List<String> getRefColumnNames() {
        return refColumnNames;
    }

    /**
     * Eval value.
     */
    @Override
    public Object evalValue(ObCollationType collationType, Object... refs)
                                                                          throws IllegalArgumentException {

        if (refs.length != refColumnNames.size()) {
            throw new IllegalArgumentException("ObGeneratedColumnSubStrFunc is refer to "
                                               + refColumnNames
                                               + " so that the length of the refs must be equal "
                                               + refColumnNames.size() + ". refs:"
                                               + Arrays.toString(refs));
        }
        Object ref = refs[0];

        if (collationType == ObCollationType.CS_TYPE_BINARY) {
            byte[] evalBytes;
            if (ref instanceof String) {
                evalBytes = ((String) ref).getBytes(UTF_8);
            } else if (ref instanceof byte[]) {
                evalBytes = (byte[]) ref;
            } else {
                throw new IllegalArgumentException(
                    "Object ["
                            + ref
                            + "] can not evaluate by ObGeneratedColumnSubStrFunc with collationType ["
                            + collationType + "]");
            }
            int evalBytesLen = evalBytes.length;
            if (pos > 0) {
                if (pos > evalBytesLen) {
                    throw new IllegalArgumentException("the length of param :" + evalBytesLen
                                                       + " is less than the pos " + pos);
                } else {

                    if (len > 0 && pos - 1 + len <= evalBytes.length) {
                        byte[] res = new byte[len];
                        System.arraycopy(evalBytes, pos - 1, res, 0, len);
                        return res;
                    }
                    byte[] res = new byte[evalBytesLen - pos + 1];
                    System.arraycopy(evalBytes, pos - 1, res, 0, evalBytesLen - pos + 1);
                    return res;
                }
            } else { // pos can not be 0
                if (-pos > evalBytesLen) {
                    throw new IllegalArgumentException("the length of param:" + evalBytesLen
                                                       + " is less than the pos " + pos);
                } else {
                    int pos = evalBytesLen + this.pos;
                    if (len > 0 && pos + len <= evalBytesLen) {
                        byte[] res = new byte[len];
                        System.arraycopy(evalBytes, pos, res, 0, len);
                        return res;
                    }
                    byte[] res = new byte[evalBytesLen - pos];
                    System.arraycopy(evalBytes, pos, res, 0, evalBytesLen - pos);
                    return res;
                }
            }
        } else {
            String evalStr;
            if (ref instanceof String) {
                evalStr = (String) ref;
            } else if (ref instanceof byte[]) {
                evalStr = Serialization.decodeVString((byte[]) ref);
            } else {
                throw new IllegalArgumentException(
                    "Object ["
                            + ref
                            + "] can not evaluate by ObGeneratedColumnSubStrFunc with collationType ["
                            + collationType + "]");
            }
            int evalStrLen = evalStr.length();
            if (pos > 0) {
                if (pos > evalStrLen) {
                    throw new IllegalArgumentException("the length of param :" + evalStrLen
                                                       + " is less than the pos " + pos);
                } else {
                    if (len > 0 && pos - 1 + len <= evalStrLen) {
                        return evalStr.substring(pos - 1, pos - 1 + len);
                    }
                    return evalStr.substring(pos - 1);
                }
            } else { // pos can not be 0
                if (-pos > evalStrLen) {
                    throw new IllegalArgumentException("the length of param:" + evalStrLen
                                                       + " is less than the pos " + pos);
                } else {
                    int pos = evalStrLen + this.pos;
                    if (len > 0 && pos + len <= evalStrLen) {
                        return evalStr.substring(pos, pos + len);
                    }
                    return evalStr.substring(pos);
                }
            }
        }
    }

    /**
     * To string.
     */
    @Override
    public String toString() {
        return "ObGeneratedColumnSubStrFunc{" + "refColumnNames=" + refColumnNames + ", pos=" + pos
               + ", len=" + len + '}';
    }
}

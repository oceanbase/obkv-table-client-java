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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query;

public class ObBorderFlag {

    public static final byte INCLUSIVE_START = (0x1);
    public static final byte INCLUSIVE_END   = (0x2);
    public static final byte MIN_VALUE       = (0x3);
    public static final byte MAX_VALUE       = (0x8);

    private byte             value           = 0;

    /*
     * Value of.
     */
    public static ObBorderFlag valueOf(byte value) {
        ObBorderFlag obBorderFlag = new ObBorderFlag();
        obBorderFlag.value = value;
        return obBorderFlag;
    }

    /*
     * Get value.
     */
    public byte getValue() {
        return value;
    }

    /*
     * Set inclusive start.
     */
    public void setInclusiveStart() {
        this.value |= INCLUSIVE_START;
    }

    /*
     * Unset inclusive start.
     */
    public void unsetInclusiveStart() {
        this.value &= (~INCLUSIVE_START);
    }

    /*
     * Is inclusive start.
     */
    public boolean isInclusiveStart() {
        return (this.value & INCLUSIVE_START) == INCLUSIVE_START;
    }

    /*
     * Set inclusive end.
     */
    public void setInclusiveEnd() {
        this.value |= INCLUSIVE_END;
    }

    /*
     * Unset inclusive end.
     */
    public void unsetInclusiveEnd() {
        this.value &= (~INCLUSIVE_END);
    }

    /*
     * Is inclusive end.
     */
    public boolean isInclusiveEnd() {
        return (this.value & INCLUSIVE_END) == INCLUSIVE_END;
    }

    /*
     * Set max value.
     */
    public void setMaxValue() {
        this.value |= MAX_VALUE;
    }

    /*
     * Unset max value.
     */
    public void unsetMaxValue() {
        this.value &= (~MAX_VALUE);
    }

    /*
     * Is max value.
     */
    public boolean isMaxValue() {
        return (this.value & MAX_VALUE) == MAX_VALUE;
    }

    /*
     * Set min value.
     */
    public void setMinValue() {
        this.value |= MIN_VALUE;
    }

    /*
     * Unset min value.
     */
    public void unsetMinValue() {
        this.value &= (~MIN_VALUE);
    }

    /*
     * Is min value.
     */
    public boolean isMinValue() {
        return (this.value & MIN_VALUE) == MIN_VALUE;
    }

    /*
     * Equals.
     */
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ObBorderFlag that = (ObBorderFlag) o;
        return value == that.value;
    }

}

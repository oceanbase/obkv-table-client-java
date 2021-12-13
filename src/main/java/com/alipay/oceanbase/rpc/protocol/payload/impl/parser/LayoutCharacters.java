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

public interface LayoutCharacters {

    /*
     * Tabulator column increment.
     */
    int  TabInc = 8;

    /*
     * Tabulator character.
     */
    byte TAB    = 0x8;

    /*
     * Line feed character.
     */
    byte LF     = 0xA;

    /*
     * Form feed character.
     */
    byte FF     = 0xC;

    /*
     * Carriage return character.
     */
    byte CR     = 0xD;

    /*
     * End of input character. Used as a sentinel to denote the character one beyond the last defined character in a
     * source file.
     */
    byte EOI    = 0x1A;
}

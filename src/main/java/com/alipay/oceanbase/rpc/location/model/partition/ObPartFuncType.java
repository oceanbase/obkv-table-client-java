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

package com.alipay.oceanbase.rpc.location.model.partition;

public enum ObPartFuncType {
    UNKNOWN("UNKNOWN", -1), HASH("HASH", 0), //
    KEY("KEY", 1), // deprecated type
    KEY_IMPLICIT("KEY_IMPLICIT", 2), RANGE("RANGE", 3), //
    RANGE_COLUMNS("RANGE_COLUMNS", 4), //
    LIST("LIST", 5), // not support yet
    KEY_V2("KEY_V2", 6), //
    LIST_COLUMNS("LIST_COLUMNS", 7), //
    HASH_V2("HASH_V2", 8), //
    KEY_V3("KEY_V3", 9);

    private final String name;
    private final long   index;

    ObPartFuncType(String name, long index) {
        this.name = name;
        this.index = index;
    }

    /*
     * Get ob part func type.
     */
    public static ObPartFuncType getObPartFuncType(long index) {
        if (HASH.index == index) {
            return HASH;
        } else if (KEY.index == index) {
            return KEY;
        } else if (KEY_IMPLICIT.index == index) {
            return KEY_IMPLICIT;
        } else if (RANGE.index == index) {
            return RANGE;
        } else if (RANGE_COLUMNS.index == index) {
            return RANGE_COLUMNS;
        } else if (LIST.index == index) {
            return LIST;
        } else if (KEY_V2.index == index) {
            return KEY_V2;
        } else if (LIST_COLUMNS.index == index) {
            return LIST_COLUMNS;
        } else if (HASH_V2.index == index) {
            return HASH_V2;
        } else if (KEY_V3.index == index) {
            return KEY_V3;
        } else {
            return UNKNOWN;
        }
    }

    /*
     * Get ob part func type.
     */
    public static ObPartFuncType getObPartFuncType(String name) {
        if (HASH.name.equalsIgnoreCase(name)) {
            return HASH;
        } else if (KEY.name.equalsIgnoreCase(name)) {
            return KEY;
        } else if (KEY_IMPLICIT.name.equalsIgnoreCase(name)) {
            return KEY_IMPLICIT;
        } else if (RANGE.name.equalsIgnoreCase(name)) {
            return RANGE;
        } else if (RANGE_COLUMNS.name.equalsIgnoreCase(name)) {
            return RANGE_COLUMNS;
        } else if (LIST.name.equalsIgnoreCase(name)) {
            return LIST;
        } else if (KEY_V2.name.equalsIgnoreCase(name)) {
            return KEY_V2;
        } else if (LIST_COLUMNS.name.equalsIgnoreCase(name)) {
            return LIST_COLUMNS;
        } else if (HASH_V2.name.equalsIgnoreCase(name)) {
            return HASH_V2;
        } else if (KEY_V3.name.equalsIgnoreCase(name)) {
            return KEY_V3;
        } else {
            return UNKNOWN;
        }
    }

    /*
     * Get index.
     */
    public long getIndex() {
        return index;
    }

    /*
     * Get name.
     */
    public String getName() {
        return name;
    }

    /*
     * Is range part.
     */
    public boolean isRangePart() {
        return this.index == RANGE.getIndex() || this.index == RANGE_COLUMNS.getIndex();
    }

    /*
     * Is hash part.
     */
    public boolean isHashPart() {
        return this.index == HASH.getIndex() || this.index == HASH_V2.getIndex();
    }

    /*
     * Is key part.
     */
    public boolean isKeyPart() {
        return this.index == KEY_IMPLICIT.getIndex() || this.index == KEY_V2.getIndex()
               || this.index == KEY_V3.getIndex() || this.index == KEY.getIndex();
    }

    /*
     * Is list part.
     */
    public boolean isListPart() {
        return this.index == LIST.getIndex() || this.index == LIST_COLUMNS.getIndex();
    }
}

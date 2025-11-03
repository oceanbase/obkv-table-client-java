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

package com.alipay.oceanbase.rpc;

public class ObGlobal {
    static long  OB_VSN_MAJOR_SHIFT       = 32;
    static long  OB_VSN_MINOR_SHIFT       = 16;
    static long  OB_VSN_MAJOR_PATCH_SHIFT = 8;
    static long  OB_VSN_MINOR_PATCH_SHIFT = 0;
    static int   OB_VSN_MAJOR_MASK        = 0xffffffff;
    static short OB_VSN_MINOR_MASK        = (short) 0xffff;
    static byte  OB_VSN_MAJOR_PATCH_MASK  = (byte) 0xff;
    static byte  OB_VSN_MINOR_PATCH_MASK  = (byte) 0xff;

    public static long calcVersion(int major, short minor, byte major_patch, byte minor_patch) {
        return ((long) major << OB_VSN_MAJOR_SHIFT) + ((long) minor << OB_VSN_MINOR_SHIFT)
               + ((long) major_patch << OB_VSN_MAJOR_PATCH_SHIFT)
               + ((long) minor_patch << OB_VSN_MINOR_PATCH_SHIFT);
    }

    public static long calcVersion(long major, long minor, long major_patch, long minor_patch) {
        return (major << OB_VSN_MAJOR_SHIFT) + (minor << OB_VSN_MINOR_SHIFT)
               + (major_patch << OB_VSN_MAJOR_PATCH_SHIFT)
               + (minor_patch << OB_VSN_MINOR_PATCH_SHIFT);
    }

    public static long calcVersionForV2(int major, short minor, byte major_patch) {
        return ((long) major << OB_VSN_MAJOR_SHIFT) + ((long) minor << OB_VSN_MINOR_SHIFT)
               + ((long) major_patch << OB_VSN_MAJOR_PATCH_SHIFT);
    }

    public static long calcVersionForV2(long major, long minor, long major_patch) {
        return (major << OB_VSN_MAJOR_SHIFT) + (minor << OB_VSN_MINOR_SHIFT)
               + (major_patch << OB_VSN_MAJOR_PATCH_SHIFT);
    }

    /**
     * @deprecated Use getObVsnMajor(long version) instead with instance version
     */
    @Deprecated
    public static int obVsnMajor() {
        return getObVsnMajor(OB_VERSION);
    }

    public static int getObVsnMajor(long version) {
        return (int) ((version >> OB_VSN_MAJOR_SHIFT) & OB_VSN_MAJOR_MASK);
    }

    /**
     * @deprecated Use getObVsnMinor(long version) instead with instance version
     */
    @Deprecated
    public static short obVsnMinor() {
        return getObVsnMinor(OB_VERSION);
    }

    public static short getObVsnMinor(long version) {
        return (short) ((version >> OB_VSN_MINOR_SHIFT) & OB_VSN_MINOR_MASK);
    }

    /**
     * @deprecated Use getObVsnMajorPatch(long version) instead with instance version
     */
    @Deprecated
    public static byte obVsnMajorPatch() {
        return getObVsnMajorPatch(OB_VERSION);
    }

    public static byte getObVsnMajorPatch(long version) {
        return (byte) ((version >> OB_VSN_MAJOR_PATCH_SHIFT) & OB_VSN_MAJOR_PATCH_MASK);
    }

    /**
     * @deprecated Use getObVsnMinorPatch(long version) instead with instance version
     */
    @Deprecated
    public static byte obVsnMinorPatch() {
        return getObVsnMinorPatch(OB_VERSION);
    }

    public static byte getObVsnMinorPatch(long version) {
        return (byte) ((version >> OB_VSN_MINOR_PATCH_SHIFT) & OB_VSN_MINOR_PATCH_MASK);
    }

    /**
     * @deprecated Use getObVsnString(long version) instead with instance version
     */
    @Deprecated
    public static String obVsnString() {
        return String.format("%d.%d.%d.%d", obVsnMajor(), obVsnMinor(), obVsnMajorPatch(),
            obVsnMinorPatch());
    }

    public static String getObVsnString(long version) {
        return String.format("%d.%d.%d.%d", getObVsnMajor(version), getObVsnMinor(version),
            getObVsnMajorPatch(version), getObVsnMinorPatch(version));
    }

    /**
     * @deprecated Use isLsOpSupport(long version) instead with instance version
     */
    @Deprecated
    public static boolean isLsOpSupport() {
        return isLsOpSupport(OB_VERSION);
    }

    public static boolean isLsOpSupport(long version) {
        return version >= OB_VERSION_4_2_3_0 && version < OB_VERSION_4_3_0_0
               || version >= OB_VERSION_4_3_4_0;
    }

    /**
     * @deprecated Use isFtsQuerySupport(long version) instead with instance version
     */
    @Deprecated
    public static boolean isFtsQuerySupport() {
        return isFtsQuerySupport(OB_VERSION);
    }

    public static boolean isFtsQuerySupport(long version) {
        return version >= OB_VERSION_4_3_5_1;
    }

    /**
     * @deprecated Use isReturnOneResultSupport(long version) instead with instance version
     */
    @Deprecated
    public static boolean isReturnOneResultSupport() {
        return isReturnOneResultSupport(OB_VERSION);
    }

    public static boolean isReturnOneResultSupport(long version) {
        return version >= OB_VERSION_4_2_3_0 && version < OB_VERSION_4_3_0_0
               || version >= OB_VERSION_4_3_4_0;
    }

    /**
     * @deprecated Use isHBaseBatchGetSupport(long version) instead with instance version
     */
    @Deprecated
    public static boolean isHBaseBatchGetSupport() {
        return isHBaseBatchGetSupport(OB_VERSION);
    }

    public static boolean isHBaseBatchGetSupport(long version) {
        return version >= OB_VERSION_4_2_5_2 && version < OB_VERSION_4_3_0_0
               || version >= OB_VERSION_4_3_5_1;
    }

    /**
     * @deprecated Use isHBaseBatchSupport(long version) instead with instance version
     */
    @Deprecated
    public static boolean isHBaseBatchSupport() {
        return isHBaseBatchSupport(OB_VERSION);
    }

    public static boolean isHBaseBatchSupport(long version) {
        return version >= OB_VERSION_4_2_5_2 && version < OB_VERSION_4_3_0_0
               || version >= OB_VERSION_4_3_5_0;
    }

    /**
     * @deprecated Use isCellTTLSupport(long version) instead with instance version
     */
    @Deprecated
    public static boolean isCellTTLSupport() {
        return isCellTTLSupport(OB_VERSION);
    }

    public static boolean isCellTTLSupport(long version) {
        return version >= OB_VERSION_4_3_5_1;
    }

    public static final long OB_VERSION_4_2_3_0 = calcVersion(4, (short) 2, (byte) 3, (byte) 0);

    public static final long OB_VERSION_4_2_5_2 = calcVersion(4, (short) 2, (byte) 5, (byte) 2);

    public static final long OB_VERSION_4_3_0_0 = calcVersion(4, (short) 3, (byte) 0, (byte) 0);

    public static final long OB_VERSION_4_3_4_0 = calcVersion(4, (short) 3, (byte) 4, (byte) 0);

    public static final long OB_VERSION_4_3_5_0 = calcVersion(4, (short) 3, (byte) 5, (byte) 0);

    public static final long OB_VERSION_4_3_5_1 = calcVersion(4, (short) 3, (byte) 5, (byte) 1);

    /**
     * @deprecated This global version is deprecated. Use instance-level version in ObTable/ObTableClient instead.
     * This is kept for backward compatibility only.
     */
    @Deprecated
    public static long       OB_VERSION         = calcVersion(0, (short) 0, (byte) 0, (byte) 0);
}

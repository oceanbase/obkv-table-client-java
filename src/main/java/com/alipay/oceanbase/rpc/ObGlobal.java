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

    public static int obVsnMajor() {
        return getObVsnMajor(OB_VERSION);
    }

    public static int getObVsnMajor(long version) {
        return (int) ((version >> OB_VSN_MAJOR_SHIFT) & OB_VSN_MAJOR_MASK);
    }

    public static short obVsnMinor() {
        return getObVsnMinor(OB_VERSION);
    }

    public static short getObVsnMinor(long version) {
        return (short) ((version >> OB_VSN_MINOR_SHIFT) & OB_VSN_MINOR_MASK);
    }

    public static byte obVsnMajorPatch() {
        return getObVsnMajorPatch(OB_VERSION);
    }

    public static byte getObVsnMajorPatch(long version) {
        return (byte) ((version >> OB_VSN_MAJOR_PATCH_SHIFT) & OB_VSN_MAJOR_PATCH_MASK);
    }

    public static byte obVsnMinorPatch() {
        return getObVsnMinorPatch(OB_VERSION);
    }

    public static byte getObVsnMinorPatch(long version) {
        return (byte) ((version >> OB_VSN_MINOR_PATCH_SHIFT) & OB_VSN_MINOR_PATCH_MASK);
    }

    public static String obVsnString() {
        return String.format("%d.%d.%d.%d", obVsnMajor(), obVsnMinor(), obVsnMajorPatch(),
            obVsnMinorPatch());
    }

    public static String getObVsnString(long version) {
        return String.format("%d.%d.%d.%d", getObVsnMajor(version), getObVsnMinor(version),
            getObVsnMajorPatch(version), getObVsnMinorPatch(version));
    }

    public static boolean isLsOpSupport() {
        boolean isSupp = false;
        if (OB_VERSION != 0) {
            if (obVsnMajor() == 4 && obVsnMinor() == 2 && obVsnMajorPatch() == 3
                    && OB_VERSION >= OB_VERSION_4_2_3_0) {
                isSupp = true;
            }
        }
        return isSupp;
    }

    public static final long OB_VERSION_4_2_1_0 = calcVersion(4, (short) 2, (byte) 1, (byte) 0);

    public static final long OB_VERSION_4_2_3_0 = calcVersion(4, (short) 2, (byte) 3, (byte) 0);

    public static long       OB_VERSION         = calcVersion(0, (short) 0, (byte) 0, (byte) 0);
}

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
    public static class ObVersion {
        public long majorVersion;
        public long minorVersion;
        public long buildVersion;
        public long revisionVersion;

        public ObVersion(long majorVersion, long minorVersion, long buildVersion, long revisionVersion) {
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
            this.buildVersion = buildVersion;
            this.revisionVersion = revisionVersion;
        }

        public boolean isInit() {
            return !(this.majorVersion == 0 && this.minorVersion == 0 && this.buildVersion == 0 && this.revisionVersion == 0);
        }

        public int compareTo(ObVersion anotherVersion) {
            if (this.majorVersion != anotherVersion.majorVersion) {
                return this.majorVersion > anotherVersion.majorVersion ? 1 : -1;
            } else if (this.minorVersion != anotherVersion.minorVersion) {
                return this.minorVersion > anotherVersion.minorVersion ? 1 : -1;
            } else if (this.buildVersion != anotherVersion.buildVersion) {
                return this.buildVersion > anotherVersion.buildVersion ? 1 : -1;
            } else if (this.revisionVersion != anotherVersion.revisionVersion) {
                return this.revisionVersion > anotherVersion.revisionVersion ? 1 : -1;
            } else {
                return 0;
            }
        }
    }

    public static ObVersion OB_VERSION = new ObVersion(0, 0, 0, 0);
}

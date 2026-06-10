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

package com.alipay.oceanbase.rpc.location;

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.exception.FeatureNotSupportedException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LocationUtilVersionParseTest {

    private long oldObVersion;
    private long oldObProxyVersion;

    @Before
    public void setUp() {
        oldObVersion = ObGlobal.OB_VERSION;
        oldObProxyVersion = ObGlobal.OB_PROXY_VERSION;
        ObGlobal.OB_VERSION = 0;
        ObGlobal.OB_PROXY_VERSION = 0;
    }

    @After
    public void tearDown() {
        ObGlobal.OB_VERSION = oldObVersion;
        ObGlobal.OB_PROXY_VERSION = oldObProxyVersion;
    }

    @Test
    public void testParseOceanBaseVersionWithObproxy() {
        String serverVersion = "OceanBase 4.3.5.2 + Obproxy 4.3.6.0";

        LocationUtil.parseObVerionFromLogin(serverVersion);

        Assert.assertEquals(ObGlobal.calcVersion(4, (short) 3, (byte) 5, (byte) 2),
            ObGlobal.OB_VERSION);
        Assert.assertEquals(ObGlobal.calcVersion(4, (short) 3, (byte) 6, (byte) 0),
            ObGlobal.OB_PROXY_VERSION);
    }

    @Test
    public void testParseOceanBaseCeVersionWithObproxy() {
        String serverVersion = "OceanBase_CE 4.4.1.0 + Obproxy 4.4.0.0";

        LocationUtil.parseObVerionFromLogin(serverVersion);

        Assert.assertEquals(ObGlobal.calcVersion(4, (short) 4, (byte) 1, (byte) 0),
            ObGlobal.OB_VERSION);
        Assert.assertEquals(ObGlobal.calcVersion(4, (short) 4, (byte) 0, (byte) 0),
            ObGlobal.OB_PROXY_VERSION);
    }

    @Test
    public void testNotOverrideWhenVersionAlreadyInitialized() {
        ObGlobal.OB_VERSION = ObGlobal.calcVersion(4, (short) 2, (byte) 1, (byte) 7);
        ObGlobal.OB_PROXY_VERSION = ObGlobal.calcVersion(4, (short) 3, (byte) 5, (byte) 0);
        String serverVersion = "OceanBase 4.4.1.0 + Obproxy 4.4.0.0";

        LocationUtil.parseObVerionFromLogin(serverVersion);

        Assert.assertEquals(ObGlobal.calcVersion(4, (short) 2, (byte) 1, (byte) 7),
            ObGlobal.OB_VERSION);
        Assert.assertEquals(ObGlobal.calcVersion(4, (short) 3, (byte) 5, (byte) 0),
            ObGlobal.OB_PROXY_VERSION);
    }

    @Test
    public void testThrowWhenVersionLowerThanFour() {
        String serverVersion = "OceanBase 3.2.4.0";
        boolean thrown = false;

        try {
            LocationUtil.parseObVerionFromLogin(serverVersion);
        } catch (FeatureNotSupportedException e) {
            thrown = true;
        }

        Assert.assertTrue(thrown);
    }

    @Test
    public void testParseOceanBaseDatabaseAiVersionWithObproxy() {
        String serverVersion = "OceanBase Database AI 4.6.1.0 + Obproxy 4.4.0.0";

        LocationUtil.parseObVerionFromLogin(serverVersion);

        Assert.assertEquals(ObGlobal.calcVersion(4, (short) 6, (byte) 1, (byte) 0),
            ObGlobal.OB_VERSION);
        Assert.assertEquals(ObGlobal.calcVersion(4, (short) 4, (byte) 0, (byte) 0),
            ObGlobal.OB_PROXY_VERSION);
    }

}

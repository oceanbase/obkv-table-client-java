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

import com.alipay.oceanbase.rpc.exception.ObTableUnexpectedException;
import com.alipay.oceanbase.rpc.location.model.*;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import com.alipay.oceanbase.rpc.util.ZoneUtil;
import org.junit.*;
import java.util.Map;

public class ObWeakReadConsistencyTest {
    private static final String fullUserName = ObTableClientTestUtil.FULL_USER_NAME;
    private static final String password     = ObTableClientTestUtil.PASSWORD;
    private static final String paramUrl     = ObTableClientTestUtil.PARAM_URL;
    private static final String weakParamUrl = paramUrl
                                               + "&read_consistency=weak&ob_route_policy=follower_first";
    private static final int    dataSetSize  = 10;
    private static String   testIdc        = "dev";
    protected ObTableClient     client;

    public static void initZoneClient() {
        System.setProperty("zmode", "true");
        System.setProperty("com.alipay.confreg.url", "confreg-pool.stable.alipay.net");
        System.setProperty("com.alipay.ldc.zone", "GZ00A");
    }

    public static ObTableClient getObTableClient(String paramUrl) throws Exception {
        ObTableClient obTableClient = new ObTableClient();
        obTableClient.setFullUserName(fullUserName);
        obTableClient.setParamURL(paramUrl);
        obTableClient.setPassword(password);
        obTableClient.setCurrentIDC(testIdc);
        obTableClient.setSysUserName(ObTableClientTestUtil.PROXY_SYS_USER_NAME);
        obTableClient.setEncSysPassword(ObTableClientTestUtil.PROXY_SYS_USER_ENC_PASSWORD);
        obTableClient.init();
        return obTableClient;
    }

    @BeforeClass
    public static void init() throws Exception {
        ThreadLocalMap.setReadConsistency(ObReadConsistency.WEAK);
        cleanup();
        ObTableClient client = getObTableClient(paramUrl);
        for (int i = 0; i < dataSetSize; i++) {
            String key = "abc-" + i;
            String val = "xyz-" + i;
            client.insert("test_varchar_table", key, new String[] { "c2" }, new String[] { val });
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        ObTableClient client = getObTableClient(paramUrl);
        for (int i = 0; i < dataSetSize; i++) {
            String key = "abc-" + i;
            client.delete("test_varchar_table", key);
        }
    }

    @Before
    public void setup() throws Exception {
        client = getObTableClient(weakParamUrl);
    }

    @After
    public void teardown() throws Exception {
        client.close();
    }

    @Test
    public void testReadConsistencySetting() {
        Assert.assertEquals(client.getReadConsistency(), ObReadConsistency.WEAK);
        Assert.assertEquals(client.getObRoutePolicy(), ObRoutePolicy.FOLLOWER_FIRST);
    }

    @Test
    public void testGetWithWeakRead() throws Exception {
        for (int i = 0; i < dataSetSize; i++) {
            String key = "abc-" + i;
            String val = "xyz-" + i;
            try {
                Map res = client.get("test_varchar_table", key, new String[] { "c1", "c2" });
                Assert.fail("failed to get with weak read");
            } catch (ObTableUnexpectedException e) {
                Assert.assertEquals(ResultCodes.OB_NOT_SUPPORTED.errorCode, e.getErrorCode());
            }
        }
        Assert.assertFalse(client.getReadConsistency().isStrong());
    }

    @Test
    public void testZoneIdc() {
        Assert.assertEquals("dev", ZoneUtil.getCurrentIDC());
    }
}

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

package com.alipay.oceanbase.rpc.location.model;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ObServerAddrTest {

    @Test
    public void testAddrPriority() {
        ObServerAddr addr1 = new ObServerAddr();
        addr1.getPriority().set(-3);

        ObServerAddr addr2 = new ObServerAddr();
        addr2.getPriority().set(-2);

        ObServerAddr addr3 = new ObServerAddr();
        addr3.getPriority().set(-1);

        ObServerAddr addr4 = new ObServerAddr();

        List<ObServerAddr> serverRoster = new ArrayList<ObServerAddr>();

        serverRoster.add(addr1);
        serverRoster.add(addr2);
        serverRoster.add(addr3);
        serverRoster.add(addr4);

        Collections.sort(serverRoster);

        Assert.assertEquals(addr4, serverRoster.get(0));
        Assert.assertEquals(addr3, serverRoster.get(1));
        Assert.assertEquals(addr2, serverRoster.get(2));
        Assert.assertEquals(addr1, serverRoster.get(3));
    }

    @Test
    public void testAddrExpired() {
        long cachingTimeout = 200;
        ObServerAddr addr = new ObServerAddr();
        Assert.assertFalse(addr.isExpired(cachingTimeout));
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        Assert.assertTrue(addr.isExpired(cachingTimeout));
        addr.recordAccess();
        Assert.assertFalse(addr.isExpired(cachingTimeout));
    }
}

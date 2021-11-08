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
import java.util.List;

public class ServerRosterTest {

    @Test
    public void testRoster() {

        ServerRoster serverRoster = new ServerRoster();

        Assert.assertEquals(0, serverRoster.getMaxPriority());

        Assert.assertEquals(0, serverRoster.getMembers().size());

        ObServerAddr addr1 = new ObServerAddr();
        addr1.getPriority().set(-3);

        ObServerAddr addr2 = new ObServerAddr();
        addr2.getPriority().set(-2);

        ObServerAddr addr3 = new ObServerAddr();
        addr3.getPriority().set(-1);

        ObServerAddr addr4 = new ObServerAddr();

        List<ObServerAddr> servers = new ArrayList<ObServerAddr>();

        servers.add(addr1);
        servers.add(addr2);
        servers.add(addr3);
        servers.add(addr4);

        serverRoster.reset(servers);
        serverRoster.resetMaxPriority();

        Assert.assertEquals(0, serverRoster.getMaxPriority());

        serverRoster.downgradeMaxPriority(addr4.getPriority().decrementAndGet());

        Assert.assertEquals(-1, serverRoster.getMaxPriority());

        serverRoster.downgradeMaxPriority(addr4.getPriority().decrementAndGet());

        Assert.assertEquals(-1, serverRoster.getMaxPriority());

        serverRoster.downgradeMaxPriority(addr3.getPriority().decrementAndGet());

        Assert.assertEquals(-2, serverRoster.getMaxPriority());

        serverRoster.downgradeMaxPriority(addr3.getPriority().decrementAndGet());

        Assert.assertEquals(-2, serverRoster.getMaxPriority());

        serverRoster.downgradeMaxPriority(addr2.getPriority().decrementAndGet());

        Assert.assertEquals(-2, serverRoster.getMaxPriority());

        serverRoster.downgradeMaxPriority(addr4.getPriority().decrementAndGet());

        Assert.assertEquals(-3, serverRoster.getMaxPriority());

        addr1.getPriority().set(0);

        serverRoster.downgradeMaxPriority(0);

        Assert.assertEquals(-3, serverRoster.getMaxPriority());
    }
}

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

package com.alipay.oceanbase.rpc.threadlocal;

import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap.PROCESS_PRIORITY;

public class ThreadLocalMapTest {
    @Test
    public void testProcessPriority() {
        ThreadLocalMap.setProcessHighPriority();
        Assert.assertEquals((short) 4, ThreadLocalMap.getProcessPriority());
        ThreadLocalMap.setProcessNormalPriority();
        Assert.assertEquals((short) 5, ThreadLocalMap.getProcessPriority());
        ThreadLocalMap.setProcessLowPriority();
        Assert.assertEquals((short) 6, ThreadLocalMap.getProcessPriority());
        ThreadLocalMap.reset();
        Assert.assertEquals((short) 5, ThreadLocalMap.getProcessPriority());
    }

    @Test
    public void testTransmit() {
        Map<Object, Object> mockContext = new HashMap<Object, Object>();
        mockContext.put("1", 2);
        mockContext.put(2, "2");
        ThreadLocalMap.transmitContextMap(mockContext);
        Assert.assertEquals(mockContext, ThreadLocalMap.getContextMap());
    }
}

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

import com.alipay.oceanbase.rpc.location.SecureIdentityLoginModule;
import org.junit.Assert;
import org.junit.Test;

public class ObUserAuthTest {

    @Test
    public void testObUserAuth() throws Exception {
        String username = "user-abc";
        String password = "pwd-xyz";
        ObUserAuth ua = new ObUserAuth(username, password);
        Assert.assertEquals(username, ua.getUserName());
        Assert.assertEquals(password, ua.getPassword());
        Assert.assertNull(ua.getEncPassword());
    }

    @Test
    public void testEncPassword() throws Exception {
        String username = "user-abc";
        String password = "pwd-xyz";
        String encPassword = "2e3b1a87f304a46f0bf3cb262721f0df";
        String decPassword = SecureIdentityLoginModule.decode(encPassword);

        ObUserAuth ua = new ObUserAuth();
        ua.setUserName(username);
        ua.setPassword(password);
        Assert.assertEquals(username, ua.getUserName());
        Assert.assertEquals(password, ua.getPassword());
        Assert.assertNull(ua.getEncPassword());

        ua.setEncPassword(encPassword);
        Assert.assertEquals(decPassword, ua.getPassword());
    }
}

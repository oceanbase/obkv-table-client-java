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

package com.alipay.oceanbase.rpc.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SecurityTest {

    @Test
    public void test_passwordScramble() {
        assertEquals(20, Security.getPasswordScramble(20).length());
    }

    @Test
    public void test_scramblePassword() {
        assertEquals(0, Security.scramblePassword("", Security.getPasswordScramble(20)).length());
        // TODO 不好单元测试
        // assertEquals(new ObBytesString("TODO"), Security.scramblePassword("123", Security.getPasswordScramble(20)));
    }

}

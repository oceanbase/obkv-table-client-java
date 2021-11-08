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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query;

import org.junit.Test;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class ObBorderFlagTest {

    @Test
    public void test() {
        ObBorderFlag obBorderFlag = new ObBorderFlag();
        obBorderFlag.setInclusiveStart();
        assertTrue(obBorderFlag.isInclusiveStart());
        obBorderFlag.unsetInclusiveStart();
        assertFalse(obBorderFlag.isInclusiveStart());

        obBorderFlag.setInclusiveEnd();
        assertTrue(obBorderFlag.isInclusiveEnd());
        obBorderFlag.unsetInclusiveEnd();
        assertFalse(obBorderFlag.isInclusiveEnd());
    }

}

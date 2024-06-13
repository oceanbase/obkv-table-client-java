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

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnSimpleFunc;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObExprGrammerVisitorImpl;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnExpressParser;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.util.ObTableClientTestUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ObTableClient.class)
@PowerMockIgnore({ "javax.crypto.*" })
public class ObTableClientInitTest {
    private ObTableClient client;

    @Test
    public void testInit() throws Exception {
        // builder = new ObTable.Builder("1,1,1,1", 123);
        // PowerMockito.whenNew(ObTable.Builder.class).withAnyArguments().thenReturn(builder);
//        client = ObTableClientTestUtil.newTestClient();
//        client.init();

        ObGeneratedColumnExpressParser parser = new ObGeneratedColumnExpressParser("substr(substr(substr(c, 1, 2), 1, 2), 1, 2)");
        ObGeneratedColumnSimpleFunc func = parser.parse();
        HashMap<String, Object> rowkeyMap = new HashMap<>();
        rowkeyMap.put("c", "hello world");
        Object res = func.evalValue(ObCollationType.CS_TYPE_BINARY, rowkeyMap);
        if (res instanceof byte[]) {
            System.out.println("this is byte");
        }
        System.out.println("res: " + new String((byte[]) res));
    }




}

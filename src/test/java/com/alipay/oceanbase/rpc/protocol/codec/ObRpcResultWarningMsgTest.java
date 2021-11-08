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

package com.alipay.oceanbase.rpc.protocol.codec;

import com.alipay.oceanbase.rpc.protocol.payload.ObRpcResultWarningMsg;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.junit.Test;

public class ObRpcResultWarningMsgTest {
    @Test
    public void test_codec() {
        ObRpcResultWarningMsg msg = new ObRpcResultWarningMsg();
        msg.setCode(1);
        msg.setLineNo(1);
        msg.setLogLevel(1);
        msg.setMsg(new byte[] { 1, 2, 3 });
        byte[] bytes = msg.encode();
        ByteBuf buf = PooledByteBufAllocator.DEFAULT.buffer();
        buf.writeBytes(bytes);
        msg.decode(buf);
    }
}

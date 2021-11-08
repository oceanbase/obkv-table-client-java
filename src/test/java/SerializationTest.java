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

import com.alipay.oceanbase.rpc.util.Serialization;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SerializationTest {

    @Test
    public void test_getNeedBytes_int() {
        assertEquals(1, Serialization.getNeedBytes(127));
        assertEquals(2, Serialization.getNeedBytes(128));

        assertEquals(2, Serialization.getNeedBytes(16383));
        assertEquals(3, Serialization.getNeedBytes(16384));

        assertEquals(3, Serialization.getNeedBytes(2097151));
        assertEquals(4, Serialization.getNeedBytes(2097152));

        assertEquals(4, Serialization.getNeedBytes(268435455));
        assertEquals(5, Serialization.getNeedBytes(268435456));

        assertEquals(5, Serialization.getNeedBytes(Integer.MAX_VALUE));
    }

    @Test
    public void test_getNeedBytes_long() {
        assertEquals(1, Serialization.getNeedBytes(127L));
        assertEquals(2, Serialization.getNeedBytes(128L));

        assertEquals(2, Serialization.getNeedBytes(16383L));
        assertEquals(3, Serialization.getNeedBytes(16384L));

        assertEquals(3, Serialization.getNeedBytes(2097151L));
        assertEquals(4, Serialization.getNeedBytes(2097152L));

        assertEquals(4, Serialization.getNeedBytes(268435455L));
        assertEquals(5, Serialization.getNeedBytes(268435456L));

        assertEquals(5, Serialization.getNeedBytes(34359738367L));
        assertEquals(6, Serialization.getNeedBytes(34359738368L));

        assertEquals(6, Serialization.getNeedBytes(4398046511103L));
        assertEquals(7, Serialization.getNeedBytes(4398046511104L));

        assertEquals(7, Serialization.getNeedBytes(562949953421311L));
        assertEquals(8, Serialization.getNeedBytes(562949953421312L));

        assertEquals(9, Serialization.getNeedBytes(Long.MAX_VALUE));
    }

}

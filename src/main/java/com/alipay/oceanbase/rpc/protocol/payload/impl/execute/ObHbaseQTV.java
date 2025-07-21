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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableSerialUtil;
import com.alipay.oceanbase.rpc.util.ObByteBuf;

public class ObHbaseQTV {
    private ObObj Q;
    private ObObj T;
    private ObObj V;

    public ObHbaseQTV() {
        this.Q = new ObObj();
        this.T = new ObObj();
        this.V = new ObObj();
    }

    public ObHbaseQTV(ObObj Q, ObObj T, ObObj V) {
        this.Q = Q;
        this.T = T;
        this.V = V;
    }

    public void encode(ObByteBuf buf) {
        ObTableSerialUtil.encode(buf, Q);
        ObTableSerialUtil.encode(buf, T);
        ObTableSerialUtil.encode(buf, V);
    }

    public long getPayloadSize() {
        return ObTableSerialUtil.getEncodedSize(Q) + ObTableSerialUtil.getEncodedSize(T)
               + ObTableSerialUtil.getEncodedSize(V);
    }

    public ObObj getQ() {
        return Q;
    }

    public ObObj getT() {
        return T;
    }

    public ObObj getV() {
        return V;
    }

    public void setQ(ObObj Q) {
        this.Q = Q;
    }

    public void setT(ObObj T) {
        this.T = T;
    }

    public void setV(ObObj V) {
        this.V = V;
    }
}

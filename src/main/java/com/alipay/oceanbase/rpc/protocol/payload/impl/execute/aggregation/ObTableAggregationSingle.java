/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2023 OceanBase
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

package com.alipay.oceanbase.rpc.protocol.payload.impl.execute.aggregation;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.util.Serialization;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableAggregationSingle extends AbstractPayload {

    private ObTableAggregationType aggType;
    private String                 aggColumn;

    public ObTableAggregationSingle(ObTableAggregationType aggType, String aggColumn) {
        this.aggColumn = aggColumn;
        this.aggType = aggType;
    }

    /*
     * Serialize.
     */
    public byte[] encode() {
        byte[] bytes = new byte[(int) this.getPayloadSize()];
        int idx = 0;

        int headerLen = (int) getObUniVersionHeaderLength(getVersion(),
            this.getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), this.getPayloadContentSize()), 0,
            bytes, idx, headerLen);
        idx += headerLen;

        int len = Serialization.getNeedBytes(aggType.getByteValue());
        System.arraycopy(Serialization.encodeI8(aggType.getByteValue()), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(aggColumn);
        System.arraycopy(Serialization.encodeVString(aggColumn), 0, bytes, idx, len);

        return bytes;
    }

    public long getPayloadContentSize() {
        return Serialization.getNeedBytes(aggType.getByteValue())
               + Serialization.getNeedBytes(aggColumn);
    }
}

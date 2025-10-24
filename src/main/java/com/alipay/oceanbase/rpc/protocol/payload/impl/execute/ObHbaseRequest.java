/*-
 * #%L
 * com.oceanbase:obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
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

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Credentialable;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableSerialUtil;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/*
OB_SERIALIZE_MEMBER(ObHbaseRequest,
                    credential_,
                    table_name_,
                    tablet_id_,
                    option_flag_,
                    op_type_,
                    keys_,
                    cf_rows_);
 */
/*
    [k1][k2][k3]...
    [1] [3] [2] ...
    [QTV] [QTV][QTV][QTV] [QTV][QTV]
*/
public class ObHbaseRequest extends AbstractPayload implements Credentialable {
    protected ObBytesString           credential;
    protected String                  tableName; // HBase tableName, OceanBase tablegroup_name
    protected long                    tabletId;  // do not serialize
    protected ObTableHbaseReqFlag     optionFlag = new ObTableHbaseReqFlag();
    protected ObTableOperationType    opType;
    protected List<ObObj>             keys       = new ArrayList<>();
    protected List<ObHbaseCfRows>     cfRows;

    public ObHbaseRequest() {
        this.credential = new ObBytesString();
        this.tableName = "";
        this.keys = new ArrayList<>();
        this.cfRows = new ArrayList<>();
    }

    public ObHbaseRequest(ObBytesString credential, String tableName, List<ObObj> keys, List<Integer> cellNumArray, List<ObHbaseCfRows> cfRows) {
        this.credential = credential;
        this.tableName = tableName;
        this.keys = keys;
        this.cfRows = cfRows;
    }

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_HBASE_EXECUTE;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        long calculatedSize = getPayloadSize();
        ObByteBuf buf = new ObByteBuf((int) calculatedSize);

        // 0. encode ObHbaseRequest header
        encodeHeader(buf);

        // 1. encode credential
        Serialization.encodeBytesString(buf, credential);

        // 2. encode tableName
        Serialization.encodeVString(buf, tableName);

        // 3. encode option flag
        Serialization.encodeVi64(buf, optionFlag.getValue());

        // 4. encode op_type
        Serialization.encodeI8(buf, opType.getByteValue());

        // 5. encode keys array size and keys
        Serialization.encodeVi64(buf, keys.size());
        for (int i = 0; i < keys.size(); i++) {
            ObObj key = keys.get(i);
            ObTableSerialUtil.encode(buf, key);
        }

        // 6. encode same cf rows array size and rows
        Serialization.encodeVi64(buf, cfRows.size());
        for (int i = 0; i < cfRows.size(); i++) {
            ObHbaseCfRows sameCfRows = cfRows.get(i);
            sameCfRows.encode(buf);
        }
        
        if (buf.pos != buf.bytes.length) {
            throw new IllegalArgumentException("error in encode ObHbaseRequest (" +
                    "pos:" + buf.pos + ", buf.capacity:" + buf.bytes.length + ", calculatedSize:" + calculatedSize + ")");
        }
        return buf.bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);
        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        if (payLoadContentSize == INVALID_PAYLOAD_CONTENT_SIZE) {
            payLoadContentSize = Serialization.getNeedBytes(credential)
                                + Serialization.getNeedBytes(tableName)
                                + Serialization.getNeedBytes(optionFlag.getValue())
                                + Serialization.getNeedBytes(opType.getByteValue());
            
            // Size for keys array
            payLoadContentSize += Serialization.getNeedBytes(keys.size());
            for (ObObj key : keys) {
                payLoadContentSize += ObTableSerialUtil.getEncodedSize(key);
            }
            
            // Size for sameCfRows array
            payLoadContentSize += Serialization.getNeedBytes(cfRows.size());
            for (ObHbaseCfRows cfRows : cfRows) {
                payLoadContentSize += cfRows.getPayloadSize();
            }
        }
        return payLoadContentSize;
    }

    @Override
    public void resetPayloadContentSize() {
        super.resetPayloadContentSize();
        for (ObHbaseCfRows cfRows : cfRows) {
            if (cfRows != null) {
                cfRows.resetPayloadContentSize();
            }
        }
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public void setCredential(ObBytesString credential) {
        this.credential = credential;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public void setKeys(List<ObObj> keys) {
        this.keys = keys;
    }

    public void setOpType(ObTableOperationType opType) {
        this.opType = opType;
    }

    public void setCfRows(List<ObHbaseCfRows> sameCfRows) {
        this.cfRows = sameCfRows;
    }

    public void setServerCanRetry(boolean canRetry) {
        optionFlag.setFlagServerCanRetry(canRetry);
    }

    public boolean getServerCanRetry() {
        return optionFlag.getFlagServerCanRetry();
    }

    public void setTabletId(long tabletId) {
        this.tabletId = tabletId;
    }

    public long getTabletId() {
        return tabletId;
    }

    public ObBytesString getCredential() {
        return credential;
    }

    public String getTableName() {
        return tableName;
    }

    public List<ObObj> getKeys() {
        return keys;
    }

    public List<ObHbaseCfRows> getCfRows() {
        return cfRows;
    }
}

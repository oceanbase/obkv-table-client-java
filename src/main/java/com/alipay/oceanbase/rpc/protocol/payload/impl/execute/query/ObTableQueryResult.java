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

import com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

public class ObTableQueryResult extends AbstractPayload {

    private ObRpcPacketHeader header;
    private List<String>      propertiesNames = new LinkedList<String>();
    private long              rowCount        = 0;
    // decode to propertiesRows from dataBuffer directly
    // byte[]       dataBuffer;

    // TODO 需要做成流式的，目前 OB 还不支持流式协议，单个 packet 大小过大会失败
    private List<List<ObObj>> propertiesRows  = new LinkedList<List<ObObj>>();

    /**
     * Ob table query result.
     */
    public ObTableQueryResult() {
        this.header = new ObRpcPacketHeader();
    }

    /**
     * Ob table query result.
     */
    public ObTableQueryResult(ObRpcPacketHeader header) {
        this.header = header;
    }

    /**
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_EXECUTE_QUERY;
    }

    /**
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        int len = (int) Serialization.getObUniVersionHeaderLength(getVersion(), getPayloadSize());
        byte[] header = Serialization.encodeObUniVersionHeader(getVersion(), getPayloadSize());
        System.arraycopy(header, 0, bytes, idx, len);
        idx += len;

        // 2. encode it
        len = Serialization.getNeedBytes(propertiesNames.size());
        System.arraycopy(Serialization.encodeVi64(propertiesNames.size()), 0, bytes, idx, len);
        idx += len;
        for (String propertiesName : propertiesNames) {
            len = Serialization.getNeedBytes(propertiesName);
            System.arraycopy(Serialization.encodeVString(propertiesName), 0, bytes, idx, len);
            idx += len;
        }

        len = Serialization.getNeedBytes(rowCount);
        System.arraycopy(Serialization.encodeVi64(rowCount), 0, bytes, idx, len);
        idx += len;

        len = Serialization.getNeedBytes(propertiesRows.size());
        System.arraycopy(Serialization.encodeVi64(propertiesRows.size()), 0, bytes, idx, len);
        idx += len;
        // ObDataBuffer
        for (List<ObObj> row : propertiesRows) {
            for (ObObj obObj : row) {
                len = obObj.getEncodedSize();
                System.arraycopy(obObj.encode(), 0, bytes, idx, len);
                idx += len;
            }
        }

        return bytes;
    }

    /**
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode version
        super.decode(buf);

        // 1. decode ObTableResult
        // this.header.decode(buf);

        // 2. decode itself
        long size = Serialization.decodeVi64(buf);
        for (int i = 0; i < size; i++) {
            this.propertiesNames.add(Serialization.decodeVString(buf));
        }
        this.rowCount = Serialization.decodeVi64(buf);

        // ObDataBuffer
        Serialization.decodeVi64(buf); // dataBuffer length
        for (int r = 0; r < rowCount; r++) {
            List<ObObj> row = new LinkedList<ObObj>();
            for (int i = 0; i < propertiesNames.size(); i++) {
                ObObj obObj = new ObObj();
                obObj.decode(buf);
                row.add(obObj);
            }
            addPropertiesRow(row);
        }

        return this;
    }

    /**
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long size = 0;
        // size += header.getPayloadSize();
        size += Serialization.getNeedBytes(propertiesNames.size());
        for (String propertiesName : propertiesNames) {
            size += Serialization.getNeedBytes(propertiesName);
        }
        size += Serialization.getNeedBytes(rowCount);

        size += Serialization.getNeedBytes(propertiesRows.size());
        for (List<ObObj> row : propertiesRows) {
            for (ObObj obObj : row) {
                size += obObj.getEncodedSize();
            }
        }

        return size;
    }

    /**
     * Get properties names.
     */
    public List<String> getPropertiesNames() {
        return propertiesNames;
    }

    /**
     * Set properties names.
     */
    public void setPropertiesNames(List<String> propertiesNames) {
        this.propertiesNames = propertiesNames;
    }

    /**
     * Add properties name.
     */
    public void addPropertiesName(String propertiesName) {
        this.propertiesNames.add(propertiesName);
    }

    /**
     * Get row count.
     */
    public long getRowCount() {
        return rowCount;
    }

    /**
     * Set row count.
     */
    public void setRowCount(long rowCount) {
        this.rowCount = rowCount;
    }

    /**
     * Get properties rows.
     */
    public List<List<ObObj>> getPropertiesRows() {
        return propertiesRows;
    }

    /**
     * Set properties rows.
     */
    public void setPropertiesRows(List<List<ObObj>> propertiesRows) {
        this.propertiesRows = propertiesRows;
    }

    /**
     * Add properties row.
     */
    public void addPropertiesRow(List<ObObj> propertiesRow) {
        this.propertiesRows.add(propertiesRow);
    }

    /**
     * Add all properties rows.
     */
    public void addAllPropertiesRows(List<List<ObObj>> propertiesRows) {
        this.propertiesRows.addAll(propertiesRows);
    }

    /**
     * Is stream.
     */
    public boolean isStream() {
        return header.isStream();
    }

    /**
     * Is stream last.
     */
    public boolean isStreamLast() {
        return header.isStreamLast();
    }

    /**
     * Is stream next.
     */
    public boolean isStreamNext() {
        return header.isStreamNext();
    }

    /**
     * Get session id.
     */
    public long getSessionId() {
        return header.getSessionId();
    }
}

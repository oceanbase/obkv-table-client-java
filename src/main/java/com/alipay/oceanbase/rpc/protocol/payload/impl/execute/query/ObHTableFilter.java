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

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

/**
 *
OB_SERIALIZE_MEMBER(ObHTableFilter,
        is_valid_,
        select_column_qualifier_,
        min_stamp_,
        max_stamp_,
        max_versions_,
        limit_per_row_per_cf_,
        offset_per_row_per_cf_,
        filter_string_);
 *
 */
public class ObHTableFilter extends AbstractPayload {

    private boolean             isValid               = true;                          // tell server that OBHtableFilter is not working, use HTABLE_FILTER_DUMMY_BYTES instead, always true
    private List<ObBytesString> selectColumnQualifier = new ArrayList<ObBytesString>();
    private long                minStamp              = 0;
    private long                maxStamp              = Long.MAX_VALUE;
    private int                 maxVersions           = 1;
    private int                 limitPerRowPerCf      = -1;                            // -1 means unlimited
    private int                 offsetPerRowPerCf     = 0;                             // -1 means unlimited
    private ObBytesString       filterString          = null;

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        int headerLen = (int) getObUniVersionHeaderLength(getVersion(), getPayloadContentSize());
        System.arraycopy(encodeObUniVersionHeader(getVersion(), getPayloadContentSize()), 0, bytes,
            idx, headerLen);
        idx += headerLen;

        // 1. encode
        System.arraycopy(Serialization.encodeI8(isValid ? (byte) 1 : (byte) 0), 0, bytes, idx, 1);
        idx++;
        int len = Serialization.getNeedBytes(selectColumnQualifier.size());
        System
            .arraycopy(Serialization.encodeVi64(selectColumnQualifier.size()), 0, bytes, idx, len);
        idx += len;
        for (ObBytesString q : selectColumnQualifier) {
            len = Serialization.getNeedBytes(q);
            System.arraycopy(Serialization.encodeBytesString(q), 0, bytes, idx, len);
            idx += len;
        }

        len = Serialization.getNeedBytes(minStamp);
        System.arraycopy(Serialization.encodeVi64(minStamp), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(maxStamp);
        System.arraycopy(Serialization.encodeVi64(maxStamp), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(maxVersions);
        System.arraycopy(Serialization.encodeVi32(maxVersions), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(limitPerRowPerCf);
        System.arraycopy(Serialization.encodeVi32(limitPerRowPerCf), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(offsetPerRowPerCf);
        System.arraycopy(Serialization.encodeVi32(offsetPerRowPerCf), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(filterString);
        System.arraycopy(Serialization.encodeBytesString(filterString), 0, bytes, idx, len);
        idx += len;

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf);

        this.isValid = Serialization.decodeI8(buf) == 1;
        long size = Serialization.decodeVi64(buf);
        this.selectColumnQualifier = new ArrayList<ObBytesString>((int) size);
        for (int i = 0; i < size; i++) {
            this.selectColumnQualifier.add(Serialization.decodeBytesString(buf));
        }

        this.minStamp = Serialization.decodeVi64(buf);
        this.maxStamp = Serialization.decodeVi64(buf);
        this.maxVersions = Serialization.decodeVi32(buf);
        this.limitPerRowPerCf = Serialization.decodeVi32(buf);
        this.offsetPerRowPerCf = Serialization.decodeVi32(buf);
        this.filterString = Serialization.decodeBytesString(buf);

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        if (this.payLoadContentSize == -1) {
            long contentSize = 0;
            contentSize += 1; // isValid

            contentSize += Serialization.getNeedBytes(selectColumnQualifier.size());
            for (ObBytesString q : selectColumnQualifier) {
                contentSize += Serialization.getNeedBytes(q);
            }

            contentSize += Serialization.getNeedBytes(minStamp);
            contentSize += Serialization.getNeedBytes(maxStamp);
            contentSize += Serialization.getNeedBytes(maxVersions);
            contentSize += Serialization.getNeedBytes(limitPerRowPerCf);
            contentSize += Serialization.getNeedBytes(offsetPerRowPerCf);
            contentSize += Serialization.getNeedBytes(filterString);
            this.payLoadContentSize = contentSize;
        }
        return this.payLoadContentSize;
    }

    /*
     * Is valid.
     */
    public boolean isValid() {
        return isValid;
    }

    /*
     * Set valid.
     */
    public void setValid(boolean valid) {
        isValid = valid;
    }

    /*
     * Get select column qualifier.
     */
    public List<ObBytesString> getSelectColumnQualifier() {
        return selectColumnQualifier;
    }

    /*
     * Add select column qualifier.
     */
    public void addSelectColumnQualifier(String selectColumnQualifier) {
        this.selectColumnQualifier.add(new ObBytesString(selectColumnQualifier));
    }

    /*
     * Add select column qualifier.
     */
    public void addSelectColumnQualifier(byte[] selectColumnQualifier) {
        this.selectColumnQualifier.add(new ObBytesString(selectColumnQualifier));
    }

    /*
     * Set select column qualifier.
     */
    public void setSelectColumnQualifier(List<ObBytesString> selectColumnQualifier) {
        this.selectColumnQualifier = selectColumnQualifier;
    }

    /*
     * Get min stamp.
     */
    public long getMinStamp() {
        return minStamp;
    }

    /*
     * Set min stamp.
     */
    public void setMinStamp(long minStamp) {
        this.minStamp = minStamp;
    }

    /*
     * Get max stamp.
     */
    public long getMaxStamp() {
        return maxStamp;
    }

    /*
     * Set max stamp.
     */
    public void setMaxStamp(long maxStamp) {
        this.maxStamp = maxStamp;
    }

    /*
     * Get max versions.
     */
    public int getMaxVersions() {
        return maxVersions;
    }

    /*
     * Set max versions.
     */
    public void setMaxVersions(int maxVersions) {
        this.maxVersions = maxVersions;
    }

    /*
     * Get limit per row per cf.
     */
    public int getLimitPerRowPerCf() {
        return limitPerRowPerCf;
    }

    /*
     * Set limit per row per cf.
     */
    public void setLimitPerRowPerCf(int limitPerRowPerCf) {
        this.limitPerRowPerCf = limitPerRowPerCf;
    }

    /*
     * Get offset per row per cf.
     */
    public int getOffsetPerRowPerCf() {
        return offsetPerRowPerCf;
    }

    /*
     * Set offset per row per cf.
     */
    public void setOffsetPerRowPerCf(int offsetPerRowPerCf) {
        this.offsetPerRowPerCf = offsetPerRowPerCf;
    }

    /*
     * Get filter string.
     */
    public byte[] getFilterString() {
        return filterString.bytes;
    }

    /*
     * Set filter string.
     */
    public void setFilterString(byte[] filterString) {
        if (this.filterString == null) {
            this.filterString = new ObBytesString();
        }
        this.filterString.bytes = filterString;
    }
}

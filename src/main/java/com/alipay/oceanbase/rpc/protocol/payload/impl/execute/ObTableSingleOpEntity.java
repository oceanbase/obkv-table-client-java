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
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObTableSerialUtil;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.*;

public class ObTableSingleOpEntity extends AbstractPayload {
    private List<String> rowKeyNames = new ArrayList<>();
    private byte[] rowKeyBitMap = null;
    private long rowKeyBitLen = 0;
    private List<String> aggRowKeyNames = new ArrayList<>();
    private List<ObObj> rowkey = new ArrayList<>();

    private List<String> propertiesNames = new ArrayList<>();
    private byte[] propertiesBitMap = null;
    private long propertiesBitLen = 0;
    private List<String> aggPropertiesNames = new ArrayList<>();
    private List<ObObj> propertiesValues = new ArrayList<>();

    private boolean ignoreEncodePropertiesColumnNames = false;

    public ObTableSingleOpEntity() {}

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode rowKey bitmap
        int len = Serialization.getNeedBytes(rowKeyBitLen);
        System.arraycopy(Serialization.encodeVi64(rowKeyBitLen), 0, bytes, idx, len);
        idx += len;
        for (byte b : rowKeyBitMap) {
            System.arraycopy(Serialization.encodeI8(b), 0, bytes, idx, 1);
            idx += 1;
        }

        // 2. encode rowkey
        len = Serialization.getNeedBytes(rowkey.size());
        System.arraycopy(Serialization.encodeVi64(rowkey.size()), 0, bytes, idx, len);
        idx += len;
        for (ObObj obj : rowkey) {
            len =  ObTableSerialUtil.getEncodedSize(obj);
            System.arraycopy(ObTableSerialUtil.encode(obj), 0, bytes, idx, len);
            idx += len;
        }

        // 3. encode property bitmap
        if (ignoreEncodePropertiesColumnNames) {
            len = Serialization.getNeedBytes(0L);
            System.arraycopy(Serialization.encodeVi64(0L), 0, bytes, idx, len);
            idx += len;
        } else {
            len = Serialization.getNeedBytes(propertiesBitLen);
            System.arraycopy(Serialization.encodeVi64(propertiesBitLen), 0, bytes, idx, len);
            idx += len;
            for (byte b : propertiesBitMap) {
                System.arraycopy(Serialization.encodeI8(b), 0, bytes, idx, 1);
                idx += 1;
            }
        }

        // 4. encode properties values
        len = Serialization.getNeedBytes(propertiesValues.size());
        System.arraycopy(Serialization.encodeVi64(propertiesValues.size()), 0, bytes, idx, len);
        idx += len;
        for (ObObj obj : propertiesValues) {
            len =  ObTableSerialUtil.getEncodedSize(obj);
            System.arraycopy(ObTableSerialUtil.encode(obj), 0, bytes, idx, len);
            idx += len;
        }

        return bytes;
    }

    private byte[] parseBitMap(long bitLen, List<String> aggColumnNames, List<String> columnNames, ByteBuf buf) {
        byte[] bitMap = new byte[(int) Math.ceil(bitLen / 8.0)];
        if (bitLen == 0) {
            // is same properties names
            columnNames.addAll(aggColumnNames);
        } else {
            for (int i = 0; i < bitMap.length; i++) {
                bitMap[i] = Serialization.decodeI8(buf);
                for (int j = 0; j < 8; j++) {
                    if ((bitMap[i] & (1 << j)) != 0) {
                        if (i * 8 + j < aggColumnNames.size()) {
                            columnNames.add(aggColumnNames.get(i * 8 + j));
                        }
                    }
                }
            }
        }
        return bitMap;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode header
        super.decode(buf);

        // 1. rowkey bitmap
        rowKeyBitLen = Serialization.decodeVi64(buf);
        rowKeyBitMap = parseBitMap(rowKeyBitLen, aggRowKeyNames, rowKeyNames, buf);

        // 2. rowkey obobj
        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            ObObj obj = new ObObj();
            ObTableSerialUtil.decode(buf, obj);
            rowkey.add(obj);
        }

        // 3. properties bitmap
        propertiesBitLen = Serialization.decodeVi64(buf);
        propertiesBitMap = parseBitMap(propertiesBitLen, aggPropertiesNames, propertiesNames, buf);

        // 4. properties obobj
        len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            ObObj obj = new ObObj();
            ObTableSerialUtil.decode(buf, obj);
            propertiesValues.add(obj);
        }

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long payloadContentSize = 0;

        payloadContentSize += Serialization.getNeedBytes(rowKeyBitLen);
        payloadContentSize += rowKeyBitMap.length;

        payloadContentSize += Serialization.getNeedBytes(rowkey.size());
        for (ObObj obj : rowkey) {
            payloadContentSize += ObTableSerialUtil.getEncodedSize(obj);
        }

        if (ignoreEncodePropertiesColumnNames) {
            payloadContentSize += Serialization.getNeedBytes(0L);
        } else {
            payloadContentSize += Serialization.getNeedBytes(propertiesBitLen);
            payloadContentSize += propertiesBitMap.length;
        }

        payloadContentSize += Serialization.getNeedBytes(propertiesValues.size());
        for (ObObj obj : propertiesValues) {
            payloadContentSize += ObTableSerialUtil.getEncodedSize(obj);
        }

        return payloadContentSize;
    }

    public static boolean areArraysSameLengthOrBothNull(Object[] a, Object[] b) {
        if (a == null && b == null) {
            return true;
        } else if (a != null && b != null && a.length == b.length) {
            return true;
        } else {
            return false;
        }
    }

    public static ObTableSingleOpEntity getInstance(String[] rowKeyNames, Object[] rowKey,
                                                    String[] propertiesNames, Object[] propertiesValues) {
        ObTableSingleOpEntity entity = new ObTableSingleOpEntity();
        if (!areArraysSameLengthOrBothNull(rowKeyNames, rowKey)) {
            throw new IllegalArgumentException(String.format(
                    "rowKey names length should be equals to rowKey values length, rowkeyNames: %s, rowkey: %s,",
                    rowKeyNames, rowKey));
        }

        if (rowKey != null) {
            for (int i = 0; i < rowKey.length; i++) {
                String name = rowKeyNames[i];
                Object rowkey = rowKey[i];
                ObObj obj = ObObj.getInstance(rowkey);
                entity.addRowKeyValue(name, obj);
            }
        }
        if (propertiesNames != null) {
            for (int i = 0; i < propertiesNames.length; i++) {
                String name = propertiesNames[i];
                Object value = null;
                if (propertiesValues != null) {
                    value = propertiesValues[i];
                }
                ObObj c = ObObj.getInstance(value);
                entity.addPropertyValue(name, c);
            }
        }
        return entity;
    }

    // Support class, which is used for column name sorted
    private static class ColumnNamePair implements Comparable<ColumnNamePair> {
        long number;
        // we could use idx here, and adjust obj after compare
        ObObj obj;

        ColumnNamePair(long number, ObObj obj) {
            this.number = number;
            this.obj = obj;
        }

        @Override
        public int compareTo(ColumnNamePair other) {
            return Long.compare(this.number, other.number);
        }
    }

    /*
     * isSamePropertiesColumnNamesLen check whether length is equal
     */
    public boolean isSamePropertiesColumnNamesLen(int columnNameIdxMapLen) {
        return columnNameIdxMapLen == this.propertiesNames.size();
    }

    public void adjustRowkeyColumnName(Map<String, Long> columnNameIdxMap) {
        this.rowKeyBitLen = columnNameIdxMap.size();
        int size = (int) Math.ceil(columnNameIdxMap.size() / 8.0);
        byte[] byteArray = new byte[size];
        List<Long> columnNameIdx = new LinkedList<>();

        for (String name : rowKeyNames) {
            Long index = columnNameIdxMap.get(name);
            columnNameIdx.add(index);
            if (index != null) {
                int byteIndex = index.intValue() / 8;
                int bitIndex = index.intValue() % 8;
                byteArray[byteIndex] |= (byte) (1 << bitIndex);
            }
        }

        List<ColumnNamePair> pairs = new ArrayList<>();
        for (int i = 0; i < columnNameIdx.size(); i++) {
            pairs.add(new ColumnNamePair(columnNameIdx.get(i), rowkey.get(i)));
        }

        Collections.sort(pairs);

        rowkey = new ArrayList<>(pairs.size());
        for (ColumnNamePair pair : pairs) {
            rowkey.add(pair.obj);
        }

        this.rowKeyBitMap = byteArray;
    }

    public void adjustPropertiesColumnName(Map<String, Long> columnNameIdxMap) {
        if (!ignoreEncodePropertiesColumnNames) {
            this.propertiesBitLen = columnNameIdxMap.size();
        }
        int size = (int) Math.ceil(columnNameIdxMap.size() / 8.0);
        byte[] byteArray = new byte[size];
        List<Long> columnNameIdx = new LinkedList<>();

        for (String name : propertiesNames) {
            Long index = columnNameIdxMap.get(name);
            columnNameIdx.add(index);
            if (index != null) {
                int byteIndex = index.intValue() / 8;
                int bitIndex = index.intValue() % 8;
                byteArray[byteIndex] |= (byte) (1 << bitIndex);
            }
        }

        List<ColumnNamePair> pairs = new ArrayList<>();
        for (int i = 0; i < columnNameIdx.size(); i++) {
            pairs.add(new ColumnNamePair(columnNameIdx.get(i), propertiesValues.get(i)));
        }

        Collections.sort(pairs);

        propertiesValues = new ArrayList<>(pairs.size());
        for (ColumnNamePair pair : pairs) {
            propertiesValues.add(pair.obj);
        }

        this.propertiesBitMap = byteArray;
    }

    public void addRowKeyValue(String rowKeyName, ObObj rowKeyValue) {
        this.rowKeyNames.add(rowKeyName);
        this.rowkey.add(rowKeyValue);
    }

    public void addPropertyValue(String propertyName, ObObj propertyValue) {
        this.propertiesNames.add(propertyName);
        this.propertiesValues.add(propertyValue);
    }

    public List<String> getRowKeyNames() {
        return this.rowKeyNames;
    }

    public List<String> getPropertiesNames() {
        return this.propertiesNames;
    }

    public void setIgnoreEncodePropertiesColumnNames(boolean ignoreEncodePropertiesColumnNames) {
        this.ignoreEncodePropertiesColumnNames = ignoreEncodePropertiesColumnNames;
    }

    public boolean ignoreEncodePropertiesColumnNames() {
        return this.ignoreEncodePropertiesColumnNames;
    }

    public void setAggRowKeyNames(List<String> columnNames) {
        this.aggRowKeyNames = columnNames;
    }

    public void setAggPropertiesNames(List<String> columnNames) {
        this.aggPropertiesNames = columnNames;
    }

    public Map<String, Object> getSimpleProperties() {
        Map<String, Object> values = new HashMap<String, Object>((int) propertiesValues.size());
        for (int i = 0; i < propertiesValues.size(); i++) {
            values.put(propertiesNames.get(i), propertiesValues.get(i).getValue());
        }
        return values;
    }

    public List<ObObj> getRowkey() {
        return rowkey;
    }

    public void setRowkey(List<ObObj> rowkey) {
        this.rowkey = rowkey;
    }

}

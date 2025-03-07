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
import com.alipay.oceanbase.rpc.util.ObByteBuf;
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
    private boolean isHbase = false;
    public ObTableSingleOpEntity() {}

    public boolean isHbase() {
        return isHbase;
    }

    public void setHbase(boolean hbase) {
        isHbase = hbase;
    }

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
        byte[] tmpBytes = Serialization.encodeVi64(rowKeyBitLen);
        System.arraycopy(tmpBytes, 0, bytes, idx, tmpBytes.length);
        idx += tmpBytes.length;
        for (byte b : rowKeyBitMap) {
            System.arraycopy(Serialization.encodeI8(b), 0, bytes, idx, 1);
            idx += 1;
        }

        // 2. encode rowkey
        tmpBytes = Serialization.encodeVi64(rowkey.size());
        System.arraycopy(tmpBytes, 0, bytes, idx, tmpBytes.length);
        idx += tmpBytes.length;
        for (ObObj obj : rowkey) {
            tmpBytes =  ObTableSerialUtil.encode(obj);
            System.arraycopy(tmpBytes, 0, bytes, idx, tmpBytes.length);
            idx += tmpBytes.length;
        }

        // 3. encode property bitmap
        if (ignoreEncodePropertiesColumnNames) {
            tmpBytes = Serialization.encodeVi64(0L);
            System.arraycopy(tmpBytes, 0, bytes, idx, tmpBytes.length);
            idx += tmpBytes.length;
        } else {
            tmpBytes = Serialization.encodeVi64(propertiesBitLen);
            System.arraycopy(tmpBytes, 0, bytes, idx, tmpBytes.length);
            idx += tmpBytes.length;
            for (byte b : propertiesBitMap) {
                System.arraycopy(Serialization.encodeI8(b), 0, bytes, idx, 1);
                idx += 1;
            }
        }

        // 4. encode properties values
        tmpBytes = Serialization.encodeVi64(propertiesValues.size());
        System.arraycopy(tmpBytes, 0, bytes, idx, tmpBytes.length);
        idx += tmpBytes.length;
        for (ObObj obj : propertiesValues) {
            tmpBytes = ObTableSerialUtil.encode(obj);
            System.arraycopy(tmpBytes, 0, bytes, idx, tmpBytes.length);
            idx += tmpBytes.length;
        }

        return bytes;
    }

    public void encode(ObByteBuf buf) {
        // 0. encode header
        encodeHeader(buf);

        // 1. encode rowKey bitmap
        if (isHbase) {
            Serialization.encodeVi64(buf, 3L);
            Serialization.encodeI8(buf, (byte) 0b00000111);
        } else {
            Serialization.encodeVi64(buf, rowKeyBitLen);
            for (byte b : rowKeyBitMap) {
                Serialization.encodeI8(buf, b);
            }
        }


        // 2. encode rowkey
        Serialization.encodeVi64(buf, rowkey.size());
        for (ObObj obj : rowkey) {
            ObTableSerialUtil.encode(buf, obj);
        }

        // 3. encode property bitmap
        if (ignoreEncodePropertiesColumnNames) {
            Serialization.encodeVi64(buf,0L);
        } else if (isHbase) {
            Serialization.encodeVi64(buf, 1);
            Serialization.encodeI8(buf, (byte) 0b00000001);
        } else {
            Serialization.encodeVi64(buf, propertiesBitLen);
            for (byte b : propertiesBitMap) {
                Serialization.encodeI8(buf, b);
            }
        }

        // 4. encode properties values
        Serialization.encodeVi64(buf, propertiesValues.size());
        for (ObObj obj : propertiesValues) {
            ObTableSerialUtil.encode(buf, obj);
        }
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
        if (this.payLoadContentSize == -1) {
            long payloadContentSize = 0;
            if (isHbase) {
                payloadContentSize += Serialization.getNeedBytes(3L);
                payloadContentSize += 1;
            } else {
                payloadContentSize += Serialization.getNeedBytes(rowKeyBitLen);
                payloadContentSize += rowKeyBitMap.length;
            }

            payloadContentSize += Serialization.getNeedBytes(rowkey.size());
            for (ObObj obj : rowkey) {
                payloadContentSize += ObTableSerialUtil.getEncodedSize(obj);
            }

            if (ignoreEncodePropertiesColumnNames) {
                payloadContentSize += Serialization.getNeedBytes(0L);
            } else if (isHbase) {
                payloadContentSize += Serialization.getNeedBytes(1L);
                payloadContentSize += 1;
            } else {
                payloadContentSize += Serialization.getNeedBytes(propertiesBitLen);
                payloadContentSize += propertiesBitMap.length;
            }

            payloadContentSize += Serialization.getNeedBytes(propertiesValues.size());
            for (ObObj obj : propertiesValues) {
                payloadContentSize += ObTableSerialUtil.getEncodedSize(obj);
            }
            this.payLoadContentSize = payloadContentSize;
        }

        return this.payLoadContentSize;
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

    // Support class, which is used for column name sorted
    private static class ColumnNameValuePair implements Comparable<ColumnNameValuePair> {
        long number;
        // we could use idx here, and adjust obj after compare
        String name;
        ObObj obj;

        ColumnNameValuePair(long number, String name, ObObj obj) {
            this.number = number;
            this.name = name;
            this.obj = obj;
        }

        @Override
        public int compareTo(ColumnNameValuePair other) {
            return Long.compare(this.number, other.number);
        }
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

        List<ColumnNameValuePair> pairs = new ArrayList<>();
        for (int i = 0; i < columnNameIdx.size(); i++) {
            pairs.add(new ColumnNameValuePair(columnNameIdx.get(i), propertiesNames.get(i), propertiesValues.get(i)));
        }

        Collections.sort(pairs);

        propertiesNames = new ArrayList<>(pairs.size());
        propertiesValues = new ArrayList<>(pairs.size());
        for (ColumnNameValuePair pair : pairs) {
            propertiesNames.add(pair.name);
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

    public List<ObObj> getPropertiesValues() {
        return this.propertiesValues;
    }

}

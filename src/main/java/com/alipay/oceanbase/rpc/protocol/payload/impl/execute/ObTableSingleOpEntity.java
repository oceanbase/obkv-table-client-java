package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjMeta;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class ObTableSingleOpEntity  extends AbstractPayload {
    private List<String> rowKeyNames = new ArrayList<>();
    private List<ObObj> rowkey = new ArrayList<>();
    private List<String> propertiesNames = new ArrayList<>();
    private List<ObObj> propertiesValues = new ArrayList<>();

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

        // 1. encode rowKey names
        int len = Serialization.getNeedBytes(rowKeyNames.size());
        System.arraycopy(Serialization.encodeVi64(rowKeyNames.size()), 0, bytes, idx, len);
        idx += len;
        for (String column : rowKeyNames) {
            len =  Serialization.getNeedBytes(column);
            System.arraycopy(Serialization.encodeVString(column), 0, bytes, idx, len);
            idx += len;
        }

        // 2. encode scan ranges
        len = Serialization.getNeedBytes(rowkey.size());
        System.arraycopy(Serialization.encodeVi64(rowkey.size()), 0, bytes, idx, len);
        idx += len;
        for (ObObj obj : rowkey) {
            len =  obj.getEncodedSize();
            System.arraycopy(obj.encode(), 0, bytes, idx, len);
            idx += len;
        }

        // 3. encode property names
        len = Serialization.getNeedBytes(propertiesNames.size());
        System.arraycopy(Serialization.encodeVi64(propertiesNames.size()), 0, bytes, idx, len);
        idx += len;
        for (String column : propertiesNames) {
            len =  Serialization.getNeedBytes(column);
            System.arraycopy(Serialization.encodeVString(column), 0, bytes, idx, len);
            idx += len;
        }

        // 4. encode properties values
        len = Serialization.getNeedBytes(propertiesValues.size());
        System.arraycopy(Serialization.encodeVi64(propertiesValues.size()), 0, bytes, idx, len);
        idx += len;
        for (ObObj obj : propertiesValues) {
            len =  obj.getEncodedSize();
            System.arraycopy(obj.encode(), 0, bytes, idx, len);
            idx += len;
        }

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        // 0. decode header
        super.decode(buf);

        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            String column = Serialization.decodeVString(buf);
            rowKeyNames.add(column);
        }

        len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            ObObj obj = new ObObj();
            obj.decode(buf);
            rowkey.add(obj);
        }

        len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            String column = Serialization.decodeVString(buf);
            propertiesNames.add(column);
        }

        len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            ObObj obj = new ObObj();
            obj.decode(buf);
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

        payloadContentSize += Serialization.getNeedBytes(rowKeyNames.size());
        for (String column : rowKeyNames) {
            payloadContentSize += Serialization.getNeedBytes(column);
        }

        payloadContentSize += Serialization.getNeedBytes(rowkey.size());
        for (ObObj obj : rowkey) {
            payloadContentSize += obj.getEncodedSize();
        }

        payloadContentSize += Serialization.getNeedBytes(propertiesNames.size());
        for (String column : propertiesNames) {
            payloadContentSize += Serialization.getNeedBytes(column);
        }

        payloadContentSize += Serialization.getNeedBytes(propertiesValues.size());
        for (ObObj obj : propertiesValues) {
            payloadContentSize += obj.getEncodedSize();
        }

        return payloadContentSize;
    }

    public static ObTableSingleOpEntity getInstance(String[] rowKeyNames, Object[] rowKey,
                                                    String[] propertiesNames, Object[] propertiesValues) {
        if (rowKeyNames.length != rowKey.length || propertiesNames.length != propertiesValues.length) {
            throw new IllegalArgumentException(String.format(
                    "column length should be equals to values length, rowkeyNames: %s, rowkey: %s,"
                            + "propertiesNames: %s," + "propertiesValues: %s",rowKeyNames,
                            rowKey, propertiesNames, propertiesValues));
        }
        ObTableSingleOpEntity entity = new ObTableSingleOpEntity();
        if (rowKey != null && rowKeyNames != null) {
            for (int i = 0; i < rowKey.length; i++) {
                String name = rowKeyNames[i];
                Object rowkey = rowKey[i];
                ObObjMeta rowkeyMeta = ObObjType.defaultObjMeta(rowkey);
                ObObj obj = new ObObj();
                obj.setMeta(rowkeyMeta);
                obj.setValue(rowkey);
                entity.addRowKeyValue(name, obj);
            }
        }

        if (propertiesNames != null && propertiesValues != null) {
            for (int i = 0; i < propertiesNames.length; i++) {
                String name = propertiesNames[i];
                Object value = propertiesValues[i];
                ObObjMeta meta = ObObjType.defaultObjMeta(value);
                ObObj c = new ObObj();
                c.setMeta(meta);
                c.setValue(value);
                entity.addPropertyValue(name, c);
            }
        }

        return entity;
    }

    public void addRowKeyValue(String rowKeyName, ObObj rowKeyValue) {
        this.rowKeyNames.add(rowKeyName);
        this.rowkey.add(rowKeyValue);
    }

    public void addPropertyValue(String propertyName, ObObj propertyValue) {
        this.propertiesNames.add(propertyName);
        this.propertiesValues.add(propertyValue);
    }
}

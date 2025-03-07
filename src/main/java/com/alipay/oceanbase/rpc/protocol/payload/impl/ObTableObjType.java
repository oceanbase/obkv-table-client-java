/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2024 OceanBase
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

package com.alipay.oceanbase.rpc.protocol.payload.impl;

import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;

public enum ObTableObjType {

    ObTableNullType(0) {
    },

    ObTableTinyIntType(1) {
    },

    ObTableSmallIntType(2) {
    },

    ObTableInt32Type(3) {
    },

    ObTableInt64Type(4) {
    },

    ObTableVarcharType(5) {
    },

    ObTableVarbinaryType(6) {
        @Override
        public void decode(ByteBuf buf, ObObj obj) {
            ObObjType objType = getObjType(this);
            ObObjMeta objMeta = objType.getDefaultObjMeta();
            objMeta.setCsType(ObCollationType.CS_TYPE_BINARY);
            obj.setMeta(objMeta);
            obj.setValueFromTableObj(objType.decode(buf, objMeta.getCsType()));
        }
    },

    ObTableDoubleType(7) {
    },

    ObTableFloatType(8) {
    },

    ObTableTimestampType(9) {
        @Override
        public byte[] encode(ObObj obj) {
            return encodeWithMeta(obj);
        }

        @Override
        public void encode(ObByteBuf buf, ObObj obj) {
            encodeWithMeta(buf, obj);
        }

        @Override
        public void decode(ByteBuf buf, ObObj obj) {
            decodeWithMeta(buf, obj);
        }

        @Override
        public int getEncodedSize(ObObj obj) {
            return getEncodedSizeWithMeta(obj);
        }
    },

    ObTableDateTimeType(10) {
        @Override
        public byte[] encode(ObObj obj) {
            return encodeWithMeta(obj);
        }

        @Override
        public void encode(ObByteBuf buf, ObObj obj) {
            encodeWithMeta(buf, obj);
        }

        @Override
        public void decode(ByteBuf buf, ObObj obj) {
            decodeWithMeta(buf, obj);
        }

        @Override
        public int getEncodedSize(ObObj obj) {
            return getEncodedSizeWithMeta(obj);
        }
    },

    ObTableMinType(11) {
        public byte[] encode(ObObj obj) {
            byte[] bytes = new byte[this.getEncodedSize(obj)];
            int idx = 0;
            System.arraycopy(Serialization.encodeI8(this.getValue()), 0, bytes, idx, 1);
            idx += 1;
            return bytes;
        }

        public void encode(ObByteBuf buf, ObObj obj) {
            Serialization.encodeI8(buf, this.getValue());
        }

        public void decode(ByteBuf buf, ObObj obj) {
            ObObjType objType = getObjType(this);
            ObObjMeta objMeta = objType.getDefaultObjMeta();
            obj.setMeta(objMeta);
            obj.setValueFromTableObj(-2L);
        }

        public int getEncodedSize(ObObj obj) {
            return DEFAULT_TABLE_OBJ_TYPE_SIZE;
        }
    },

    ObTableMaxType(12) {
        public byte[] encode(ObObj obj) {
            byte[] bytes = new byte[this.getEncodedSize(obj)];
            int idx = 0;
            System.arraycopy(Serialization.encodeI8(this.getValue()), 0, bytes, idx, 1);
            idx += 1;
            return bytes;
        }

        public void encode(ObByteBuf buf, ObObj obj) {
            Serialization.encodeI8(buf, this.getValue());
        }

        public void decode(ByteBuf buf, ObObj obj) {
            ObObjType objType = getObjType(this);
            ObObjMeta objMeta = objType.getDefaultObjMeta();
            obj.setMeta(objMeta);
            obj.setValueFromTableObj(-3L);
        }

        public int getEncodedSize(ObObj obj) {
            return DEFAULT_TABLE_OBJ_TYPE_SIZE;
        }
    },

    // 13 ObTableUTinyIntType
    // 14 ObTableUSmallIntType
    // 15 ObTableUInt32Type
    // 16 ObTableUInt64Type

    ObTableTinyTextType(17) {
        public void decode(ByteBuf buf, ObObj obj) {
            decodeWithUtf8(buf, obj);
        }
    },

    ObTableTextType(18) {
        public void decode(ByteBuf buf, ObObj obj) {
            decodeWithUtf8(buf, obj);
        }
    },

    ObTableMediumTextType(19) {
        public void decode(ByteBuf buf, ObObj obj) {
            decodeWithUtf8(buf, obj);
        }
    },

    ObTableLongTextType(20) {
        public void decode(ByteBuf buf, ObObj obj) {
            decodeWithUtf8(buf, obj);
        }
    },

    ObTableTinyBlobType(21) {
    },

    ObTableBlobType(22) {
    },

    ObTableMediumBlobType(23) {
    },

    ObTableLongBlobType(24) {
    },

    ObTableCharType(25) {
    },

    ObTableInvalidType(26) {
    };

    private int                                 value;
    // mapping from value to enum
    private static Map<Integer, ObTableObjType> valueMap = new HashMap<Integer, ObTableObjType>();
    // mapping from ObTableObjType to ObObjType
    private static Map<ObTableObjType, ObObjType> tableObjTypeMap = new HashMap<>();

    ObTableObjType(int value) {
        this.value = value;
    }

    static {
        for (ObTableObjType type : ObTableObjType.values()) {
            valueMap.put(type.value, type);
        }
    }

    public static ObTableObjType getTableObjType(ObObj obj) {
        ObObjType objType = obj.getMeta().getType();
        ObCollationType objCsType = obj.getMeta().getCsType();
        if (objType == ObObjType.ObNullType) {
            // only for GET operation default value
            return ObTableNullType;
        } else if (objType == ObObjType.ObTinyIntType) {
            return ObTableTinyIntType;
        } else if (objType == ObObjType.ObSmallIntType) {
            return ObTableObjType.ObTableSmallIntType;
        } else if (objType == ObObjType.ObInt32Type) {
            return ObTableObjType.ObTableInt32Type;
        } else if (objType == ObObjType.ObInt64Type) {
            return ObTableObjType.ObTableInt64Type;
        } else if (objType == ObObjType.ObVarcharType) {
            if (objCsType == ObCollationType.CS_TYPE_BINARY) {
                return ObTableObjType.ObTableVarbinaryType;
            } else {
                return ObTableObjType.ObTableVarcharType;
            }
        } else if (objType == ObObjType.ObDoubleType) {
            return ObTableObjType.ObTableDoubleType;
        } else if (objType == ObObjType.ObFloatType) {
            return ObTableObjType.ObTableFloatType;
        } else if (objType == ObObjType.ObTimestampType) {
            return ObTableObjType.ObTableTimestampType;
        } else if (objType == ObObjType.ObDateTimeType) {
            return ObTableObjType.ObTableDateTimeType;
        } else if (objType == ObObjType.ObExtendType) {
            if (obj.isMinObj()) {
                return ObTableObjType.ObTableMinType;
            } else if (obj.isMaxObj()) {
                return ObTableObjType.ObTableMaxType;
            }
        } else if (objType == ObObjType.ObCharType) {
            return ObTableObjType.ObTableCharType;
        }

        throw new IllegalArgumentException("cannot get ObTableObjType, invalid ob obj type: "
                                           + objType.getClass().getName());
    }

    static {
        tableObjTypeMap.put(ObTableNullType, ObObjType.ObNullType);
        tableObjTypeMap.put(ObTableTinyIntType, ObObjType.ObTinyIntType);
        tableObjTypeMap.put(ObTableSmallIntType, ObObjType.ObSmallIntType);
        tableObjTypeMap.put(ObTableInt32Type, ObObjType.ObInt32Type);
        tableObjTypeMap.put(ObTableInt64Type, ObObjType.ObInt64Type);
        tableObjTypeMap.put(ObTableVarcharType, ObObjType.ObVarcharType);
        tableObjTypeMap.put(ObTableVarbinaryType, ObObjType.ObVarcharType);
        tableObjTypeMap.put(ObTableDoubleType, ObObjType.ObDoubleType);
        tableObjTypeMap.put(ObTableFloatType, ObObjType.ObFloatType);
        tableObjTypeMap.put(ObTableTimestampType, ObObjType.ObTimestampType);
        tableObjTypeMap.put(ObTableDateTimeType, ObObjType.ObDateTimeType);
        tableObjTypeMap.put(ObTableTinyTextType, ObObjType.ObTinyTextType);
        tableObjTypeMap.put(ObTableTextType, ObObjType.ObTextType);
        tableObjTypeMap.put(ObTableMediumTextType, ObObjType.ObMediumTextType);
        tableObjTypeMap.put(ObTableLongTextType, ObObjType.ObLongTextType);
        tableObjTypeMap.put(ObTableTinyBlobType, ObObjType.ObTinyTextType);
        tableObjTypeMap.put(ObTableBlobType, ObObjType.ObTextType);
        tableObjTypeMap.put(ObTableMediumBlobType, ObObjType.ObMediumTextType);
        tableObjTypeMap.put(ObTableLongBlobType, ObObjType.ObLongTextType);
        tableObjTypeMap.put(ObTableMinType, ObObjType.ObExtendType);
        tableObjTypeMap.put(ObTableMaxType, ObObjType.ObExtendType);
        tableObjTypeMap.put(ObTableCharType, ObObjType.ObCharType);
    }

    public static ObObjType getObjType(ObTableObjType tableObjType) {
        ObObjType objType = tableObjTypeMap.get(tableObjType);
        if (objType == null) {
            throw new IllegalArgumentException("cannot get ObTableObjType, invalid table obj type: "
                    + tableObjType.getClass().getName());
        }
        return objType;
    }

    /*
     * Value of.
     */
    public static ObTableObjType valueOf(int value) {
        return valueMap.get(value);
    }

    /*
     * Get value.
     */
    public byte getValue() {
        return (byte) value;
    }

    public byte[] encode(ObObj obj) {
        ObObjType objType = obj.getMeta().getType();
        byte[] bytes = new byte[this.getEncodedSize(obj)];
        int idx = 0;
        System.arraycopy(Serialization.encodeI8(this.getValue()), 0, bytes, idx, 1);
        idx += 1;

        byte[] valueBytes = objType.encode(obj.getValue());
        System.arraycopy(valueBytes, 0, bytes, idx, valueBytes.length);
        idx += valueBytes.length;

        return bytes;
    }

    public void encode(ObByteBuf buf, ObObj obj) {
        ObObjType objType = obj.getMeta().getType();
        Serialization.encodeI8(buf, this.getValue());

        objType.encode(buf, obj.getValue());
    }

    public void decode(ByteBuf buf, ObObj obj) {
        ObObjType objType = getObjType(this);
        ObObjMeta objMeta = objType.getDefaultObjMeta();
        obj.setMeta(objMeta);
        obj.setValueFromTableObj(objType.decode(buf, objMeta.getCsType()));
    }

    public int getEncodedSize(ObObj obj) {
        ObObjType objType = obj.getMeta().getType();
        return DEFAULT_TABLE_OBJ_TYPE_SIZE + objType.getEncodedSize(obj.getValue());
    }

    public byte[] encodeWithMeta(ObObj obj) {
        ObObjType objType = obj.getMeta().getType();
        ObTableObjType tableObjType = getTableObjType(obj);
        byte[] bytes = new byte[tableObjType.getEncodedSize(obj)];
        int idx = 0;
        System.arraycopy(Serialization.encodeI8(tableObjType.getValue()), 0, bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeI8(obj.getMeta().getCsLevel().getByteValue()), 0,
            bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeI8(obj.getMeta().getCsType().getByteValue()), 0,
            bytes, idx, 1);
        idx += 1;
        System.arraycopy(Serialization.encodeI8(obj.getMeta().getScale()), 0, bytes, idx, 1);
        idx += 1;
        byte[] valueBytes = objType.encode(obj.getValue());
        System.arraycopy(valueBytes, 0, bytes, idx, valueBytes.length);
        idx += valueBytes.length;

        return bytes;
    }

    public void encodeWithMeta(ObByteBuf buf, ObObj obj) {
        ObObjType objType = obj.getMeta().getType();

        Serialization.encodeI8(buf, this.getValue());

        Serialization.encodeI8(buf, obj.getMeta().getCsLevel().getByteValue());

        Serialization.encodeI8(buf, obj.getMeta().getCsType().getByteValue());

        Serialization.encodeI8(buf, obj.getMeta().getScale());

        objType.encode(buf, obj.getValue());
    }

    public void decodeWithMeta(ByteBuf buf, ObObj obj) {
        ObObjType objType = getObjType(this);
        ObObjMeta meta = obj.getMeta();
        meta.setType(objType);
        meta.setCsLevel(ObCollationLevel.valueOf(Serialization.decodeI8(buf.readByte())));
        meta.setCsType(ObCollationType.valueOf(Serialization.decodeI8(buf.readByte())));
        meta.setScale(Serialization.decodeI8(buf.readByte()));
        obj.setValueFromTableObj(objType.decode(buf, meta.getCsType()));
    }

    public int getEncodedSizeWithMeta(ObObj obj) {
        ObObjType objType = getObjType(this);
        int len = DEFAULT_TABLE_OBJ_META_SIZE + objType.getEncodedSize(obj.getValue());
        return len;
    }

    public void decodeWithUtf8(ByteBuf buf, ObObj obj) {
        ObObjType objType = getObjType(this);
        ObObjMeta objMeta = objType.getDefaultObjMeta();
        objMeta.setCsType(ObCollationType.CS_TYPE_UTF8MB4_GENERAL_CI);
        obj.setMeta(objMeta);
        obj.setValueFromTableObj(objType.decode(buf, objMeta.getCsType()));
    }

    public static int DEFAULT_TABLE_OBJ_TYPE_SIZE = 1;
    public static int DEFAULT_TABLE_OBJ_META_SIZE = 4;
}

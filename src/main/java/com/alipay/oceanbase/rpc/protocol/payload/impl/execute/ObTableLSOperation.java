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
import com.alipay.oceanbase.rpc.protocol.payload.Constants;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.*;

import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;

public class ObTableLSOperation extends AbstractPayload {

    private List<ObTableTabletOp> tabletOperations = new ArrayList<ObTableTabletOp>();
    private long                  lsId             = INVALID_LS_ID;                   // i64
    private String tableName;

    private long tableId = Constants.OB_INVALID_ID;;

    // common column names for all single operation
    private List<String> rowKeyNames = new ArrayList<>();
    private Set<String> rowKeyNamesSet = new LinkedHashSet<>();
    private Map<String, Long> rowkeyColumnNamesIdxMap = new HashMap<>();

    private List<String> propertiesNames = new ArrayList<>();
    private Set<String> propertiesNamesSet = new LinkedHashSet<>();
    private Map<String, Long> propertiesColumnNamesIdxMap = new HashMap<>();

    private ObTableLSOpFlag       optionFlag       = new ObTableLSOpFlag();

    private static final int      LS_ID_SIZE       = 8;
    private static final long     INVALID_LS_ID    = -1;

    /*
    OB_UNIS_DEF_SERIALIZE(ObTableLSOp,
                      ls_id_,
                      option_flag_,
                      tablet_ops_);
     */

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        idx = encodeHeader(bytes, idx);

        // 1. encode ls id
        System.arraycopy(Serialization.encodeI64(lsId), 0, bytes, idx, 8);
        idx += 8;

        // 2. encode table name
        int len =  Serialization.getNeedBytes(tableName);
        System.arraycopy(Serialization.encodeVString(tableName), 0, bytes, idx, len);
        idx += len;

        // 3. encode table id
        len = Serialization.getNeedBytes(tableId);
        System.arraycopy(Serialization.encodeVi64(tableId), 0, bytes, idx, len);
        idx += len;

        // 4. encode rowKey names
        len = Serialization.getNeedBytes(rowKeyNames.size());
        System.arraycopy(Serialization.encodeVi64(rowKeyNames.size()), 0, bytes, idx, len);
        idx += len;
        for (String rowKeyName : rowKeyNames) {
            len =  Serialization.getNeedBytes(rowKeyName);
            System.arraycopy(Serialization.encodeVString(rowKeyName), 0, bytes, idx, len);
            idx += len;
        }

        // 5. encode properties names
        len = Serialization.getNeedBytes(propertiesNames.size());
        System.arraycopy(Serialization.encodeVi64(propertiesNames.size()), 0, bytes, idx, len);
        idx += len;
        for (String propertyName : propertiesNames) {
            len =  Serialization.getNeedBytes(propertyName);
            System.arraycopy(Serialization.encodeVString(propertyName), 0, bytes, idx, len);
            idx += len;
        }

        // 6. encode option flag
        len = Serialization.getNeedBytes(optionFlag.getValue());
        System.arraycopy(Serialization.encodeVi64(optionFlag.getValue()), 0, bytes, idx, len);
        idx += len;

        // 7. encode Operation
        len = Serialization.getNeedBytes(tabletOperations.size());
        System.arraycopy(Serialization.encodeVi64(tabletOperations.size()), 0, bytes, idx, len);
        idx += len;
        for (ObTableTabletOp tabletOperation : tabletOperations) {
            len = (int) tabletOperation.getPayloadSize();
            System.arraycopy(tabletOperation.encode(), 0, bytes, idx, len);
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

        // 1. decode others
        this.lsId = Serialization.decodeI64(buf);

        // 2. decode table name
        this.tableName = Serialization.decodeVString(buf);

        // 3. decode table id
        this.tableId = Serialization.decodeVi64(buf);

        // 4. decode rowKey names
        int len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            String rowkeyName = Serialization.decodeVString(buf);
            rowKeyNames.add(rowkeyName);
        }

        // 5. decode properties names
        len = (int) Serialization.decodeVi64(buf);
        for (int i = 0; i < len; i++) {
            String propertyName = Serialization.decodeVString(buf);
            propertiesNames.add(propertyName);
        }

        // 6. decode flags
        long flagValue = Serialization.decodeVi64(buf);
        optionFlag.setValue(flagValue);

        // 7. decode Operation
        len = (int) Serialization.decodeVi64(buf);
        tabletOperations = new ArrayList<ObTableTabletOp>(len);
        for (int i = 0; i < len; i++) {
            ObTableTabletOp tabletOperation = new ObTableTabletOp();
            tabletOperation.decode(buf);
            tabletOperations.add(tabletOperation);
        }

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long payloadContentSize = 0;
        payloadContentSize += Serialization.getNeedBytes(tabletOperations.size());
        for (ObTableTabletOp operation : tabletOperations) {
            payloadContentSize += operation.getPayloadSize();
        }

        payloadContentSize += Serialization.getNeedBytes(rowKeyNames.size());
        for (String rowKeyName : rowKeyNames) {
            payloadContentSize += Serialization.getNeedBytes(rowKeyName);
        }

        payloadContentSize += Serialization.getNeedBytes(propertiesNames.size());
        for (String propertyName : propertiesNames) {
            payloadContentSize += Serialization.getNeedBytes(propertyName);
        }

        return payloadContentSize + LS_ID_SIZE + Serialization.getNeedBytes(optionFlag.getValue())
                +  Serialization.getNeedBytes(tableName) + Serialization.getNeedBytes(tableId);
    }

    /*
     * Get table operations.
     */
    public List<ObTableTabletOp> getTabletOperations() {
        return tabletOperations;
    }

    /*
     * Add table operation.
     */
    public void addTabletOperation(ObTableTabletOp tabletOperation) {
        this.tabletOperations.add(tabletOperation);
        int length = this.tabletOperations.size();

        // set column names
        this.rowKeyNamesSet.addAll(tabletOperation.getRowKeyNamesSet());
        this.propertiesNamesSet.addAll(tabletOperation.getPropertiesNamesSet());

        if (length == 1 && tabletOperation.isSameType()) {
            setIsSameType(true);
            return;
        }

        if (isSameType()
            && length > 1
            && !(tabletOperation.isSameType() && (this.tabletOperations.get(length - 1)
                .getSingleOperations().get(0).getSingleOpType() == this.tabletOperations
                .get(length - 2).getSingleOperations().get(0).getSingleOpType()))) {
            setIsSameType(false);
        }
    }

    public void setLsId(long lsId) {
        this.lsId = lsId;
    }

    public void setReturnOneResult(boolean returnOneResult) {
        optionFlag.setReturnOneResult(returnOneResult);
    }

    public boolean isSameType() {
        return optionFlag.getFlagIsSameType();
    }

    public void setIsSameType(boolean isSameType) {
        optionFlag.setFlagIsSameType(isSameType);
    }

    public boolean isSamePropertiesNames() {
        return optionFlag.getFlagIsSamePropertiesNames();
    }

    public void setIsSamePropertiesNames(boolean isSamePropertiesNames) {
        optionFlag.setFlagIsSamePropertiesNames(isSamePropertiesNames);
    }

    public long getTableId() {
        return tableId;
    }

    public void setTableId(long tableId) {
        this.tableId = tableId;
    }

    public void prepareOption() {
        // set isSamePropertiesNames into entities
        if (isSamePropertiesNames()) {
            for (ObTableTabletOp tabletOp : tabletOperations) {
                for (ObTableSingleOp singleOp : tabletOp.getSingleOperations()) {
                    for (ObTableSingleOpEntity entity : singleOp.getEntities()) {
                        entity.setIgnoreEncodePropertiesColumnNames(true);
                        // todo: set other option in one loop
                    }
                }
            }
        }

        // todo: set other option in other loop
    }

    public void collectColumnNamesIdxMap() {
        // prepare rowkey idx map
        long index = 0;
        for (String rowkeyName : rowKeyNamesSet) {
            this.rowKeyNames.add(rowkeyName);
            this.rowkeyColumnNamesIdxMap.put(rowkeyName, index);
            index += 1;
        }

        // prepare properties idx map
        index = 0;
        for (String propertiesName : propertiesNamesSet) {
            this.propertiesNames.add(propertiesName);
            this.propertiesColumnNamesIdxMap.put(propertiesName, index);
            index += 1;
        }
    }

    /*
     * beforeOption is used to collect necessary data from entity before prepareOption
     */
    public void beforeOption() {
        boolean isSamePropertiesColumnNames = true;

        for (ObTableTabletOp tabletOp : tabletOperations) {
            for (ObTableSingleOp singleOp : tabletOp.getSingleOperations()) {
                for (ObTableSingleOpEntity entity : singleOp.getEntities()) {
                    // if column names are the same, then the length should be the same
                    isSamePropertiesColumnNames = entity.isSamePropertiesColumnNamesLen(this.propertiesColumnNamesIdxMap.size());
                    if (!isSamePropertiesColumnNames) break;
                }
                if (!isSamePropertiesColumnNames) break;
            }
            if (!isSamePropertiesColumnNames) break;
        }

        if (isSamePropertiesColumnNames) this.setIsSamePropertiesNames(true);
    }


    public void prepareColumnNamesBitMap() {
        // adjust query & entity
        for (ObTableTabletOp tabletOp : tabletOperations) {
            for (ObTableSingleOp singleOp : tabletOp.getSingleOperations()) {
                singleOp.getQuery().adjustScanRangeColumns(rowkeyColumnNamesIdxMap);
                for (ObTableSingleOpEntity entity : singleOp.getEntities()) {
                    entity.adjustRowkeyColumnName(rowkeyColumnNamesIdxMap);
                    entity.adjustPropertiesColumnName(propertiesColumnNamesIdxMap);
                }
            }
        }
    }

    public void prepare() {
        this.collectColumnNamesIdxMap();
        this.beforeOption();
        this.prepareOption();
        this.prepareColumnNamesBitMap();
    }

    public long getLsId() {
        return lsId;
    }

}

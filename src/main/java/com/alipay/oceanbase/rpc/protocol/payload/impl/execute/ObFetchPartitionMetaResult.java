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

import com.alipay.oceanbase.rpc.ObGlobal;
import com.alipay.oceanbase.rpc.exception.ObTablePartitionInfoRefreshException;
import com.alipay.oceanbase.rpc.location.model.TableEntry;
import com.alipay.oceanbase.rpc.location.model.partition.*;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Pcodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObCollationType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObjType;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObGeneratedColumnSimpleFunc;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.ObSimpleColumn;
import com.alipay.oceanbase.rpc.protocol.payload.impl.column.OdpSinglePartKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.parser.ObGeneratedColumnExpressParser;
import com.alipay.oceanbase.rpc.util.Serialization;
import io.netty.buffer.ByteBuf;

import java.util.*;

import static com.alipay.oceanbase.rpc.location.model.partition.ObPartitionKey.MAX_PARTITION_ELEMENT;
import static com.alipay.oceanbase.rpc.location.model.partition.ObPartitionKey.MIN_PARTITION_ELEMENT;
import static com.alipay.oceanbase.rpc.util.Serialization.encodeObUniVersionHeader;
import static com.alipay.oceanbase.rpc.util.Serialization.getObUniVersionHeaderLength;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.LCD;
import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.RUNTIME;
import static java.lang.String.format;

/**
 * OB_SERIALIZE_MEMBER(ObFetchPartitionMetaResult
 *  private long route_version_,
 *  private long create_time_,
 *  private long tableId_,
 *  private long partitionNum_,
 *  private ObPartitionInfo partitionInfo_,
 *  private odpFirstSingleParts_,
 *  private odpSubSingleParts_,
 * );
 */

public class ObFetchPartitionMetaResult extends AbstractPayload {
    private long                routeVersion;
    private long                createTime;
    private long                tableId;
    private long                partitionNum;
    private OdpPartitionData    odpPartitionData    = new OdpPartitionData();
    private List<OdpSinglePart> odpFirstSingleParts = new ArrayList<>();
    private List<OdpSinglePart> odpSubSingleParts   = new ArrayList<>();
    private Map<Long, Long>     tabletLsIdMap       = new HashMap<>();;
    private ObPartitionInfo     partitionInfo       = null;
    private TableEntry          tableEntry          = new TableEntry();

    public long getRouteVersion() {
        return routeVersion;
    }

    public long getCreateTime() {
        return createTime;
    }

    public TableEntry getTableEntry() {
        return tableEntry;
    }

    /*
     * Get pcode.
     */
    @Override
    public int getPcode() {
        return Pcodes.OB_TABLE_API_PART_META_QUERY;
    }

    /*
     * Encode.
     */
    @Override
    public byte[] encode() {
        byte[] bytes = new byte[(int) getPayloadSize()];
        int idx = 0;

        // 0. encode header
        // ver + plen + payload
        idx = encodeHeader(bytes, idx);

        // encode routeVersion + createTime
        int len = Serialization.getNeedBytes(routeVersion);
        System.arraycopy(Serialization.encodeVi64(routeVersion), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(createTime);
        System.arraycopy(Serialization.encodeVi64(createTime), 0, bytes, idx, len);
        idx += len;

        // encode tableId + partitionNum
        len = Serialization.getNeedBytes(tableId);
        System.arraycopy(Serialization.encodeVi64(tableId), 0, bytes, idx, len);
        idx += len;
        len = Serialization.getNeedBytes(partitionNum);
        System.arraycopy(Serialization.encodeVi64(partitionNum), 0, bytes, idx, len);
        idx += len;

        // encode partitionInfo into OdpPartitionData
        len = (int) odpPartitionData.getPayloadSize();
        System.arraycopy(odpPartitionData.encode(), 0, bytes, idx, len);
        idx += len;

        // encode first single part
        len = Serialization.getNeedBytes(odpFirstSingleParts.size());
        System.arraycopy(Serialization.encodeVi64(odpFirstSingleParts.size()), 0, bytes, idx, len);
        idx += len;
        for (OdpSinglePart singlePart : odpFirstSingleParts) {
            len = (int) singlePart.getPayloadSize();
            System.arraycopy(singlePart.encode(), 0, bytes, idx, len);
            idx += len;
        }

        // encode second single part
        len = Serialization.getNeedBytes(odpSubSingleParts.size());
        System.arraycopy(Serialization.encodeVi64(odpSubSingleParts.size()), 0, bytes, idx, len);
        idx += len;
        for (OdpSinglePart singlePart : odpSubSingleParts) {
            len = (int) singlePart.getPayloadSize();
            System.arraycopy(singlePart.encode(), 0, bytes, idx, len);
            idx += len;
        }

        return bytes;
    }

    /*
     * Decode.
     */
    @Override
    public Object decode(ByteBuf buf) {
        super.decode(buf); // get version and payload length

        // get route_version + create_time
        routeVersion = Serialization.decodeVi64(buf);
        createTime = Serialization.decodeVi64(buf);

        // get tableId + partitionNum
        tableId = Serialization.decodeVi64(buf);
        partitionNum = Serialization.decodeVi64(buf);
        // if the current table is a partition-table
        // decode odpPartitionData get partitionInfo data
        odpPartitionData.decode(buf);
        if (odpPartitionData.getLevel() == 1 || odpPartitionData.getLevel() == 2) {
            // get first partition id and bounds information
            long len = Serialization.decodeVi64(buf);
            for (long i = 0; i < len; ++i) {
                OdpSinglePart odpSinglePart = new OdpSinglePart();
                odpSinglePart.decode(buf);
                odpFirstSingleParts.add(odpSinglePart);
            }
            // get sub partition id and bounds information
            len = Serialization.decodeVi64(buf);
            for (long i = 0; i < len; ++i) {
                OdpSinglePart odpSinglePart = new OdpSinglePart();
                odpSinglePart.decode(buf);
                odpSubSingleParts.add(odpSinglePart);
            }
            // build ObPartitionInfo from OdpPartitionData
            partitionInfo = buildPartitionInfo(odpPartitionData);

            Map<Long, Long> tmpTableLsIdMap = new HashMap<>();
            if (partitionInfo != null) {
                if (partitionInfo.getFirstPartDesc() != null) {
                    ObPartFuncType firstPartFuncType = partitionInfo.getFirstPartDesc().getPartFuncType();
                    // build first ObPartDesc according to odpFirstSingleParts
                    buildFirstPartFromODP(odpFirstSingleParts, partitionInfo, firstPartFuncType);
                    // build tablet -> lsId according odpFirstSingleParts
                    if (ObGlobal.obVsnMajor() >= 4) {
                        tmpTableLsIdMap = buildTabletLsIdMap(odpFirstSingleParts);
                        this.tabletLsIdMap = tmpTableLsIdMap;
                    }
                }

                if (partitionInfo.getSubPartDesc() != null) {
                    ObPartFuncType subPartFuncType = partitionInfo.getSubPartDesc().getPartFuncType();
                    // build sub ObPartDesc according to odpSubSingleParts
                    buildSubPartFromODP(odpSubSingleParts, partitionInfo, subPartFuncType);
                    // build tablet -> lsId according odpSubSingleParts
                    if (ObGlobal.obVsnMajor() >= 4) {
                        tmpTableLsIdMap = buildTabletLsIdMap(odpSubSingleParts);
                        this.tabletLsIdMap = tmpTableLsIdMap;
                    }
                }
            }
        } else {
            if (odpPartitionData.getLevel() == 0) {
                partitionInfo = new ObPartitionInfo();
                partitionInfo.setLevel(ObPartitionLevel.valueOf(odpPartitionData.getLevel()));
            }
            tabletLsIdMap.put(0L, -1L);
        }
        ObPartitionEntry partitionEntry = new ObPartitionEntry();
        if (ObGlobal.obVsnMajor() >= 4) { // get tabletId -> lsId mapping only in 4.x
            // data from PROXY_LOCATION_SQL_PARTITION_V4/PROXY_LOCATION_SQL_PARTITION
            partitionEntry.setTabletLsIdMap(tabletLsIdMap);
        }
        tableEntry.setPartitionEntry(partitionEntry);
        tableEntry.setTableId(tableId);
        tableEntry.setPartitionNum(partitionNum);
        tableEntry.setPartitionInfo(partitionInfo);
        tableEntry.setRefreshMetaTimeMills(System.currentTimeMillis());
        tableEntry.setODPMetaCreateTimeMills(createTime);
        tableEntry.prepare();

        return this;
    }

    /*
     * Get payload content size.
     */
    @Override
    public long getPayloadContentSize() {
        long payloadContentSize = 0;
        payloadContentSize += Serialization.getNeedBytes(routeVersion);
        payloadContentSize += Serialization.getNeedBytes(createTime);
        payloadContentSize += Serialization.getNeedBytes(tableId);
        payloadContentSize += Serialization.getNeedBytes(partitionNum);
        payloadContentSize += odpPartitionData.getPayloadSize();
        payloadContentSize += odpFirstSingleParts.size();
        for (OdpSinglePart singlePart : odpFirstSingleParts) {
            payloadContentSize += singlePart.getPayloadSize();
        }
        payloadContentSize += odpSubSingleParts.size();
        for (OdpSinglePart singlePart : odpSubSingleParts) {
            payloadContentSize += singlePart.getPayloadSize();
        }

        return payloadContentSize;
    }


    /**
     Data from PROXY_PART_INFO_SQL_V4/PROXY_PART_INFO_SQL
     OB_SERIALIZE_MEMBER(ObPartitionInfo
         ObPartitionLevel level_(long index)
         (if exist) ObPartDesc firstPartDesc_
         (if exist) ObPartDesc subPartDesc_
         List<ObColumn> partColumns_
     );
     */
    private ObPartitionInfo buildPartitionInfo(OdpPartitionData odpPartitionData) {
        ObPartitionInfo partitionInfo = new ObPartitionInfo();
        // get level
        partitionInfo.setLevel(ObPartitionLevel.valueOf(odpPartitionData.getLevel()));
        // if there are first level partitions
        if (partitionInfo.getLevel().getIndex() >= ObPartitionLevel.LEVEL_ONE.getIndex()) {
            // build first partition
            // get partNum, partSpace, part type, partColumn names, range partition types
            partitionInfo.setFirstPartDesc(buildPartDesc(odpPartitionData, false));
        }
        // if there are second level partitions
        if (partitionInfo.getLevel().getIndex() == ObPartitionLevel.LEVEL_TWO.getIndex()) {
            // build sub partition
            partitionInfo.setSubPartDesc(buildPartDesc(odpPartitionData, true));
        }
        // get all partColumns
        List<OdpSinglePartKey> odpPartColumns = odpPartitionData.getSinglePartKeys();
        for (OdpSinglePartKey odpPartColumn : odpPartColumns) {
            String columnName = odpPartColumn.getColumnName();
            int index = (int) odpPartColumn.getIndex();
            ObObjType obObjType = ObObjType.valueOf((int) odpPartColumn.getObObjTypeIdx());
            ObCollationType obCollationType = ObCollationType.valueOf((int) odpPartColumn.getObCollationTypeIdx());
            String partKeyExtra = odpPartColumn.getPartKeyExtra();
            partKeyExtra = partKeyExtra.replace("`", "");
            partKeyExtra = partKeyExtra.replace(" ", "");
            if (partKeyExtra.isEmpty()) {
                ObSimpleColumn column = new ObSimpleColumn(columnName, index, obObjType, obCollationType);
                partitionInfo.addColumn(column);
            } else {
                ObGeneratedColumnSimpleFunc columnExpress =
                        new ObGeneratedColumnExpressParser(getPlainString(partKeyExtra)).parse();
                ObGeneratedColumn column = new ObGeneratedColumn(columnName, index, obObjType,
                        obCollationType, columnExpress);
                partitionInfo.addColumn(column);
            }
        }

        // set the property of first part and sub part
        List<ObColumn> firstPartColumns = new ArrayList<ObColumn>(), subPartColumns = new ArrayList<ObColumn>();
        if (null != partitionInfo.getFirstPartDesc()) {
            for (String partColumnNames : partitionInfo.getFirstPartDesc().getOrderedPartColumnNames()) {
                for (ObColumn curColumn : partitionInfo.getPartColumns()) {
                    if (curColumn.getColumnName().equalsIgnoreCase(partColumnNames)) {
                        firstPartColumns.add(curColumn);
                        break;
                    }
                }
            }
        }
        if (null != partitionInfo.getSubPartDesc()) {
            for (String partColumnNames : partitionInfo.getSubPartDesc().getOrderedPartColumnNames()) {
                for (ObColumn curColumn : partitionInfo.getPartColumns()) {
                    if (curColumn.getColumnName().equalsIgnoreCase(partColumnNames)) {
                        subPartColumns.add(curColumn);
                        break;
                    }
                }
            }
        }
        setPartColumnsToPartDesc(partitionInfo.getFirstPartDesc(), firstPartColumns);
        setPartColumnsToPartDesc(partitionInfo.getSubPartDesc(), subPartColumns);

        return partitionInfo;
    }

    private void setPartColumnsToPartDesc(ObPartDesc partDesc, List<ObColumn> partColumns) {
        if (partDesc != null) {
            partDesc.setPartColumns(partColumns);
            ObPartFuncType partType = partDesc.getPartFuncType();
            if (partType.isKeyPart()) {
                if (partColumns == null || partColumns.size() == 0) {
                    RUNTIME.error("key part desc need part ref columns but found " + partColumns);
                    throw new ObTablePartitionInfoRefreshException(
                            "key part desc need part ref columns but found " + partColumns);
                }
            }
            else if (partType.isRangePart()) {
                ((ObRangePartDesc) partDesc).setOrderedCompareColumns(partColumns);
            }
        }
    }

    private static String getPlainString(String str) {
        int start = str.length() > 0 && str.charAt(0) == '\'' ? 1 : 0;
        int end = str.length() > 0 && str.charAt(str.length() - 1) == '\'' ? str.length() - 1 : str
                .length();
        return str.substring(start, end);
    }

    /**
     Data from PROXY_PART_INFO_SQL_V4/PROXY_PART_INFO_SQL
     OB_SERIALIZE_MEMBER(ObPartDesc
         long version = 1
         ObPartFuncType partType_(byte)
         String partExpr_
         int32_t part_num_
         int32_t part_spcae_
         String objTypesStr_ (Range partition)
     );
     */
    private ObPartDesc buildPartDesc(OdpPartitionData odpPartitionData, boolean isSubPart) {
        ObPartDesc partDesc =  null;
        long partNum = (!isSubPart) ? odpPartitionData.getPartNum() : odpPartitionData.getSubPartNum();
        String partExpr = (!isSubPart)? odpPartitionData.getPartExr() : odpPartitionData.getSubPartExr();
        long partType = (!isSubPart) ? odpPartitionData.getPartType() : odpPartitionData.getSubPartType();
        long partSpace = (!isSubPart) ? odpPartitionData.getPartSpace() : odpPartitionData.getSubPartSpace();
        String objTypesStr = (!isSubPart) ? odpPartitionData.getPartRangeType() : odpPartitionData.getSubPartRangeType();

        ObPartFuncType obPartFuncType = ObPartFuncType.getObPartFuncType(partType);
        partExpr = partExpr.replace("`", "");

        if (obPartFuncType.isRangePart()) {
            ObRangePartDesc rangeDesc = new ObRangePartDesc();
            rangeDesc.setPartExpr(partExpr);
            // get part num for current level
            rangeDesc.setPartNum((int) partNum);
            // get part space for current level
            rangeDesc.setPartSpace((int) partSpace);
            // get ObObj typs for range partition
            ArrayList<ObObjType> types = new ArrayList<ObObjType>(1);
            for (String typeStr : objTypesStr.split(",")) {
                types.add(ObObjType.valueOf(Integer.valueOf(typeStr)));
            }
            rangeDesc.setOrderedCompareColumnTypes(types);
            partDesc = rangeDesc;
        } else if (obPartFuncType.isHashPart()) {
            ObHashPartDesc hashDesc = new ObHashPartDesc();
            hashDesc.setPartExpr(partExpr);
            // get part num for current level
            hashDesc.setPartNum((int) partNum);
            // get part space for current level
            hashDesc.setPartSpace((int) partSpace);
            partDesc = hashDesc;
        } else if (obPartFuncType.isKeyPart()) {
            ObKeyPartDesc keyDesc = new ObKeyPartDesc();
            keyDesc.setPartExpr(partExpr);
            // get part num for current level
            keyDesc.setPartNum((int) partNum);
            // get part space for current level
            keyDesc.setPartSpace((int) partSpace);
            partDesc = keyDesc;
        } else {
            RUNTIME.error(LCD.convert("01-00015"), partType);
            throw new IllegalArgumentException(format("not supported part type, type = %s",
                    partType));
        }
        return partDesc;
    }

    /**
     Data from PROXY_FIRST_PARTITION_SQL_V4/PROXY_FIRST_PARTITION_SQL
     OB_SERIALIZE_MEMBER(
         Range Partition:
             4.x: sub_part_num + highBoundVal + tabletIds + lsIds
             3.x: highBoundVal + partIds
         Hash/Key Partition
             4.x: sub_part_num
             3.x: no need
     );
     */
    private void buildFirstPartFromODP(List<OdpSinglePart> odpFirstSingleParts, ObPartitionInfo partitionInfo,
                                               ObPartFuncType obPartFuncType) {
        if (obPartFuncType.isRangePart()) {
            List<List<byte[]>> highBoundVals = new ArrayList<>();
            // get bounds for first partition
            List<ObComparableKV<ObPartitionKey, Long>> bounds = buildFirstRangePart(odpFirstSingleParts,
                    partitionInfo, highBoundVals);
            ((ObRangePartDesc) partitionInfo.getFirstPartDesc())
                    .setBounds(bounds);
            ((ObRangePartDesc) partitionInfo.getFirstPartDesc())
                    .setHighBoundValues(highBoundVals);
        }
        else if (ObGlobal.obVsnMajor() >= 4
                && (obPartFuncType.isKeyPart() || obPartFuncType.isHashPart())) {
            // get sub_part_num
            partitionInfo.setPartTabletIdMap(
                    buildFirstKeyHashPart(odpFirstSingleParts, partitionInfo));
        }
    }

    /**
     Data from PROXY_SUB_PARTITION_SQL_V4/PROXY_SUB_PARTITION_SQL
     OB_SERIALIZE_MEMBER(
         Range Partition:
             4.x: sub_part_num + highBoundVal + tabletIds + lsIds
             3.x: highBoundVal + partIds
     );
     */
    private void buildSubPartFromODP(List<OdpSinglePart> odpSubSingleParts, ObPartitionInfo partitionInfo,
                                               ObPartFuncType obPartFuncType)  {
        if (obPartFuncType.isRangePart()) {
            List<List<byte[]>> highBoundVals = new ArrayList<>();
            // get bounds for first partition
            List<ObComparableKV<ObPartitionKey, Long>> bounds = buildSubRangePart(odpSubSingleParts,
                    partitionInfo, highBoundVals);
            ((ObRangePartDesc) partitionInfo.getSubPartDesc())
                    .setBounds(bounds);
            ((ObRangePartDesc) partitionInfo.getSubPartDesc())
                    .setHighBoundValues(highBoundVals);
        }
        else if (ObGlobal.obVsnMajor() >= 4
                && (obPartFuncType.isKeyPart() || obPartFuncType.isHashPart())) {
            // get sub_part_num
            partitionInfo.setPartTabletIdMap(
                    buildSubKeyHashPart(odpSubSingleParts, partitionInfo));
        }
    }

    private List<ObComparableKV<ObPartitionKey, Long>> buildFirstRangePart(List<OdpSinglePart> odpFirstSingleParts, ObPartitionInfo partitionInfo,
                                                                                    List<List<byte[]>> highBoundVals) {
        return buildRangePart(odpFirstSingleParts, partitionInfo, highBoundVals, false);
    }

    private List<ObComparableKV<ObPartitionKey, Long>> buildSubRangePart(List<OdpSinglePart> odpSubSingleParts, ObPartitionInfo partitionInfo,
                                                                                 List<List<byte[]>> highBoundVals) {
        return buildRangePart(odpSubSingleParts, partitionInfo, highBoundVals, true);
    }

    /**
     OB_SERIALIZE_MEMBER(RangePart
         long sub_part_num_ (4.x)
         List<Object[]> [[highBoundVal, tabletId, lsId], ...] (4.x)
         List<Object[]> [[highBoundVal, partId], ...] (3.x)
     );
     */
    private List<ObComparableKV<ObPartitionKey, Long>> buildRangePart(List<OdpSinglePart> odpSingleParts, ObPartitionInfo partitionInfo,
                                                                      List<List<byte[]>> highBoundVals, boolean isSubPart) {
        ObPartDesc partDesc = partitionInfo.getFirstPartDesc();
        if (isSubPart) {
            partDesc = partitionInfo.getSubPartDesc();
        }
        List<ObColumn> orderPartColumns = ((ObRangePartDesc) partDesc).getOrderedCompareColumns();
        List<ObComparableKV<ObPartitionKey, Long>> bounds = new ArrayList<ObComparableKV<ObPartitionKey, Long>>();
        Map<String, Long> partNameIdMap = new HashMap<String, Long>();
        Map<Long, Long> partTabletIdMap = new HashMap<Long, Long>();
        ObPartDesc subPartDesc = partitionInfo.getSubPartDesc();
        if (ObGlobal.obVsnMajor() >= 4 && subPartDesc != null) {
            // get sub_part_num_ in 4.x
            if (!isSubPart && subPartDesc.getPartNum() == 0) {
                // client only support template partition table
                // so the sub_part_num is a constant and will store in subPartDesc which is different from proxy
                long subPartNum = odpSingleParts.get(0).getSubPartNum();
                subPartDesc.setPartNum((int) subPartNum);
            }
        }
        for (long i = 0; i < (long) odpSingleParts.size(); ++i) {
            // get highBoundVals
            String highBoundVal = odpSingleParts.get((int) i).getHighBoundVal();
            String[] splits = highBoundVal.split(",");
            List<Comparable> partElements = new ArrayList<Comparable>();
            List<byte[]> singleHighBoundVal = new ArrayList<byte[]>();

            for (int j = 0; j < splits.length; j++) {
                String elementStr = getPlainString(splits[j]);
                if (elementStr.equalsIgnoreCase("MAXVALUE")) {
                    singleHighBoundVal.add(new byte[0]); // like EMPTY_BYTE_ARRAY
                    partElements.add(MAX_PARTITION_ELEMENT);
                } else if (elementStr.equalsIgnoreCase("MINVALUE")) {
                    singleHighBoundVal.add(new byte[0]); // like EMPTY_BYTE_ARRAY
                    partElements.add(MIN_PARTITION_ELEMENT);
                } else {
                    ObObjType type = orderPartColumns.get(j).getObObjType();
                    partElements.add(type.parseToComparable(elementStr, orderPartColumns.get(j)
                            .getObCollationType()));
                    singleHighBoundVal.add(type.parseToBytes(elementStr, orderPartColumns.get(j)
                            .getObCollationType()));
                }
            }
            ObPartitionKey partitionKey = new ObPartitionKey(orderPartColumns, partElements);
            if (ObGlobal.obVsnMajor() >= 4) {
                // get tablet_id for 4.x
                long tabletId = odpSingleParts.get((int) i).getTabletId();
                bounds.add(new ObComparableKV<ObPartitionKey, Long>(partitionKey, i));
                highBoundVals.add(singleHighBoundVal);
                partTabletIdMap.put(i, tabletId);
            } else {
                // get part_id or sub_part_id for 3.x
                long partId = odpSingleParts.get((int) i).getPartId();
                bounds.add(new ObComparableKV<ObPartitionKey, Long>(partitionKey, partId));
                highBoundVals.add(singleHighBoundVal);
            }
        }
        if (ObGlobal.obVsnMajor() >= 4) {
            //set part_id -> tablet_id mapping
            partitionInfo.setPartTabletIdMap(partTabletIdMap);
        } else {
            //set part_id -> part_name mapping
            partDesc.setPartNameIdMap(partNameIdMap);
        }
        Collections.sort(bounds);
        return bounds;
    }

    private Map<Long, Long> buildFirstKeyHashPart(List<OdpSinglePart> odpFirstSingleParts, ObPartitionInfo partitionInfo) {
        return buildKeyHashPart(odpFirstSingleParts, partitionInfo, false);
    }

    private Map<Long, Long> buildSubKeyHashPart(List<OdpSinglePart> odpSubSingleParts, ObPartitionInfo partitionInfo) {
        return buildKeyHashPart(odpSubSingleParts, partitionInfo, true);
    }

    /**
     OB_SERIALIZE_MEMBER(KeyHashPart
     long sub_part_num_ (4.x)
     partId -> tabletId mapping
     );
     */
    private Map<Long, Long> buildKeyHashPart(List<OdpSinglePart> odpSingleParts, ObPartitionInfo partitionInfo, boolean isSubPart) {
        ObPartDesc subPartDesc = partitionInfo.getSubPartDesc();
        // in 4.x and if there are subParts, the tablet_ids queried from first partition are invalid
        // the real tablet_ids are in the sub partition query, the number is the total tablet number
        if (null != subPartDesc && !isSubPart) {
            OdpSinglePart odpSinglePart = odpSingleParts.get(0);
            // get sub_part_num
            long subPartNum = odpSinglePart.getSubPartNum();
            // client only support template partition table
            // so the sub_part_num is a constant and will store in subPartDesc which is different from proxy
            if (subPartDesc.getPartFuncType().isKeyPart()) {
                ObKeyPartDesc subKeyPartDesc = (ObKeyPartDesc) subPartDesc;
                if (subKeyPartDesc.getPartNum() == 0) {
                    subKeyPartDesc.setPartNum((int) subPartNum);
                }
            } else if (subPartDesc.getPartFuncType().isHashPart()) {
                ObHashPartDesc subHashPartDesc = (ObHashPartDesc) subPartDesc;
                if (subHashPartDesc.getPartNum() == 0) {
                    subHashPartDesc.setPartNum((int) subPartNum);
                }
            }
        }
        Map<Long, Long> partTabletIdMap = new HashMap<Long, Long>();
        for (long i = 0; i < (long) odpSingleParts.size(); ++i) {
            partTabletIdMap.put(i, odpSingleParts.get((int) i).getTabletId());
        }
        return partTabletIdMap;
    }

    private Map<Long, Long> buildTabletLsIdMap(List<OdpSinglePart> odpSingleParts) {
        Map<Long, Long> tabletLsId = new HashMap<>();
        for (OdpSinglePart odpSinglePart : odpSingleParts) {
            tabletLsId.put(odpSinglePart.getTabletId(), odpSinglePart.getLsId());
        }
        return tabletLsId;
    }
}

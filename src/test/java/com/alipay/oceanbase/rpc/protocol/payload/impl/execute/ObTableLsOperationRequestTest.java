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
package com.alipay.oceanbase.rpc.protocol.payload.impl.execute;

import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObHTableFilter;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObNewRange;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.ObScanOrder;
import com.alipay.oceanbase.rpc.table.ObKVParams;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType.SCAN;
import static com.alipay.oceanbase.rpc.table.ObKVParamsBase.paramType.HBase;

public class ObTableLsOperationRequestTest {
    private int lsOpReqSize = 100;
    private int tabletOpSize = 10;
    private int singleOpSize = 100;
    private static final Random random = new Random();

    @Test
    public void testEncodePerformance() {
        ObTableLSOpRequest[] lsReq = new ObTableLSOpRequest[lsOpReqSize];
        for (int i = 0; i < lsOpReqSize; i++) {
            lsReq[i] = buildLsOp();
        }
        long startTime = System.nanoTime();
        for (int i = 0; i < lsOpReqSize; i++) {
            lsReq[i].encode();
        }
        long endTime = System.nanoTime();
        long duration = endTime - startTime;
        System.out.println("each request encode took: " + (duration / lsOpReqSize) +" nanoseconds in average");
    }
    private ObTableLSOpRequest buildLsOp() {
        ObTableLSOpRequest lsOpReq = new ObTableLSOpRequest();
        lsOpReq.setCredential(new ObBytesString(generateRandomString(100).getBytes()));
        lsOpReq.setConsistencyLevel(ObTableConsistencyLevel.EVENTUAL);
        ObTableLSOperation lsOp = new ObTableLSOperation();
        lsOp.setLsId(1001);
        lsOp.setTableName(generateRandomString(10));
        lsOpReq.setLsOperation(lsOp);
        lsOpReq.setTableId(50001);
        lsOpReq.setEntityType(ObTableEntityType.HKV);
        lsOpReq.setTimeout(10000);
        lsOp.setNeedAllProp(true);
        lsOp.setReturnOneResult(false);
        for (int i = 0; i < tabletOpSize; i++) {
            lsOp.addTabletOperation(buildTabletOp());
        }
        lsOp.prepare();
        return lsOpReq;
    }

    private ObTableTabletOp buildTabletOp() {
        ObTableTabletOp tabletOp = new ObTableTabletOp();
        tabletOp.setTabletId(random.nextLong());
        List<ObTableSingleOp> singleOperations = new ArrayList<>();
        for (int i = 0; i < singleOpSize; i++) {
            singleOperations.add(budilSingleOp());
        }
        tabletOp.setSingleOperations(singleOperations);
        tabletOp.setIsSameType(random.nextBoolean());
        tabletOp.setIsSamePropertiesNames(random.nextBoolean());
        tabletOp.setIsReadOnly(random.nextBoolean());
        return tabletOp;
    }

    private ObTableSingleOp budilSingleOp() {
        ObTableSingleOp singleOp = new ObTableSingleOp();
        singleOp.setSingleOpType(SCAN);
        ObTableSingleOpQuery query = buildSingleOpQuery();
        singleOp.setQuery(query);
        List<ObTableSingleOpEntity> entities = new ArrayList<>();
        entities.add(buildSingleOpEntity());
        singleOp.setEntities(entities);
        singleOp.setIsCheckNoExists(random.nextBoolean());
        singleOp.setIsRollbackWhenCheckFailed(random.nextBoolean());
        return singleOp;
    }

    private ObTableSingleOpQuery buildSingleOpQuery() {
        String indexName = generateRandomString(10);

        List<ObNewRange> keyRanges = new ArrayList<>();
        keyRanges.add(buildRandomRange());

        List<String> selectColumns = new ArrayList<>();
        selectColumns.add("K");
        selectColumns.add("Q");
        selectColumns.add("T");
        selectColumns.add("V");

        ObScanOrder scanOrder = ObScanOrder.Forward;

        boolean isHbaseQuery = random.nextBoolean();

        ObHTableFilter obHTableFilter = new ObHTableFilter();
        ObKVParams obKVParams = new ObKVParams();
        obKVParams.setObParamsBase(obKVParams.getObParams(HBase));

        String filterString = generateRandomString(20);

        return ObTableSingleOpQuery.getInstance(
                indexName,
                keyRanges,
                selectColumns,
                scanOrder,
                isHbaseQuery,
                obHTableFilter,
                obKVParams,
                filterString
        );
    }

    private ObTableSingleOpEntity buildSingleOpEntity() {
        String[] rowKeyNames = {"K", "Q", "T"};
        Object[] rowKey = {
            generateRandomString(10),
            generateRandomString(10),
            generateRandomString(10)
        };

        String[] propertiesNames = {"V"};
        Object[] propertiesValues = { generateRandomString(20) };

        return ObTableSingleOpEntity.getInstance(
                rowKeyNames,
                rowKey,
                propertiesNames,
                propertiesValues
        );
    }

    public static String generateRandomString(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);

        for (int i = 0; i < length; i++) {
            // 生成一个随机字符（ASCII 码范围：32-126，包含字母、数字和符号）
            char randomChar = (char) (random.nextInt(95) + 32);
            sb.append(randomChar);
        }

        return sb.toString();
    }

    private static ObNewRange buildRandomRange() {
        ObNewRange range = new ObNewRange();

        List<ObObj> startKey = new ArrayList<>();
        startKey.add(ObObj.getInstance(generateRandomString(10)));
        startKey.add(ObObj.getInstance(generateRandomString(10)));
        startKey.add(ObObj.getInstance(generateRandomString(10)));

        List<ObObj> endKey = new ArrayList<>();
        endKey.add(ObObj.getInstance(generateRandomString(10)));
        endKey.add(ObObj.getInstance(generateRandomString(10)));
        endKey.add(ObObj.getInstance(generateRandomString(10)));

        ObRowKey startRk = new ObRowKey();
        startRk.setObjs(startKey);
        range.setStartKey(startRk);
        ObRowKey endRk = new ObRowKey();
        endRk.setObjs(endKey);
        range.setEndKey(endRk);

        return range;
    }
}

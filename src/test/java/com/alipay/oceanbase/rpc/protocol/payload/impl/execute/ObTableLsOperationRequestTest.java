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
import com.alipay.oceanbase.rpc.table.ObHBaseParams;
import com.alipay.oceanbase.rpc.table.ObKVParams;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.util.ObByteBuf;
import com.alipay.oceanbase.rpc.util.ObBytesString;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationType.SCAN;
import static com.alipay.oceanbase.rpc.table.ObKVParamsBase.paramType.HBase;

public class ObTableLsOperationRequestTest {
    private int lsOpReqSize = 10;
    private int tabletOpSize = 1;
    private int singleOpSize = 1;
    private int defaultIterSize = 1;

    private static final Random random = new Random();

    @Test
    public void testLsReqEncodePerformance() {
        PerformanceComparator perf_comparator = new PerformanceComparator();
        ObTableLSOpRequest[] lsReq = new ObTableLSOpRequest[lsOpReqSize];
        for (int i = 0; i < lsOpReqSize; i++) {
            lsReq[i] = buildLsReq();
        }
        for (int i = 0; i < lsOpReqSize; i++) {
            ObTableLSOpRequest lsRequest = lsReq[i];
            perf_comparator.execFirstWithReturn(() -> lsRequest.encode());
        }
        // Calculate and print average times
        perf_comparator.printResult("testLsReqEncodePerformance");
    }

    @Test
    public void testLsOpEncode() {
        PerformanceComparator perf_comparator = new PerformanceComparator();
        for (int i = 0; i < defaultIterSize; i++) {
            ObTableLSOperation lsOp = buildLsOperation();
            long payLoadSize = lsOp.getPayloadSize();
            byte[] oldEncodeBytes = perf_comparator.execFirstWithReturn(() -> lsOp.encode());
            Assert.assertEquals(oldEncodeBytes.length, payLoadSize);
            byte[] newEncodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perf_comparator.execSecond(() -> lsOp.encode(byteBuf));
            assertEncodeByteArray(oldEncodeBytes, newEncodeBytes);
        }
        // Calculate and print average times
        perf_comparator.printResult("testLsOpEncode");
    }

    @Test
    public void testTabletOpEncode() {
        PerformanceComparator perf_comparator = new PerformanceComparator();
        for (int i = 0; i < defaultIterSize; i++) {
            ObTableTabletOp tabletOp = buildTabletOp();
            long payLoadSize = tabletOp.getPayloadSize();
            byte[] oldEncodeBytes = perf_comparator.execFirstWithReturn(() -> tabletOp.encode());
            Assert.assertEquals(oldEncodeBytes.length, payLoadSize);
            byte[] newEncodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perf_comparator.execSecond(() -> tabletOp.encode(byteBuf));
            assertEncodeByteArray(oldEncodeBytes, newEncodeBytes);
        }
        // Calculate and print average times
        perf_comparator.printResult("testTabletOpEncode");
    }

    @Test
    public void testSingleOpEncode() {
        PerformanceComparator perf_comparator = new PerformanceComparator();
        for (int i = 0; i < defaultIterSize; i++) {
            ObTableSingleOp singleOp = buildSingleOp();
            long payLoadSize = singleOp.getPayloadSize();
            byte[] oldEncodeBytes = perf_comparator.execFirstWithReturn(() -> singleOp.encode());
            Assert.assertEquals(oldEncodeBytes.length, payLoadSize);
            byte[] newEncodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perf_comparator.execSecond(() -> singleOp.encode(byteBuf));
            assertEncodeByteArray(oldEncodeBytes, newEncodeBytes);
        }
        // Calculate and print average times
        perf_comparator.printResult("testSingleOpEncode");
    }

    @Test
    public void testSingleOpQueryEncode() {
        PerformanceComparator perfComparator = new PerformanceComparator();
        for (int i = 0; i < defaultIterSize; i++) {
            ObTableSingleOpQuery query = buildSingleOpQuery();
            long payLoadSize = query.getPayloadSize();
            byte[] oldEncodeBytes = perfComparator.execFirstWithReturn(() -> query.encode());
            Assert.assertEquals(oldEncodeBytes.length, payLoadSize);
            byte[] newEncodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perfComparator.execSecond(() -> query.encode(byteBuf));
            assertEncodeByteArray(oldEncodeBytes, newEncodeBytes);
        }
        // Calculate and print average times
        perfComparator.printResult("testSingleOpQueryEncode");
    }

    @Test
    public void testSingleOpEntityEncode() {
        PerformanceComparator perfComparator = new PerformanceComparator();
        for (int i = 0; i < defaultIterSize; i++) {
            ObTableSingleOpEntity entity = buildSingleOpEntity();
            long payLoadSize = entity.getPayloadSize();
            byte[] oldEncodeBytes = perfComparator.execFirstWithReturn(() -> entity.encode());
            Assert.assertEquals(oldEncodeBytes.length, payLoadSize);
            byte[] newEncodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perfComparator.execSecond(() -> entity.encode(byteBuf));
            assertEncodeByteArray(oldEncodeBytes, newEncodeBytes);
        }
        // Calculate and print average times
        perfComparator.printResult("testSingleOpEntityEncode");
    }

    @Test
    public void testKeyRangeEncode() {
        PerformanceComparator perfComparator = new PerformanceComparator();
        for (int i = 0; i < defaultIterSize; i++) {
            ObNewRange range = buildRandomRange();
            long encodeSize = range.getEncodedSize();
            byte[] oldEncodeBytes = perfComparator.execFirstWithReturn(() -> range.encode());
            Assert.assertEquals(oldEncodeBytes.length, encodeSize);
            byte[] newEncodeBytes = new byte[(int) encodeSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perfComparator.execSecond(() -> range.encode(byteBuf));
            assertEncodeByteArray(oldEncodeBytes, newEncodeBytes);
        }
        // Calculate and print average times
        perfComparator.printResult("testKeyRangeEncode");
    }

    @Test
    public void testHTableFilterEncode() {
        PerformanceComparator perfComparator = new PerformanceComparator();
        for (int i = 0; i < defaultIterSize; i++) {
            ObHTableFilter hTableFilter = new ObHTableFilter();
            hTableFilter.setValid(true);
            hTableFilter.setMaxVersions(generateRandomInt());
            hTableFilter.setMaxStamp(generateRandomInt());
            hTableFilter.setMinStamp(generateRandomInt());
            hTableFilter.setLimitPerRowPerCf(generateRandomInt());
            hTableFilter.setOffsetPerRowPerCf(generateRandomInt());
            hTableFilter.addSelectColumnQualifier(generateRandomString(10));
            hTableFilter.addSelectColumnQualifier(generateRandomString(10));
            hTableFilter.setFilterString(generateRandomString(10).getBytes(StandardCharsets.UTF_8));

            long payLoadSize = hTableFilter.getPayloadSize();
            byte[] oldEncodeBytes = perfComparator.execFirstWithReturn(() -> hTableFilter.encode());
            Assert.assertEquals(oldEncodeBytes.length, payLoadSize);
            byte[] newEncodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perfComparator.execSecond(() -> hTableFilter.encode(byteBuf));
            assertEncodeByteArray(oldEncodeBytes, newEncodeBytes);
        }
        // Calculate and print average times
        perfComparator.printResult("testHTableFilterEncode");
    }

    @Test
    public void testDefaultHTableFilterEncode() {
        PerformanceComparator perfComparator = new PerformanceComparator();
        {
            // caching default
            ObHTableFilter defaultHTableFilter = new ObHTableFilter();
            defaultHTableFilter.setValid(true);
            long defaultPayLoadSize = defaultHTableFilter.getPayloadSize();
            byte[] defaultEncodeBytes = new byte[(int) defaultPayLoadSize];
            ObByteBuf defaultByteBuf = new ObByteBuf(defaultEncodeBytes);
            defaultHTableFilter.encode(defaultByteBuf);
        }
        for (int i = 0; i < defaultIterSize; i++) {
            ObHTableFilter hTableFilter = new ObHTableFilter();
            hTableFilter.setValid(true);
            hTableFilter.setMaxVersions(generateRandomInt());
            hTableFilter.setMaxStamp(generateRandomInt());
            hTableFilter.setMinStamp(generateRandomInt());
            hTableFilter.setLimitPerRowPerCf(generateRandomInt());
            hTableFilter.setOffsetPerRowPerCf(generateRandomInt());
            long payLoadSize = hTableFilter.getPayloadSize();
            byte[] encodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(encodeBytes);
            perfComparator.execFirst(() -> hTableFilter.encode(byteBuf));

            // default
            ObHTableFilter defaultHTableFilter = new ObHTableFilter();
            defaultHTableFilter.setValid(true);
            long defaultPayLoadSize = defaultHTableFilter.getPayloadSize();
            byte[] defaultEncodeBytes = new byte[(int) defaultPayLoadSize];
            ObByteBuf defaultByteBuf = new ObByteBuf(defaultEncodeBytes);
            perfComparator.execSecond(() -> defaultHTableFilter.encode(defaultByteBuf));
        }
        // Calculate and print average times
        perfComparator.printResult("testDefaultHTableFilterEncode");
    }

    @Test
    public void testKVParamsEncode() {
        PerformanceComparator perfComparator = new PerformanceComparator();
        for (int i = 0; i < defaultIterSize; i++) {
            ObKVParams obKVParams = new ObKVParams();
            ObHBaseParams hbaseParams = (ObHBaseParams) obKVParams.getObParams(HBase);
            obKVParams.setObParamsBase(hbaseParams);
            hbaseParams.setCaching(generateRandomInt());
            hbaseParams.setCallTimeout(generateRandomInt());
            hbaseParams.setAllowPartialResults(generateRandomBoolean());
            hbaseParams.setCacheBlock(generateRandomBoolean());
            hbaseParams.setCheckExistenceOnly(generateRandomBoolean());

            long payLoadSize = obKVParams.getPayloadSize();

            byte[] oldEncodeBytes = perfComparator.execFirstWithReturn(() -> obKVParams.encode());
            Assert.assertEquals(oldEncodeBytes.length, payLoadSize);
            byte[] newEncodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perfComparator.execSecond(() -> obKVParams.encode(byteBuf));
            assertEncodeByteArray(oldEncodeBytes, newEncodeBytes);
        }
        // Calculate and print average times
        perfComparator.printResult("testKVParamsEncode");
    }

    @Test
    public void testDefaultKVParamsEncode() {
        PerformanceComparator perfComparator = new PerformanceComparator();
        // cache default bytes
        {
            // default encode
            ObKVParams defaultObKVParams = new ObKVParams();
            ObHBaseParams defaultHbaseParams = (ObHBaseParams) defaultObKVParams.getObParams(HBase);
            defaultObKVParams.setObParamsBase(defaultHbaseParams);
            long defaultPayLoadSize = defaultObKVParams.getPayloadSize();
            byte[] defaultEncodeBytes = new byte[(int) defaultPayLoadSize];
            ObByteBuf defaultByteBuf = new ObByteBuf(defaultEncodeBytes);
            defaultObKVParams.encode(defaultByteBuf);
        }
        for (int i = 0; i < defaultIterSize; i++) {
            ObKVParams obKVParams = new ObKVParams();
            ObHBaseParams hbaseParams = (ObHBaseParams) obKVParams.getObParams(HBase);
            obKVParams.setObParamsBase(hbaseParams);
            hbaseParams.setCaching(generateRandomInt());
            hbaseParams.setCallTimeout(generateRandomInt());
            hbaseParams.setAllowPartialResults(generateRandomBoolean());
            hbaseParams.setCacheBlock(generateRandomBoolean());
            hbaseParams.setCheckExistenceOnly(generateRandomBoolean());
            long payLoadSize = obKVParams.getPayloadSize();
            byte[] newEncodeBytes = new byte[(int) payLoadSize];
            ObByteBuf byteBuf = new ObByteBuf(newEncodeBytes);
            perfComparator.execFirst(() -> obKVParams.encode(byteBuf));
            // default encode
            ObKVParams defaultObKVParams = new ObKVParams();
            ObHBaseParams defaultHbaseParams = (ObHBaseParams) defaultObKVParams.getObParams(HBase);
            defaultObKVParams.setObParamsBase(defaultHbaseParams);
            long defaultPayLoadSize = defaultObKVParams.getPayloadSize();
            byte[] defaultEncodeBytes = new byte[(int) defaultPayLoadSize];
            ObByteBuf defaultByteBuf = new ObByteBuf(defaultEncodeBytes);
            perfComparator.execSecond(() -> defaultObKVParams.encode(defaultByteBuf));
        }
        // Calculate and print average times
        perfComparator.printResult("testDefaultKVParamsEncode");
    }


    private static void assertEncodeByteArray(byte[] bytes1, byte[] bytes2) {
        if (bytes1 == bytes2) return;
        if (bytes1 == null || bytes2 == null) Assert.fail();
        if (bytes1.length != bytes2.length) Assert.fail();

        for (int i = 0; i < bytes1.length; i++) {
            if (bytes1[i] != bytes2[i]) {
                System.err.println("byte not equal in index:"+ i + " ,bytes1:" + bytes1[i] + " ,bytes2:" + bytes2[i]);
                Assert.assertEquals(bytes1, bytes2);
            }
        }
    }


    private ObTableLSOpRequest buildLsReq() {
        ObTableLSOpRequest lsOpReq = new ObTableLSOpRequest();
        lsOpReq.setCredential(new ObBytesString(generateRandomString(100).getBytes()));
        lsOpReq.setConsistencyLevel(ObTableConsistencyLevel.EVENTUAL);
        buildLsOperation();
        lsOpReq.setLsOperation(buildLsOperation());
        lsOpReq.setTableId(50001);
        lsOpReq.setEntityType(ObTableEntityType.HKV);
        lsOpReq.setTimeout(10000);
        return lsOpReq;
    }

    private ObTableLSOperation buildLsOperation() {
        ObTableLSOperation lsOp = new ObTableLSOperation();
        lsOp.setLsId(1001);
        lsOp.setTableName(generateRandomString(10));

        lsOp.setNeedAllProp(true);
        lsOp.setReturnOneResult(false);
        for (int i = 0; i < tabletOpSize; i++) {
            lsOp.addTabletOperation(buildTabletOp());
        }
        lsOp.prepare();
        return lsOp;
    }

    private ObTableTabletOp buildTabletOp() {
        ObTableTabletOp tabletOp = new ObTableTabletOp();
        tabletOp.setTabletId(random.nextLong());
        List<ObTableSingleOp> singleOperations = new ArrayList<>();
        for (int i = 0; i < singleOpSize; i++) {
            singleOperations.add(buildSingleOp());
        }
        tabletOp.setSingleOperations(singleOperations);
        tabletOp.setIsSameType(random.nextBoolean());
        tabletOp.setIsSamePropertiesNames(random.nextBoolean());
        tabletOp.setIsReadOnly(random.nextBoolean());
        return tabletOp;
    }

    private ObTableSingleOp buildSingleOp() {
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
        obHTableFilter.setValid(true);
        ObKVParams obKVParams = new ObKVParams();
        obKVParams.setObParamsBase(obKVParams.getObParams(HBase));

        String filterString = generateRandomString(20);

        ObTableSingleOpQuery query =  ObTableSingleOpQuery.getInstance(
                                                                indexName,
                                                                keyRanges,
                                                                selectColumns,
                                                                scanOrder,
                                                                isHbaseQuery,
                                                                obHTableFilter,
                                                                obKVParams,
                                                                filterString
                                                        );
        String[] rowKeyNames = {"K", "Q", "T"};
        Map<String, Long> rowkeyColumnIdxMap = IntStream.range(0, rowKeyNames.length)
                .boxed()
                .collect(Collectors.toMap(i -> rowKeyNames[i], i -> Long.valueOf(i)));
        query.adjustScanRangeColumns(rowkeyColumnIdxMap);
        return query;
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

        ObTableSingleOpEntity entity = ObTableSingleOpEntity.getInstance(
                                                rowKeyNames,
                                                rowKey,
                                                propertiesNames,
                                                propertiesValues
                                        );
        Map<String, Long> rowkeyColumnIdxMap = IntStream.range(0, rowKeyNames.length)
                .boxed()
                .collect(Collectors.toMap(i -> rowKeyNames[i], i -> Long.valueOf(i)));
        Map<String, Long> propColumnIdxMap = IntStream.range(0, propertiesNames.length)
                .boxed()
                .collect(Collectors.toMap(i -> propertiesNames[i], i -> Long.valueOf(i)));
        entity.adjustRowkeyColumnName(rowkeyColumnIdxMap);
        entity.adjustPropertiesColumnName(propColumnIdxMap);
        return entity;
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

    public static int generateRandomInt() {
        return random.nextInt();
    }

    public static boolean generateRandomBoolean() {
        return random.nextBoolean();
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
class PerformanceComparator {
    PerformanceCalculator calc1 = new PerformanceCalculator();
    PerformanceCalculator calc2 = new PerformanceCalculator();

    public void execFirst(Runnable function) {
        calc1.execAndMeasureTime(function);
    }

    public <T> T execFirstWithReturn(Supplier<T> function) {
        return calc1.execAndmeasureTimeWithReturn(function);
    }

    public void execSecond(Runnable function) {
        calc2.execAndMeasureTime(function);
    }

    public <T> T execSecondWithReturn(Supplier<T> function) {
        return calc2.execAndmeasureTimeWithReturn(function);
    }

    public void printResult(String msg) {
        System.out.println("=========================" + msg + "================================");
        if (calc1.isValid()) {
            calc1.printResults("first result");

        }
        if (calc2.isValid()) {
            calc2.printResults("second result");
        }
        if (!calc2.isValid() && !calc1.isValid()) {
            System.out.println("not valid results");
        }
        System.out.println("===========================================================================");
    }
}

class PerformanceCalculator {
    List<Long> executionTimes = null;

    public PerformanceCalculator() {
        this.executionTimes = new ArrayList<>();
    }

    public boolean isValid() {
        return executionTimes != null && !executionTimes.isEmpty();
    }

    public void execAndMeasureTime(Runnable function) {
        long startTime = System.nanoTime();
        function.run();
        long endTime = System.nanoTime();
        executionTimes.add(endTime - startTime);
    }

    public <T> T execAndmeasureTimeWithReturn(Supplier<T> function) {
        long startTime = System.nanoTime();
        T result = function.get();
        long endTime = System.nanoTime();
        executionTimes.add(endTime - startTime);
        return result;
    }

    public double getAverageTime() {
        return executionTimes.stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0.0);
    }

    public void printResults(String msg) {
        System.out.println(msg +  ": \n\taverage execution time: " + getAverageTime() + " ns");
    }

    public void clear() {
        executionTimes.clear();
    }
}

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

package com.alipay.oceanbase.rpc;

import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.model.*;
import com.alipay.oceanbase.rpc.location.model.ReplicaLocation;
import com.alipay.oceanbase.rpc.location.model.partition.*;
import com.alipay.oceanbase.rpc.mutation.Row;
import com.alipay.oceanbase.rpc.protocol.packet.ObRpcPacketHeader;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObReadConsistency;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.query.*;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.syncquery.*;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryAsyncStreamResult;
import com.alipay.oceanbase.rpc.stream.ObTableClientQueryStreamResult;
import com.alipay.oceanbase.rpc.table.ObTable;
import com.alipay.oceanbase.rpc.table.ObTableParam;
import com.alipay.oceanbase.rpc.table.ObTableClientBatchOpsImpl;
import com.alipay.oceanbase.rpc.table.ObTableClientLSBatchOpsImpl;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/** Offline regression through real GET, query execution and replica selection. */
public class ObTableReplicaRetryTest {
    @Test
    public void rowAndLegacyGetRetryAndKeepFailuresLocal() throws Exception {
        for (boolean legacy : new boolean[] { false, true }) {
            Fixture f = new Fixture();
            f.a.response = request -> { throw unreadable(7001); };
            f.b.response = request -> success();
            for (int request = 0; request < 2; request++) {
                if (legacy) {
                    assertTrue(f.client.get("t", new Object[] { 1 }, new String[] { "v" }).isEmpty());
                } else {
                    assertTrue(f.client.get("t").setRowKey(new Row("k", 1))
                        .select("v").execute().isEmpty());
                }
            }
            assertEquals(2, f.a.sends);
            assertEquals(2, f.b.sends);
        }
    }

    @Test
    public void getRecordsTheMoveTargetRatherThanTheOriginalNode() throws Exception {
        Fixture f = new Fixture();
        f.client.moveTarget = f.b;
        f.b.response = request -> { throw unreadable(7002); };
        AtomicInteger selections = new AtomicInteger();
        // Only the MOVE target fails; the original replica remains a valid retry candidate.
        f.a.response = request -> {
            if (selections.getAndIncrement() == 0) {
                return new ObTableApiMove();
            }
            return success();
        };
        assertTrue(f.client.get("t", new Row("k", 1), new String[] { "v" }).isEmpty());
        assertEquals(2, f.a.sends);
        assertEquals(1, f.b.sends);
    }

    @Test
    public void routeFiltersPortsAndPreservesPolicyAndFallback() throws Exception {
        Fixture f = new Fixture();
        Set<String> failed = new HashSet<>(Collections.singleton("127.0.0.1:7001"));
        assertSame(f.b, f.route.getTableWithPartId("t", 0, ObReadConsistency.WEAK, failed).getObTable());
        assertSame(f.a, f.route.getTableWithPartId("t", 0, ObReadConsistency.WEAK).getObTable());
        failed.add("127.0.0.1:7002");
        assertSame(f.leader, f.route.getTableWithPartId("t", 0, ObReadConsistency.WEAK, failed).getObTable());
        f.route.setRoutePolicy(ObRoutePolicy.FOLLOWER_ONLY);
        assertSame(f.a, f.route.getTableWithPartId("t", 0, ObReadConsistency.WEAK, failed).getObTable());
        failed.add("127.0.0.1:7000");
        assertSame(f.leader, f.route.getTableWithPartId("t", 0, ObReadConsistency.STRONG, failed).getObTable());
        assertEquals(3, failed.size());
    }

    @Test
    public void noLdcFiltersAndExhaustedFollowersCanRecover() throws Exception {
        Fixture f = new Fixture();
        f.location.getSameIdc().clear();
        f.location.getOtherRegion().clear();
        Set<String> failed = new HashSet<>(Collections.singleton("127.0.0.1:7001"));
        assertSame(f.b, f.route.getTableWithPartId("t", 0, ObReadConsistency.WEAK, failed).getObTable());
        f.route.setRoutePolicy(ObRoutePolicy.FOLLOWER_ONLY);
        failed.add("127.0.0.1:7002");
        ObTable recovered = f.route.getTableWithPartId("t", 0, ObReadConsistency.WEAK, failed).getObTable();
        assertTrue(recovered == f.a || recovered == f.b);
        assertEquals(2, failed.size());
        f.location.getReplicas().removeIf(r -> !r.isLeader());
        try {
            f.route.getTableWithPartId("t", 0, ObReadConsistency.WEAK, failed);
            fail("FOLLOWER_ONLY must not select a leader");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("No follower"));
        }
    }

    @Test
    public void filteredFollowersCanRecoverEvenWhenThereIsNoLeader() throws Exception {
        Fixture f = new Fixture();
        ObPartitionLocation noLeader = new ObPartitionLocation();
        ReplicaLocation follower = f.location.getSameIdc().get(0);
        noLeader.addReplicaLocation(follower);
        assertSame(follower, noLeader.getReplica(ObReadConsistency.WEAK,
            ObRoutePolicy.FOLLOWER_FIRST, Collections.singleton("127.0.0.1:7001")));
    }

    @Test
    public void ordinaryBatchRetainsItsExistingStrongReroute() throws Exception {
        Fixture f = new Fixture();
        f.a.response = request -> { throw unreadable(7001); };
        f.leader.response = request -> {
            ObTableBatchOperationResult result = new ObTableBatchOperationResult();
            result.addResult(success());
            return result;
        };
        ObTableClientBatchOpsImpl batch = new ObTableClientBatchOpsImpl("t", f.client);
        batch.setIsWeakRead(true);
        batch.get(new Object[] { 1 }, new String[] { "v" });
        ObTableOperationResult[] results = new ObTableOperationResult[1];
        batch.partitionExecute(results, batch.partitionPrepare().entrySet().iterator().next());
        assertNotNull(results[0]);
        assertEquals(7000, results[0].getExecutePort());
        assertEquals(1, f.a.sends);
        assertEquals(1, f.leader.sends);
    }

    @Test
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public void lsBatchRetainsItsExistingWeakRetryWithoutExclusion() throws Exception {
        Fixture f = new Fixture();
        AtomicInteger attempts = new AtomicInteger();
        f.a.response = request -> {
            if (attempts.getAndIncrement() == 0) {
                throw unreadable(7001);
            }
            ObTableSingleOpResult operation = new ObTableSingleOpResult();
            operation.getHeader().setMsg(new byte[0]);
            ObTableTabletOpResult tablet = new ObTableTabletOpResult();
            tablet.addResult(operation);
            ObTableLSOpResult result = new ObTableLSOpResult();
            result.getResults().add(tablet);
            return result;
        };
        ObTableClientLSBatchOpsImpl batch = new ObTableClientLSBatchOpsImpl("t", f.client);
        batch.setIsWeakRead(true);
        batch.addOperation(f.client.get("t").setRowKey(new Row("k", 1)).select("v"));
        Map partitions = batch.partitionPrepare();
        ObTableSingleOpResult[] results = new ObTableSingleOpResult[1];
        batch.partitionExecute(results, (Map.Entry) partitions.entrySet().iterator().next());
        assertNotNull(results[0]);
        assertEquals(2, f.a.sends);
        assertEquals(0, f.b.sends);
    }

    @Test
    public void rosterRefreshStillFiltersTheFailedReplica() throws Exception {
        Fixture f = new Fixture();
        f.roster.missOnce = true;
        assertSame(f.b, f.route.getTableWithPartId("t", 0, ObReadConsistency.WEAK,
            Collections.singleton("127.0.0.1:7001")).getObTable());
        assertEquals(2, f.roster.lookups);
    }

    @Test
    public void syncScanSwitchesOnStartAndPinsTheNextPage() throws Exception {
        Fixture f = new Fixture();
        f.a.response = request -> { throw unreadable(7001); };
        AtomicInteger sends = new AtomicInteger();
        f.b.response = payload -> {
            int n = sends.getAndIncrement();
            if (n == 1) {
                throw unreadable(7002); // NEXT must stay on B, even though routing prefers A.
            }
            if (n > 0) {
                assertEquals(77, ((ObTableStreamRequest) payload).getSessionId());
            }
            return page(n == 0 ? 1 : 2, n == 0);
        };
        SyncScan scan = configure(new SyncScan(), f);
        scan.start(f.pair());
        assertTrue(scan.next());
        assertEquals(1L, scan.getRow().get(0).getValue());
        assertTrue(scan.next());
        assertEquals(2L, scan.getRow().get(0).getValue());
        assertFalse(scan.next());
        assertEquals(1, f.a.sends);
        assertEquals(3, sends.get());
    }

    @Test
    public void asyncScanPinsNextRenewAndEndToTheSuccessfulReplica() throws Exception {
        Fixture f = new Fixture();
        f.a.response = request -> { throw unreadable(7001); };
        List<ObQueryOperationType> operations = new ArrayList<>();
        AtomicInteger nextSends = new AtomicInteger();
        f.b.response = payload -> {
            ObTableQueryAsyncRequest request = (ObTableQueryAsyncRequest) payload;
            ObQueryOperationType type = request.getQueryType();
            operations.add(type);
            if (type != ObQueryOperationType.QUERY_START) {
                Field session = ObTableQueryAsyncRequest.class.getDeclaredField("querySessionId");
                session.setAccessible(true);
                assertEquals(77L, session.getLong(request));
            }
            if (type == ObQueryOperationType.QUERY_NEXT && nextSends.getAndIncrement() == 0) {
                throw unreadable(7002);
            }
            ObTableQueryAsyncResult result = new ObTableQueryAsyncResult();
            result.setSessionId(77);
            result.setEnd(type == ObQueryOperationType.QUERY_END);
            result.setAffectedEntity(type == ObQueryOperationType.QUERY_START ? page(1, false)
                : type == ObQueryOperationType.QUERY_NEXT ? page(2, false) : new ObTableQueryResult());
            return result;
        };
        ObTableClientQueryAsyncStreamResult scan = configure(new ObTableClientQueryAsyncStreamResult(), f);
        scan.setExpectant(Collections.singletonMap(0L, f.pair()));
        scan.init();
        assertTrue(scan.next());
        assertEquals(1L, scan.getRow().get(0).getValue());
        scan.renewLease();
        assertTrue(scan.next());
        assertEquals(2L, scan.getRow().get(0).getValue());
        scan.close();
        assertEquals(Arrays.asList(ObQueryOperationType.QUERY_START, ObQueryOperationType.QUERY_RENEW,
            ObQueryOperationType.QUERY_NEXT, ObQueryOperationType.QUERY_NEXT, ObQueryOperationType.QUERY_END), operations);
        assertEquals(1, f.a.sends);
    }

    @Test(timeout = 5000)
    public void continuousUnreadableGetAndScanStopAtTheExistingBudget() throws Exception {
        for (boolean scanRequest : new boolean[] { false, true }) {
            Fixture f = new Fixture();
            f.client.setRuntimeMaxWait(150);
            f.client.setRuntimeRetryInterval(10);
            for (StubTable table : f.tables.values()) {
                table.response = request -> { throw unreadable(table.getPort()); };
            }
            try {
                if (scanRequest) {
                    configure(new SyncScan(), f).start(f.pair());
                } else {
                    f.client.get("t", new Row("k", 1), new String[] { "v" });
                }
                fail("expected bounded failure");
            } catch (ObTableException expected) {
                assertTrue(expected instanceof ObTableTimeoutExcetion
                    || expected instanceof ObTableReplicaNotReadableException);
            }
            assertTrue(f.a.sends > 0);
        }
    }

    @Test
    public void odpGetDoesNotGainAReplicaRetryRule() throws Exception {
        Fixture f = new Fixture();
        f.client.setOdpMode(true);
        f.route.odpTable = f.a;
        f.a.response = request -> { throw unreadable(7001); };
        try {
            f.client.get("t", new Row("k", 1), new String[] { "v" });
            fail("expected replica exception");
        } catch (ObTableReplicaNotReadableException expected) {
            assertEquals(1, f.a.sends);
        }
    }

    private static ObTableOperationResult success() {
        ObTableOperationResult result = new ObTableOperationResult();
        result.getHeader().setMsg(new byte[0]);
        return result;
    }

    private static ObTableException unreadable(int port) {
        return ExceptionUtil.convertToObTableException("127.0.0.1", port, 1, 1, -6231, "");
    }

    private static ObTableQueryResult page(long value, boolean more) {
        ObRpcPacketHeader header = new ObRpcPacketHeader();
        header.setSessionId(77);
        if (more) {
            header.setStreamNext();
        } else {
            header.setStreamLast();
        }
        ObTableQueryResult result = new ObTableQueryResult(header);
        result.setPropertiesNames(Collections.singletonList("v"));
        result.setPropertiesRows(Collections.singletonList(Collections.singletonList(ObObj.getInstance(value))));
        result.setRowCount(1);
        return result;
    }

    private static <T extends AbstractQueryStreamResult> T configure(T scan, Fixture f) {
        scan.setClient(f.client);
        scan.setTableName("t");
        scan.setIndexTableName("t");
        scan.setEntityType(ObTableEntityType.KV);
        scan.setTableQuery(new ObTableQuery());
        scan.setReadConsistency(ObReadConsistency.WEAK);
        scan.setExpectant(new LinkedHashMap<>());
        return scan;
    }

    private static void field(Object target, Class<?> type, String name, Object value) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static final class TestClient extends ObTableClient {
        ObTable moveTarget;
        @Override public void checkStatus() { }
        @Override public void syncRefreshMetadata(boolean force) { }
        @Override public ObTable getTable(ObTableApiMove move) { return moveTarget; }
    }

    private static final class SyncScan extends ObTableClientQueryStreamResult {
        void start(ObPair<Long, ObTableParam> pair) throws Exception {
            execute(pair, new ObTableQueryRequest());
            initialized = true;
        }
    }

    private interface Response {
        ObPayload execute(ObPayload request) throws Exception;
    }

    private static final class StubTable extends ObTable {
        final int port;
        int sends;
        Response response;
        StubTable(int port) { this.port = port; }
        @Override public String getIp() { return "127.0.0.1"; }
        @Override public int getPort() { return port; }
        @Override public ObPayload execute(ObPayload request) {
            sends++;
            try {
                return response.execute(request);
            } catch (RuntimeException | Error e) {
                throw e;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }
    }

    private static final class RecordingRoster extends TableRoster {
        int lookups;
        boolean missOnce;
        @Override public ObTable getTable(ObServerAddr addr) {
            lookups++;
            if (missOnce) {
                missOnce = false;
                return null;
            }
            return super.getTable(addr);
        }
    }

    private static final class LocalRoute extends TableRoute {
        final TableEntry entry = new TableEntry();
        ObTable odpTable;
        LocalRoute(ObTableClient client) { super(client, null); }
        @Override public TableEntry getTableEntry(String name) { return entry; }
        @Override public TableEntry refreshPartitionLocation(String name, long tabletId, TableEntry previous) {
            return entry;
        }
        @Override public ObTable getOdpTable() { return odpTable; }
    }

    private static final class Fixture {
        final TestClient client = new TestClient();
        final LocalRoute route = new LocalRoute(client);
        final RecordingRoster roster = new RecordingRoster();
        final ObPartitionLocation location = new ObPartitionLocation();
        final Map<Integer, StubTable> tables = new HashMap<>();
        final StubTable leader = table(7000, ObServerRole.LEADER);
        final StubTable a = table(7001, ObServerRole.FOLLOWER);
        final StubTable b = table(7002, ObServerRole.FOLLOWER);

        Fixture() throws Exception {
            client.setRuntimeMaxWait(2000);
            client.setRuntimeRetryInterval(1);
            route.setReadConsistency(ObReadConsistency.WEAK);
            ObPartitionEntry partitions = new ObPartitionEntry();
            ObPartitionLocationInfo info = partitions.getPartitionInfo(0);
            info.updateLocation(location, 1L);
            info.initialized.set(true);
            route.entry.setPartitionEntry(partitions);
            route.entry.setPartitionNum(1L);
            route.entry.setTableId(42L);
            field(client, ObTableClient.class, "tableRoute", route);
            field(route, TableRoute.class, "tableRoster", roster);
        }

        private StubTable table(int port, ObServerRole role) {
            StubTable table = new StubTable(port);
            tables.put(port, table);
            ObServerAddr addr = new ObServerAddr("127.0.0.1", port);
            roster.putIfAbsent(addr, table);
            ReplicaLocation replica = new ReplicaLocation();
            replica.setAddr(addr);
            replica.setRole(role);
            ObServerInfo info = new ObServerInfo();
            info.setStatus("ACTIVE");
            replica.setInfo(info);
            location.addReplicaLocation(replica);
            if (port == 7001) {
                location.getSameIdc().add(replica);
            } else if (port == 7002) {
                location.getOtherRegion().add(replica);
            }
            return table;
        }

        ObPair<Long, ObTableParam> pair() {
            return new ObPair<>(0L, new ObTableParam(a, 42, 0));
        }
    }
}
